import { XMLParser } from 'fast-xml-parser';

/* ====== Config ====== */
const SHOP_DOMAIN    = process.env.SHOP_DOMAIN;      // ör: "shkx8d-wy"
const ACCESS_TOKEN   = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION    = "2024-07";
const SOURCE_URL     = process.env.SOURCE_URL;
const BATCH_SIZE     = Number(process.env.BATCH_SIZE || 25);

/* Temizlik modları */
const CLEANUP_IMAGES   = /^(1|true|yes)$/i.test(process.env.CLEANUP_IMAGES   || '');
const CLEANUP_VARIANTS = /^(1|true|yes)$/i.test(process.env.CLEANUP_VARIANTS || '');
const VARIANT_DELETE   = /^(1|true|yes)$/i.test(process.env.VARIANT_DELETE   || '');

/* Hız sınırı */
const QPS              = Number(process.env.QPS || 3);
const MAX_RETRY        = 7;
const BASE_BACKOFF_MS  = 1200;

/* Teşhis (harita dökümü) */
const DIAG             = /^(1|true|yes)$/i.test(process.env.DIAG || '');

/* Finish modu: 'split' (ayrı ürün) | 'option' (3. seçenek) */
const FINISH_MODE      = (process.env.FINISH_MODE || 'split').toLowerCase(); // default split

/* ====== Utils ====== */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const num   = (x)=> { if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=> { const n=num(x); return n ? n.toFixed(2) : "0.00"; };
const uc    = (s)=> (s||"").toString().trim().toUpperCase();
const uniq  = (arr)=> Array.from(new Set(arr.filter(Boolean)));
const clip  = (s,m)=> { s=String(s||"").trim(); return s.length>m? s.slice(0,m-1)+"…": s; };
const norm  = (s)=> (s||"").toString().replace(/\s+/g,' ').trim();

/* ----- Shopify REST + throttle/backoff ----- */
function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if(json) h["Content-Type"] = "application/json";
  return h;
}
let lastRequestAt = 0;
async function throttleGate(){
  const minGap = Math.max(10, Math.floor(1000 / Math.max(1, QPS)));
  const now = Date.now();
  const wait = Math.max(0, (lastRequestAt + minGap) - now);
  if (wait) await sleep(wait + Math.floor(Math.random()*60));
  lastRequestAt = Date.now();
}
async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  for (let attempt=0; attempt<MAX_RETRY; attempt++){
    await throttleGate();
    const res = await fetch(url, { method, headers: headers(true), body: body ? JSON.stringify(body) : undefined });
    if (res.status===429 || res.status>=500){
      const ra = Number(res.headers.get('Retry-After')) || 0;
      const backoff = ra>0 ? ra*1000 : Math.min(BASE_BACKOFF_MS*Math.pow(1.6,attempt), 15000);
      await sleep(backoff + Math.floor(Math.random()*200));
      continue;
    }
    if (!res.ok){
      const t = await res.text().catch(()=>res.statusText);
      throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${t}`);
    }
    const text = await res.text().catch(()=>null);
    if (!text) return {};
    try { return JSON.parse(text); } catch { return {}; }
  }
  throw new Error(`Shopify ${method} ${path} -> too many retries`);
}

/* ----- Publish (izin yoksa fallback) ----- */
async function getOnlineStorePublicationId(){
  const pubs = await rest(`/publications.json`, 'GET');
  const pub = (pubs?.publications||[]).find(p=>/online/i.test(p.name));
  return pub?.id;
}
async function publishProduct(productId){
  try{
    const pubId = await getOnlineStorePublicationId();
    if(!pubId) throw new Error('No publication id');
    await rest(`/publications/${pubId}/publish.json`, 'POST', {
      publication: { publishable_id: productId, publishable_type: "product" }
    });
  }catch{
    try{
      await rest(`/products/${productId}.json`,'PUT',{ product:{ id: productId, status:'active', published_scope:'web' } });
    }catch(e){ console.log('PUBLISH-FALLBACK WARN:', String(e.message||e).slice(0,160)); }
  }
}

/* ----- Lokasyon / Stok ----- */
let LOCATION_ID_CACHE=null;
async function getLocationId(){
  if(LOCATION_ID_CACHE) return LOCATION_ID_CACHE;
  const js = await rest(`/locations.json`,'GET');
  LOCATION_ID_CACHE = js?.locations?.[0]?.id;
  return LOCATION_ID_CACHE;
}
async function setInventory(inventoryItemId, qty){
  const locationId = await getLocationId();
  if(!locationId || !inventoryItemId) return;
  await rest(`/inventory_levels/set.json`,'POST',{ location_id: locationId, inventory_item_id: inventoryItemId, available: Number(qty||0) });
}

/* ----- Mevcut ürün indeksini yükle (model:* tag) ----- */
async function loadProductIndex(){
  const map = new Map(); // model:<...> -> product
  let pageInfo = null;
  while(true){
    const url = `/products.json?limit=250${pageInfo?`&page_info=${pageInfo}`:''}&fields=id,title,tags,images,variants,status`;
    const res = await fetch(`https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${url}`, { headers: headers(false) });
    if(!res.ok) throw new Error(`Shopify GET ${url} -> ${res.status}`);
    const link = res.headers.get('link') || '';
    const js = await res.json();
    for(const p of (js.products||[])){
      const tag = (p.tags||"").split(',').map(t=>t.trim()).find(t=>t.startsWith('model:'));
      if(tag) map.set(tag, p);
    }
    const m = link.match(/<([^>]+page_info=([^>]+))>; rel="next"/);
    if(m){ pageInfo = m[2]; } else break;
    await sleep(120);
  }
  return map;
}

/* ====== XML Ayrıştırma ====== */
const COLOR_WORDS = [
  'SIYAH','BEYAZ','KAHVE','LACIVERT','TABA','GRI','ANTRASIT','SAX',
  'BEYAZ/SIYAH','SIYAH/BEYAZ','BYZ','AT','KE'
];

function detectFinish(name, desc){
  const s = uc((name||'') + ' ' + (desc||''));
  if (/\bVERNICIATA\b/.test(s) || /\bPARLAK\b/.test(s)) return 'Parlak';
  if (/\bMAT\b/.test(s)) return 'Mat';
  // İstersen ileride 'Süet', 'Nubuk' vb. eklenir.
  return null;
}

function extractBaseColor(nameRaw){
  const s = uc(nameRaw||'').replace(/[^\wÇĞİÖŞÜ\/\s-]/g,' ').replace(/\s+/g,' ').trim();
  const parts = s.split(' ');
  let color = parts[parts.length-1].replace(/[^\w\/-]/g,'').toUpperCase();
  if (!COLOR_WORDS.includes(color)) color = '';
  const base = color ? s.slice(0, s.lastIndexOf(' '+color)).trim() : s;
  return { baseTitle: base, colorGuess: color };
}

function niceTitle(base, brand, mpn){
  const human = base.replace(/^MN\d+\s*-\s*/,'').trim();
  return `${human} – ${brand} ${mpn}`.replace(/\s+/g,' ').trim();
}

function parseXML(xml){
  const parser = new XMLParser({ ignoreAttributes:true, attributeNamePrefix:'', trimValues:true });
  const root = parser.parse(xml)||{};
  const list = [].concat(root?.Products?.Product||[]);
  return list.map(p=>{
    const mpn    = norm(p.Mpn||'');
    const brand  = norm(p.Brand||'') || 'MOOİEN';
    const name   = norm(p.Name||'');
    const desc   = (p.Description||'').toString();
    const { baseTitle, colorGuess } = extractBaseColor(name);
    const finish = detectFinish(name, desc); // 'Parlak' | 'Mat' | null
    const price  = num(p.Price);
    const tax    = num(p.Tax);
    const imgs   = uniq([p.Image1,p.Image2,p.Image3,p.Image4,p.Image5].filter(Boolean));
    const main   = norm(p.mainCategory||'');
    const cat    = norm(p.category||'');

    const variants = []
      .concat(p?.variants?.variant||[])
      .map(v=>{
        const specs  = [].concat(v?.spec||[]);
        const renk   = uc(specs.find(s=>s?.name==='Renk')?.['#text'] || colorGuess || 'STANDART');
        const beden  = String(specs.find(s=>s?.name==='Beden')?.['#text'] || '').trim();
        return {
          color   : renk,
          size    : beden,
          finish  : finish || 'Std',  // varyanta taşınır (option modunda)
          sku     : String(v.productCode||v.barcode||v.variantId||'').trim(),
          barcode : String(v.barcode||'').trim(),
          qty     : num(v.quantity),
          price   : num(v.price) || price
        };
      });

    // Model anahtarı: MPN + (split modunda finish)
    const baseKey  = `model:${brand}|${mpn}`;
    const modelKey = (FINISH_MODE==='split' && finish) ? `${baseKey}|finish:${finish}` : baseKey;

    const title    = niceTitle(baseTitle, brand, mpn);
    const tags     = uniq([
      baseKey,                                   // her ürünün temel modeli
      modelKey,                                  // çalıştığımız ürünün spesifik anahtarı (split'te finish dahil)
      `brand:${brand}`,
      `kategori:${/BOT/i.test(cat+main)?'bot':/SPOR/i.test(cat+main)?'spor':'klasik'}`,
      finish ? `finish:${finish}` : null,
      'satis:acik'
    ]);

    return { mpn, brand, baseTitle, colorGuess, finish, title, price, tax, images:imgs, variants, modelKey, baseKey, tags };
  });
}

/* ====== Gruplama ====== */
function groupByModel(items){
  // split: modelKey (brand|mpn|finish?) tek ürün
  // option: baseKey (brand|mpn) tek ürün; finish varyant alanına gider
  const map = new Map();
  for(const it of items){
    const key = (FINISH_MODE==='option') ? it.baseKey : it.modelKey;
    if(!map.has(key)){
      map.set(key, { mpn: it.mpn, brand: it.brand, title: it.title, tags: it.tags, colors: new Map(), finishMode: FINISH_MODE });
    }
    const g = map.get(key);

    const colorForImages = it.colorGuess || (it.variants[0]?.color) || 'STANDART';
    const nodeImg = g.colors.get(colorForImages) || { images:[], variants:[] };
    nodeImg.images = uniq(nodeImg.images.concat(it.images));
    g.colors.set(colorForImages, nodeImg);

    for(const v of it.variants){
      const c = v.color || colorForImages;
      const node = g.colors.get(c) || { images:[], variants:[] };
      const key2 = `${c}|${v.size||'Std'}|${v.sku||''}|${v.finish||'Std'}`;
      if (!node._seen) node._seen = new Set();
      if (!node._seen.has(key2)){
        node.variants.push(v);
        node._seen.add(key2);
      }
      g.colors.set(c, node);
    }
  }
  for (const g of map.values()){
    for (const [c,node] of g.colors.entries()){
      delete node._seen;
      g.colors.set(c, node);
    }
  }
  return Array.from(map.values());
}

/* ====== Metafield (image imzaları) ====== */
async function getMetaMap(productId){
  const js = await rest(`/products/${productId}/metafields.json`,'GET');
  const map = new Map();
  for(const m of (js.metafields||[])) map.set(`${m.namespace}:${m.key}`, m);
  return map;
}
async function setMeta(productId, namespace, key, value){
  await rest(`/products/${productId}/metafields.json`,'POST', {
    metafield:{ namespace, key, value: JSON.stringify(value), type:'json' }
  });
}
async function updateMeta(productId, metaMap, namespace, key, value){
  const full = `${namespace}:${key}`;
  const cur = metaMap.get(full);
  const json = JSON.stringify(value);
  if(cur){
    await rest(`/metafields/${cur.id}.json`,'PUT',{ metafield:{ id:cur.id, value: json, type:'json' } });
  }else{
    await setMeta(productId, namespace, key, value);
  }
}
function imageSign(url){ return url.replace(/^https?:\/\//,'').toLowerCase(); }

/* ====== Görseller: eksik ekle + (opsiyonel) temizlik ====== */
async function ensureImages(product, colorImagesMap){
  const metaMap = await getMetaMap(product.id);
  const current = JSON.parse(metaMap.get('sync:images')?.value || '[]');

  const wantAll = uniq([].concat(...Array.from(colorImagesMap.values()).map(x=>x.images||[])));
  const wantSet = new Set(wantAll.map(imageSign));

  const toAdd = wantAll.filter(u => !current.includes(imageSign(u)));
  for(const src of toAdd){
    try{
      await rest(`/products/${product.id}/images.json`,'POST',{ image: { src, alt: `SRC:${imageSign(src)}` } });
    }catch(e){ console.log('WARN image-add', src, String(e.message||e).slice(0,160)); }
  }

  let imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];

  if (CLEANUP_IMAGES){
    const signed = imagesFull.map(im => {
      const alt = im.alt || '';
      const sig = alt.startsWith('SRC:') ? alt.slice(4) : null;
      return { ...im, sig };
    });
    const seen = new Set();
    for(const im of signed){
      if(!im.sig) continue;
      const shouldExist = wantSet.has(im.sig);
      const isDup = seen.has(im.sig);
      if(!shouldExist || isDup){
        try{ await rest(`/products/${product.id}/images/${im.id}.json`,'DELETE'); }catch(e){ /* ignore */ }
      } else {
        seen.add(im.sig);
      }
    }
    imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];
  }

  const merged = CLEANUP_IMAGES ? Array.from(wantSet) : uniq(current.concat(wantAll.map(imageSign)));
  await updateMeta(product.id, metaMap, 'sync','images', merged);

  return imagesFull;
}

/* ====== Varyant upsert + temizlik ====== */
function keyVS(c,s,f, useFinish){ return useFinish ? `${uc(c)}|${String(s||'')}|${f||'Std'}` : `${uc(c)}|${String(s||'')}`; }

function variantPayload(v, useFinish){
  const base = {
    option1: v.color,
    option2: v.size || 'Std',
    price: to2(v.price),
    sku: v.sku || undefined,
    barcode: v.barcode || undefined,
    inventory_management: 'shopify'
  };
  return useFinish ? { ...base, option3: v.finish || 'Std' } : base;
}

async function upsertVariants(product, colorImagesMap, useFinish){
  const existing = product.variants || [];
  const byKey = new Map(existing.map(v=>{
    const k = useFinish ? keyVS(v.option1, v.option2, v.option3, true) : keyVS(v.option1, v.option2, null, false);
    return [k, v];
  }));

  const wantKeys = new Set();
  for (const [, node] of colorImagesMap.entries()){
    for (const v of (node.variants||[])){
      wantKeys.add( keyVS(v.color, v.size || 'Std', useFinish ? (v.finish||'Std') : null, useFinish) );
    }
  }

  const imagesFull = await ensureImages(product, colorImagesMap);
  const imageIdByColor = new Map();
  for(const [col, node] of colorImagesMap.entries()){
    const first = node.images?.[0];
    if(!first) continue;
    const sig = imageSign(first);
    const found = imagesFull.find(im => (im.alt||'').includes(`SRC:${sig}`));
    if(found) imageIdByColor.set(uc(col), found.id);
  }

  let totalQty = 0;
  for(const [col, node] of colorImagesMap.entries()){
    for(const v of node.variants){
      const sizeVal = v.size ? String(v.size) : 'Std';
      const k = keyVS(v.color, sizeVal, useFinish ? (v.finish||'Std') : null, useFinish);
      const ex = byKey.get(k);

      totalQty += Number(v.qty||0);

      if(ex){
        try{
          await rest(`/variants/${ex.id}.json`,'PUT',{
            variant:{
              id: ex.id,
              ...variantPayload({ ...v, size: sizeVal }, useFinish),
              image_id: imageIdByColor.get(uc(v.color)) || ex.image_id || null
            }
          });
          await setInventory(ex.inventory_item_id, v.qty);
        }catch(e){ console.log('WARN update variant', k, e.message); }
      }else{
        try{
          const add = await rest(`/products/${product.id}/variants.json`, 'POST', {
            variant: variantPayload({ ...v, size: sizeVal }, useFinish)
          });
          const nv = add?.variant;
          if(nv){
            const imgId = imageIdByColor.get(uc(v.color));
            if(imgId){ await rest(`/variants/${nv.id}.json`,'PUT',{ variant:{ id:nv.id, image_id: imgId } }); }
            await setInventory(nv.inventory_item_id, v.qty);
          }
        }catch(e){
          if(!/already exists/i.test(e.message||'')) console.log('WARN add variant', k, e.message);
        }
      }
    }
  }

  if (CLEANUP_VARIANTS){
    for(const ex of existing){
      const k = useFinish ? keyVS(ex.option1, ex.option2, ex.option3, true) : keyVS(ex.option1, ex.option2, null, false);
      if(!wantKeys.has(k)){
        try{
          if(VARIANT_DELETE){
            await rest(`/variants/${ex.id}.json`, 'DELETE');
          }else{
            await rest(`/variants/${ex.id}.json`, 'PUT', { variant:{ id: ex.id, inventory_policy: 'deny' } });
            await setInventory(ex.inventory_item_id, 0);
          }
        }catch(e){ console.log('WARN orphan variant', k, e.message); }
      }
    }
  }

  return { totalQty };
}

/* ====== Ürün oluştur/güncelle ====== */
async function createProduct(payload, seedVariant, firstImage, useFinish){
  const optionsArr = useFinish
    ? [ { name:'Renk' }, { name:'Beden' }, { name:'Finish' } ]
    : [ { name:'Renk' }, { name:'Beden' } ];

  const seed = variantPayload(seedVariant, useFinish);

  const js = await rest(`/products.json`,'POST',{
    product: {
      ...payload,
      options: optionsArr,
      variants: [ seed ],
      images: firstImage ? [{ src: firstImage, alt: `SRC:${imageSign(firstImage)}` }] : []
    }
  });
  return js?.product;
}
async function updateProduct(id, payload){
  const js = await rest(`/products/${id}.json`,'PUT',{ product: { id, ...payload } });
  return js?.product;
}

function pickSeedVariant(colorsMap){
  for(const [, node] of colorsMap.entries()){
    if(node.variants && node.variants.length){
      const v = node.variants[0];
      return { ...v, size: v.size || 'Std' };
    }
  }
  return { color:'STANDART', size:'Std', price:0, qty:0, sku:'', barcode:'', finish:'Std' };
}
function pickFirstImage(colorsMap){
  for(const [, node] of colorsMap.entries()){
    const img = node.images?.[0];
    if(img) return img;
  }
  return null;
}

/* ====== Main ====== */
async function loadIndex(){ return await loadProductIndex(); }

async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.error('Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL');
    process.exit(1);
  }

  console.log('XML okunuyor…');
  const xml = await (await fetch(SOURCE_URL)).text();
  const raw = parseXML(xml);
  const groups = groupByModel(raw);

  console.log('Model sayısı:', groups.length);
  if (DIAG){
    for (const g of groups){
      const colors = Array.from(g.colors.entries()).map(([c,n])=>{
        const sizes = uniq(n.variants.map(v=>v.size||'Std')).join('/');
        const fins  = uniq(n.variants.map(v=>v.finish||'Std')).join('/');
        return `${c}(${n.variants.length}v,size:${sizes},finish:${fins})`;
      }).join(', ');
      console.log(`GRUP: ${g.brand}|${g.mpn} [mode:${g.finishMode}] -> ${colors}`);
    }
  }

  const index = await loadIndex();

  let processed = 0;
  for(const g of groups){
    // split: tag model:<brand>|<mpn>|finish:Parlak   option: model:<brand>|<mpn>
    const tagModel = Array.from(new Set((g.tags||[]).filter(t=>t.startsWith('model:')))).find(Boolean) || `model:${g.brand}|${g.mpn}`;

    const productPayloadBase = {
      title: g.title,
      body_html: '',
      vendor: g.brand,
      product_type: 'Ayakkabı',
      tags: uniq([...(g.tags||[]), tagModel]).join(', '),
      status: 'active'
    };

    const found = index.get(tagModel);
    const useFinish = (FINISH_MODE==='option');

    let prod;
    if(!found){
      const seed = pickSeedVariant(g.colors);
      const firstImg = pickFirstImage(g.colors);
      prod = await createProduct(productPayloadBase, seed, firstImg, useFinish);
      if(!prod) continue;
      if(prod?.variants?.[0]) await setInventory(prod.variants[0].inventory_item_id, seed.qty);
      await publishProduct(prod.id);
    }else{
      prod = await updateProduct(found.id, { ...productPayloadBase });
      await publishProduct(found.id);
      prod = { ...found, ...prod };
    }

    const { totalQty } = await upsertVariants(prod, g.colors, useFinish);

    // stok etiketi
    const wantOpen = totalQty > 0;
    const tagsArr = new Set((productPayloadBase.tags || '').split(',').map(s=>s.trim()).filter(Boolean));
    if(wantOpen){ tagsArr.delete('satis:kapali'); tagsArr.add('satis:acik'); }
    else        { tagsArr.delete('satis:acik');   tagsArr.add('satis:kapali'); }
    await updateProduct(prod.id, { tags: Array.from(tagsArr).join(', ') });

    processed++;
    if(processed % BATCH_SIZE === 0) await sleep(800);
    console.log(`OK: ${clip(g.title,80)} | Stok:${totalQty}`);
  }

  console.log('Bitti. İşlenen model:', processed);
}

main().catch(e=>{ console.error('HATA:', e.message||e); process.exit(1); });
