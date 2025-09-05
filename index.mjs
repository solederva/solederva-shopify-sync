import { XMLParser } from 'fast-xml-parser';

/* ====== Config (Actions Secrets) ====== */
const SHOP_DOMAIN    = process.env.SHOP_DOMAIN;      // ör: "shkx8d-wy"
const ACCESS_TOKEN   = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION    = "2024-07";
const SOURCE_URL     = process.env.SOURCE_URL;
const PRIMARY_DOMAIN = process.env.PRIMARY_DOMAIN || "www.solederva.com";
const BATCH_SIZE     = Number(process.env.BATCH_SIZE || 25);

/* ---- Temizlik modları ---- */
const CLEANUP_IMAGES   = /^(1|true|yes)$/i.test(process.env.CLEANUP_IMAGES   || '');
const CLEANUP_VARIANTS = /^(1|true|yes)$/i.test(process.env.CLEANUP_VARIANTS || '');
const VARIANT_DELETE   = /^(1|true|yes)$/i.test(process.env.VARIANT_DELETE   || '');

/* ---- Throttle / Backoff ---- */
const QPS              = Number(process.env.QPS || 3); // saniyede en çok istek
const MAX_RETRY        = 7;
const BASE_BACKOFF_MS  = 1200; // 429/5xx sonrası taban bekleme
let   lastRequestAt    = 0;

const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const num   = (x)=> { if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=> { const n=num(x); return n ? n.toFixed(2) : "0.00"; };
const uc    = (s)=> (s||"").toString().trim().toUpperCase();
const uniq  = (arr)=> Array.from(new Set(arr.filter(Boolean)));
const clip  = (s,m)=> { s=String(s||"").trim(); return s.length>m? s.slice(0,m-1)+"…": s; };

function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if(json) h["Content-Type"] = "application/json";
  return h;
}

async function throttleGate(){
  const minGap = Math.max(10, Math.floor(1000 / Math.max(1, QPS)));
  const now = Date.now();
  const wait = Math.max(0, (lastRequestAt + minGap) - now);
  if (wait) await sleep(wait + Math.floor(Math.random()*60)); // jitter
  lastRequestAt = Date.now();
}

async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;

  for (let attempt = 0; attempt < MAX_RETRY; attempt++){
    await throttleGate();

    const res = await fetch(url, {
      method,
      headers: headers(true),
      body: body ? JSON.stringify(body) : undefined
    });

    // 429 / 5xx → backoff + retry
    if (res.status === 429 || res.status >= 500){
      const ra = Number(res.headers.get('Retry-After')) || 0;
      const backoff = ra > 0
        ? ra * 1000
        : Math.min(BASE_BACKOFF_MS * Math.pow(1.6, attempt), 15000);
      await sleep(backoff + Math.floor(Math.random()*200));
      continue;
    }

    // Diğer hatalar → metinle fırlat
    if (!res.ok){
      const t = await res.text().catch(()=>res.statusText);
      throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${t}`);
    }

    // Güvenli JSON parse
    const text = await res.text().catch(()=>null);
    if (!text) return {};
    try { return JSON.parse(text); } catch { return {}; }
  }

  throw new Error(`Shopify ${method} ${path} -> too many retries`);
}

/* ---------- Publish (izin varsa) + fallback ---------- */
async function getOnlineStorePublicationId(){
  const pubs = await rest(`/publications.json`, 'GET');
  const pub = (pubs?.publications||[]).find(p=>/online/i.test(p.name));
  return pub?.id;
}
async function fallbackMarkWeb(productId){
  try {
    await rest(`/products/${productId}.json`,'PUT',{ product:{ id: productId, status:'active', published_scope:'web' } });
  } catch(e){
    console.log('PUBLISH-FALLBACK WARN:', String(e.message||e).slice(0,160));
  }
}
async function publishProduct(productId){
  try{
    const pubId = await getOnlineStorePublicationId();
    if(!pubId) throw new Error('No publication id');
    await rest(`/publications/${pubId}/publish.json`, 'POST', {
      publication: { publishable_id: productId, publishable_type: "product" }
    });
  }catch(e){
    console.log('PUBLISH via publications failed -> fallback web publish');
    await fallbackMarkWeb(productId);
  }
}

/* ---------- Lokasyon / stok ---------- */
let LOCATION_ID_CACHE = null;
async function getLocationId(){
  if(LOCATION_ID_CACHE) return LOCATION_ID_CACHE;
  const js = await rest(`/locations.json`,'GET');
  LOCATION_ID_CACHE = js?.locations?.[0]?.id;
  return LOCATION_ID_CACHE;
}
async function setInventory(inventoryItemId, qty){
  const locationId = await getLocationId();
  if(!locationId || !inventoryItemId) return;
  await rest(`/inventory_levels/set.json`,'POST',{
    location_id: locationId,
    inventory_item_id: inventoryItemId,
    available: Number(qty||0)
  });
}

/* ---------- Mevcut ürün indeksini yükle (idempotent) ---------- */
async function loadProductIndex(){
  const map = new Map(); // model:<brand>|<baseTitle> -> product
  let pageInfo = null;
  while(true){
    const url = `/products.json?limit=250${pageInfo?`&page_info=${pageInfo}`:''}&fields=id,title,handle,tags,images,variants,status`;
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

/* ---------- XML ayrıştırma ve gruplama ---------- */
const COLOR_WORDS = ['SIYAH','BEYAZ','KAHVE','LACIVERT','TABA','GRI','ANTRASIT','SAX','BEYAZ/SIYAH','SIYAH/BEYAZ','BYZ','AT','KE'];

function splitTitle(nameRaw){
  const s = uc(nameRaw||"").replace(/\s+/g,' ').trim();
  const parts = s.split(' ');
  let color = parts[parts.length-1];
  if(!COLOR_WORDS.includes(color)) color = '';
  const base = color ? s.slice(0, s.lastIndexOf(' '+color)) : s;
  return { baseTitle: base, color };
}
function readableTitle(base, brand, mpn){
  const pretty = base.replace(/^MN\d+\s*-\s*/,'').trim();
  return `${pretty} – ${brand} ${mpn}`.replace(/\s+/g,' ').trim();
}

function parseXML(xml){
  const parser = new XMLParser({ ignoreAttributes:true, attributeNamePrefix:'', trimValues:true });
  const root = parser.parse(xml)||{};
  const list = [].concat(root?.Products?.Product||[]);
  return list.map(p=>{
    const mpn       = (p.Mpn||'').trim();
    const brand     = (p.Brand||'').trim() || 'MOOİEN';
    const name      = (p.Name||'').trim();
    const { baseTitle, color } = splitTitle(name);
    const title     = readableTitle(baseTitle, brand, mpn);
    const price     = num(p.Price);
    const tax       = num(p.Tax);
    const images    = uniq([p.Image1,p.Image2,p.Image3,p.Image4,p.Image5]).filter(Boolean);
    const mainCat   = (p.mainCategory||'').trim();
    const cat       = (p.category||'').trim();
    const variants  = []
      .concat(p?.variants?.variant||[])
      .map(v=>{
        const specs = [].concat(v?.spec||[]);
        const renk  = uc(specs.find(s=>s?.name==='Renk')?.['#text'] || color || 'STANDART');
        const beden = String(specs.find(s=>s?.name==='Beden')?.['#text'] || '').trim();
        return {
          color : renk,
          size  : beden,
          sku   : String(v.productCode||v.barcode||v.variantId||'').trim(),
          barcode: String(v.barcode||'').trim(),
          qty   : num(v.quantity),
          price : num(v.price) || price
        };
      });

    const modelKey  = `model:${brand}|${baseTitle}`;
    const tags      = uniq([
      modelKey,
      `brand:${brand}`,
      `kategori:${/BOT/i.test(cat+mainCat)?'bot':/SPOR/i.test(cat+mainCat)?'spor':'klasik'}`,
      'satis:acik'
    ]);

    return { mpn, brand, name, baseTitle, title, price, tax, images, variants, modelKey, tags, colorDefault: color };
  });
}

function groupByModel(items){
  const map = new Map();
  for(const it of items){
    const k = it.modelKey;
    if(!map.has(k)) map.set(k, { ...it, colors: new Map() });
    const g = map.get(k);
    const keyColor = it.colorDefault || 'STANDART';
    const node = g.colors.get(keyColor) || { images:[], variants:[] };
    node.images = uniq(node.images.concat(it.images));
    node.variants = node.variants.concat(it.variants.filter(v=>uc(v.color)===uc(keyColor)));
    g.colors.set(keyColor, node);
  }
  return Array.from(map.values());
}

/* ---------- Metafield (image imzaları) ---------- */
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

/* ---------- Görseller: eksik ekle + (opsiyonel) temizlik ---------- */
async function ensureImages(product, colorImagesMap){
  const metaMap = await getMetaMap(product.id);
  const current = JSON.parse(metaMap.get('sync:images')?.value || '[]');

  const wantAll = uniq([].concat(...Array.from(colorImagesMap.values()).map(x=>x.images||[])));
  const wantSet = new Set(wantAll.map(imageSign));

  // Eksik olanları ekle
  const toAdd = wantAll.filter(u => !current.includes(imageSign(u)));
  for(const src of toAdd){
    try{
      await rest(`/products/${product.id}/images.json`,'POST',{ image: { src, alt: `SRC:${imageSign(src)}` } });
    }catch(e){
      console.log('WARN image-add', src, String(e.message||e).slice(0,160));
    }
  }

  // Güncel listeyi çek
  let imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];

  if (CLEANUP_IMAGES){
    // Sadece imzalı (alt startsWith SRC:) resimler arasında temizlik
    const signed = imagesFull.map(im => {
      const alt = im.alt || '';
      const sig = alt.startsWith('SRC:') ? alt.slice(4) : null;
      return { ...im, sig };
    });

    // Aynı imzadan fazla varsa ilkini bırak, diğerlerini sil
    const seen = new Set();
    for(const im of signed){
      if(!im.sig) continue; // imzasız = elle eklenmiş → dokunma
      const shouldExist = wantSet.has(im.sig);
      const isDup = seen.has(im.sig);
      if(!shouldExist || isDup){
        try{
          await rest(`/products/${product.id}/images/${im.id}.json`,'DELETE');
        }catch(e){
          console.log('WARN image-del', im.id, String(e.message||e).slice(0,160));
        }
      } else {
        seen.add(im.sig);
      }
    }
    // Silme sonrası tekrar çek
    imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];
  }

  // Metafield güncelle
  const merged = CLEANUP_IMAGES ? Array.from(wantSet) : uniq(current.concat(wantAll.map(imageSign)));
  await updateMeta(product.id, metaMap, 'sync','images', merged);

  return imagesFull;
}

/* ---------- Varyant idempotent + temizlik ---------- */
function keyVS(c,s){ return `${uc(c)}|${String(s||'')}`; }
function variantPayload(v){
  return {
    option1: v.color,
    option2: v.size || 'Std',
    price: to2(v.price),
    sku: v.sku || undefined,
    barcode: v.barcode || undefined,
    inventory_management: 'shopify'
  };
}

async function upsertVariants(product, colorImagesMap){
  const existing = product.variants || [];
  const byKey = new Map(existing.map(v=>[ keyVS(v.option1, v.option2), v ]));

  const wantKeys = new Set();
  for (const [, node] of colorImagesMap.entries()){
    for (const v of (node.variants||[])){
      wantKeys.add( keyVS(v.color, v.size || 'Std') );
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

  // Ekle/güncelle
  let totalQty = 0;
  for(const [col, node] of colorImagesMap.entries()){
    for(const v of node.variants){
      const sizeVal = v.size ? String(v.size) : 'Std';
      const k = keyVS(v.color, sizeVal);
      const ex = byKey.get(k);

      totalQty += Number(v.qty||0);

      if(ex){
        try{
          await rest(`/variants/${ex.id}.json`,'PUT',{
            variant:{
              id: ex.id,
              ...variantPayload({ ...v, size: sizeVal }),
              image_id: imageIdByColor.get(uc(v.color)) || ex.image_id || null
            }
          });
          await setInventory(ex.inventory_item_id, v.qty);
        }catch(e){ console.log('WARN update variant', k, e.message); }
      }else{
        try{
          const add = await rest(`/products/${product.id}/variants.json`, 'POST', { variant: variantPayload({ ...v, size: sizeVal }) });
          const nv = add?.variant;
          if(nv){
            const imgId = imageIdByColor.get(uc(v.color));
            if(imgId){
              await rest(`/variants/${nv.id}.json`,'PUT',{ variant:{ id:nv.id, image_id: imgId } });
            }
            await setInventory(nv.inventory_item_id, v.qty);
          }
        }catch(e){
          if(!/already exists/i.test(e.message||'')) console.log('WARN add variant', k, e.message);
        }
      }
    }
  }

  // TEMİZLİK: XML’de olmayan varyantlar
  if (CLEANUP_VARIANTS){
    for(const ex of existing){
      const k = keyVS(ex.option1, ex.option2);
      if(!wantKeys.has(k)){
        try{
          if(VARIANT_DELETE){
            await rest(`/variants/${ex.id}.json`, 'DELETE');
          }else{
            await rest(`/variants/${ex.id}.json`, 'PUT', {
              variant:{ id: ex.id, inventory_policy: 'deny' }
            });
            await setInventory(ex.inventory_item_id, 0);
          }
        }catch(e){
          console.log('WARN orphan variant', k, e.message);
        }
      }
    }
  }

  return { totalQty };
}

/* ---------- Ürün oluştur/güncelle ---------- */
async function createProduct(payload, seedVariant, firstImage){
  const js = await rest(`/products.json`,'POST',{
    product: {
      ...payload,
      options: [ { name: 'Color' }, { name: 'Size' } ],
      variants: [ variantPayload(seedVariant) ],
      images: firstImage ? [{ src: firstImage, alt: `SRC:${imageSign(firstImage)}` }] : []
    }
  });
  return js?.product;
}
async function updateProduct(id, payload){
  const js = await rest(`/products/${id}.json`,'PUT',{ product: { id, ...payload } });
  return js?.product;
}

/* ----- Ana iş ----- */
function pickSeedVariant(colorsMap){
  for(const [, node] of colorsMap.entries()){
    if(node.variants && node.variants.length){
      const v = node.variants[0];
      return { ...v, size: v.size || 'Std' };
    }
  }
  return { color:'Default', size:'Std', price:0, qty:0, sku:'', barcode:'' };
}
function pickFirstImage(colorsMap){
  for(const [, node] of colorsMap.entries()){
    const img = node.images?.[0];
    if(img) return img;
  }
  return null;
}

async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.error('Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL');
    process.exit(1);
  }

  console.log('XML okunuyor…');
  const xml = await (await fetch(SOURCE_URL)).text();
  const rawItems = parseXML(xml);
  const groups = groupByModel(rawItems);

  console.log('Model sayısı:', groups.length);

  const index = await loadProductIndex();

  let processed = 0;
  for(const g of groups){
    const title  = g.title;
    const tagModel = g.modelKey;

    const productPayloadBase = {
      title,
      body_html: '',
      vendor: g.brand,
      product_type: 'Ayakkabı',
      tags: g.tags.join(', '),
      status: 'active'
    };

    const found = index.get(tagModel);

    let prod;
    if(!found){
      const seed = pickSeedVariant(g.colors);
      const firstImg = pickFirstImage(g.colors);
      prod = await createProduct(productPayloadBase, seed, firstImg);
      if(!prod) continue;
      if(prod?.variants?.[0]) await setInventory(prod.variants[0].inventory_item_id, seed.qty);
      await publishProduct(prod.id);
    }else{
      prod = await updateProduct(found.id, { ...productPayloadBase });
      await publishProduct(found.id);
      prod = { ...found, ...prod };
    }

    const { totalQty } = await upsertVariants(prod, g.colors);

    // stok etiketleri
    const wantOpen = totalQty > 0;
    const tagsArr = new Set((productPayloadBase.tags || '').split(',').map(s=>s.trim()).filter(Boolean));
    if(wantOpen){ tagsArr.delete('satis:kapali'); tagsArr.add('satis:acik'); }
    else        { tagsArr.delete('satis:acik');   tagsArr.add('satis:kapali'); }
    await updateProduct(prod.id, { tags: Array.from(tagsArr).join(', ') });

    processed++;
    if(processed % BATCH_SIZE === 0) await sleep(1000);
    console.log(`OK: ${clip(title,80)} | Stok:${totalQty}`);
  }

  console.log('Bitti. İşlenen model:', processed);
}

main().catch(e=>{ console.error('HATA:', e.message||e); process.exit(1); });
