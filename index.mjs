import { XMLParser } from 'fast-xml-parser';

/* ====== ENV ====== */
const SHOP_DOMAIN   = process.env.SHOP_DOMAIN;           // ör: "shkx8d-wy"
const ACCESS_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION   = "2024-07";
const SOURCE_URL    = process.env.SOURCE_URL;
const BATCH_SIZE    = Number(process.env.BATCH_SIZE || 40);
const QPS           = Number(process.env.QPS || 3);      // istek/sn sınırı
const MAX_RETRY     = 7;
const BASE_BACKOFF  = 1200;

// Bakım bayrakları (opsiyonel)
const CLEANUP_IMAGES   = /^(1|true|yes)$/i.test(process.env.CLEANUP_IMAGES   || '');
const CLEANUP_VARIANTS = /^(1|true|yes)$/i.test(process.env.CLEANUP_VARIANTS || '');
const VARIANT_DELETE   = /^(1|true|yes)$/i.test(process.env.VARIANT_DELETE   || '');

// Teşhis log’u
const DIAG = /^(1|true|yes)$/i.test(process.env.DIAG || '');

/* ====== Utils ====== */
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const num   = (x)=>{ if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=>{ const n=num(x); return n ? n.toFixed(2) : "0.00"; };
const uc    = (s)=>(s||'').toString().trim().toUpperCase();
const uniq  = (a)=>Array.from(new Set((a||[]).filter(Boolean)));
const clip  = (s,m=90)=>{ s=String(s||''); return s.length>m? s.slice(0,m-1)+'…': s; };
const norm  = (s)=>String(s??'').replace(/\s+/g,' ').trim();

function toArr(x){ return x==null ? [] : (Array.isArray(x) ? x : [x]); }

/* ====== Shopify REST + throttle/backoff ====== */
function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if(json) h["Content-Type"] = "application/json";
  return h;
}
let lastAt = 0;
async function throttle(){
  const gap = Math.max(12, Math.floor(1000/Math.max(1,QPS)));
  const wait = Math.max(0, (lastAt + gap) - Date.now());
  if(wait) await sleep(wait + Math.floor(Math.random()*60));
  lastAt = Date.now();
}
async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  for(let i=0;i<MAX_RETRY;i++){
    await throttle();
    const res = await fetch(url, { method, headers: headers(true), body: body?JSON.stringify(body):undefined });
    if(res.status===429 || res.status>=500){
      const ra = Number(res.headers.get('Retry-After'))||0;
      const backoff = ra? ra*1000 : Math.min(BASE_BACKOFF*Math.pow(1.6,i), 15000);
      await sleep(backoff + Math.floor(Math.random()*250));
      continue;
    }
    if(!res.ok){ throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${await res.text()}`); }
    const text = await res.text().catch(()=>null);
    if(!text) return {};
    try{ return JSON.parse(text); }catch{ return {}; }
  }
  throw new Error(`Shopify ${method} ${path} -> too many retries`);
}

/* ====== Yayınlama (izin yoksa fallback) ====== */
async function getPublicationId(){
  try{
    const pubs = await rest(`/publications.json`,'GET');
    return (pubs.publications||[]).find(p=>/online/i.test(p.name))?.id;
  }catch{ return null; }
}
async function publishProduct(productId){
  try{
    const pid = await getPublicationId();
    if(!pid) throw 0;
    await rest(`/publications/${pid}/publish.json`,'POST',{
      publication: { publishable_id: productId, publishable_type: "product" }
    });
  }catch{
    try{
      await rest(`/products/${productId}.json`,'PUT',{ product:{ id:productId, status:'active', published_scope:'web' } });
    }catch(e){ console.log('PUBLISH-FALLBACK WARN:', String(e.message||e).slice(0,160)); }
  }
}

/* ====== Stok/Lokasyon ====== */
let LOCATION_ID=null;
async function getLocationId(){
  if(LOCATION_ID) return LOCATION_ID;
  const js = await rest(`/locations.json`,'GET');
  LOCATION_ID = js.locations?.[0]?.id;
  return LOCATION_ID;
}
async function setInventory(inventoryItemId, qty){
  const lid = await getLocationId();
  if(!lid || !inventoryItemId) return;
  await rest(`/inventory_levels/set.json`,'POST',{
    location_id: lid, inventory_item_id: inventoryItemId, available: Number(qty||0)
  });
}

/* ====== Mevcut ürün indeksini yükle (model:* tag) ====== */
async function loadProductIndex(){
  const map = new Map(); // model:<brand>|<family>  -> product
  let page = null;
  while(true){
    const q = `/products.json?limit=250${page?`&page_info=${page}`:''}&fields=id,title,tags,images,variants,status`;
    const res = await fetch(`https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${q}`, { headers: headers(false) });
    if(!res.ok) throw new Error(`Shopify GET ${q} -> ${res.status}`);
    const link = res.headers.get('link')||'';
    const js = await res.json();
    for(const p of (js.products||[])){
      const tag = (p.tags||'').split(',').map(t=>t.trim()).find(t=>t.startsWith('model:'));
      if(tag) map.set(tag, p);
    }
    const m = link.match(/<[^>]+page_info=([^>]+)>; rel="next"/);
    if(m){ page = m[1]; await sleep(120); } else break;
  }
  return map;
}

/* ====== Sınıflandırma ve başlık ====== */
function classify(main, cat){
  const s = uc((main||'')+' '+(cat||''));          // SPOR / BOT / DERİ AYAKKABI vb.
  if(/\bBOT\b/.test(s))  return { type:'Bot',             tag:'bot' };
  if(/\bSPOR\b/.test(s)) return { type:'Spor Ayakkabı',   tag:'spor' };
  return { type:'Klasik Ayakkabı', tag:'klasik' };       // DERİ vb. -> Klasik
}

// Aile (family) kodu: MN2505..., Name başındaki "MN2505 - ..." gibi
function extractFamily(name, mpn, productCode){
  const candidates = [];
  if(name){ const m = String(name).match(/^\s*([A-Z]{2}\d{3,5})\s*-/); if(m) candidates.push(m[1]); }
  if(mpn){  const m = String(mpn).match(/^([A-Z]{2}\d{3,5})/);         if(m) candidates.push(m[1]); }
  if(productCode){ const m = String(productCode).match(/^([A-Z]{2}\d{3,5})/); if(m) candidates.push(m[1]); }
  return candidates[0] || (String(mpn||productCode||'').slice(0,6));
}

function extractBaseColor(nameRaw){
  const s = uc(nameRaw||'').replace(/[^\wÇĞİÖŞÜ\/\s-]/g,' ').replace(/\s+/g,' ').trim();
  // Çoğu isimde renk sonda: "... SIYAH", "... BEYAZ" v.s.
  const parts = s.split(' ');
  let last = parts[parts.length-1];
  const COLORS = ['SIYAH','BEYAZ','KAHVE','LACIVERT','TABA','GRI','ANTRASIT','SAX','BYZ','SIYAH/BEYAZ','BEYAZ/SIYAH'];
  let color = COLORS.includes(last) ? last : '';
  const base = color ? s.slice(0, s.lastIndexOf(' '+color)).trim() : s;
  return { baseTitle: base, colorGuess: color };
}

function detectFinish(name, desc){
  const s = uc((name||'')+' '+(desc||''));
  if(/\bVERNICIATA\b/.test(s) || /\bPARLAK\b/.test(s)) return 'Parlak';
  if(/\bMAT\b/.test(s)) return 'Mat';
  return null;
}

function niceTitle(base, brand, family){
  // "DST Classica Pelle Di Cuoio Erkek Ayakkabı – MN003 (MOOİEN)"
  const human = base.replace(/^[A-Z]{2}\d{3,5}\s*-\s*/,'').trim();
  return `${human} – ${family} (${brand})`;
}

/* ====== XML parse ====== */
function specText(specObj){
  if(specObj==null) return '';
  if(typeof specObj==='string' || typeof specObj==='number') return String(specObj);
  if(typeof specObj==='object'){
    if('#text' in specObj) return String(specObj['#text']);
    if('text' in specObj)  return String(specObj.text);
    if('value' in specObj) return String(specObj.value);
  }
  return '';
}
function readSpec(specs, wanted){
  const hit = specs.find(s => uc(s?.name||s?.['@_name']||'') === uc(wanted));
  return specText(hit).trim();
}

function parseXML(xml){
  const parser = new XMLParser({ ignoreAttributes:true, attributeNamePrefix:'', trimValues:true });
  const root = parser.parse(xml)||{};
  const list = [].concat(root?.Products?.Product||[]);
  return list.map(p=>{
    const mpn   = norm(p.Mpn||'');
    const name  = norm(p.Name||'');
    const brand = norm(p.Brand||'') || 'MOOİEN';
    const prodCode = norm(p.Product_code||'');
    const { baseTitle, colorGuess } = extractBaseColor(name);
    const family = extractFamily(name, mpn, prodCode) || mpn || prodCode;
    const finish = detectFinish(name, p.Description);
    const klass  = classify(p.mainCategory, p.category);

    const price = num(p.Price);
    const tax   = num(p.Tax);
    const imgs  = uniq([p.Image1,p.Image2,p.Image3,p.Image4,p.Image5].filter(Boolean));
    const descHtml = p.Description || '';

    const variants = toArr(p?.variants?.variant).map(v=>{
      const specs = toArr(v?.spec);
      const renk  = uc(readSpec(specs,'Renk') || colorGuess || 'RENK');
      const beden = norm(readSpec(specs,'Beden'));
      const vPrice = num(v.price) || price;

      return {
        color   : renk,
        size    : beden || 'Std',
        finish  : finish || 'Std',
        sku     : String(v.productCode||v.barcode||v.variantId||'').trim(),
        barcode : String(v.barcode||'').trim(),
        qty     : num(v.quantity),
        price   : vPrice
      };
    });

    const title = niceTitle(baseTitle, brand, family);
    const tagModel = `model:${brand}|${family}`;
    const tags = uniq([
      tagModel,
      `brand:${brand}`,
      `kategori:${klass.tag}`,
      finish ? `finish:${finish}` : null,
      'satis:acik'
    ]);

    return { family, brand, baseTitle, colorGuess, finish, title, price, tax, images:imgs, variants, tagModel, tags, descHtml, product_type: klass.type };
  });
}

/* ====== Aile halinde gruplama ====== */
function groupByFamily(items){
  const map = new Map();
  for(const it of items){
    const key = `${it.brand}|${it.family}`;
    if(!map.has(key)){
      map.set(key, {
        family: it.family, brand: it.brand, title: it.title,
        colors: new Map(), tags: new Set(it.tags||[]),
        descHtml: it.descHtml, product_type: it.product_type
      });
    }
    const g = map.get(key);

    const colorForImages = it.colorGuess || it.variants[0]?.color || 'RENK';
    const nodeImg = g.colors.get(colorForImages) || { images:[], variants:[] };
    nodeImg.images = uniq(nodeImg.images.concat(it.images));
    g.colors.set(colorForImages, nodeImg);

    for(const v of it.variants){
      const c = v.color || colorForImages;
      const node = g.colors.get(c) || { images:[], variants:[] };
      const dedupKey = `${c}|${v.size}|${v.sku}`;
      if(!node._seen) node._seen = new Set();
      if(!node._seen.has(dedupKey)){
        node.variants.push(v);
        node._seen.add(dedupKey);
      }
      g.colors.set(c, node);
    }
    (it.tags||[]).forEach(t=>g.tags.add(t));
  }
  for(const g of map.values()){
    for(const [c,node] of g.colors.entries()){ delete node._seen; g.colors.set(c,node); }
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
  await rest(`/products/${productId}/metafields.json`,'POST',{
    metafield:{ namespace, key, value: JSON.stringify(value), type:'json' }
  });
}
async function updateMeta(productId, metaMap, ns, key, value){
  const id = metaMap.get(`${ns}:${key}`)?.id;
  const val = JSON.stringify(value);
  if(id){
    await rest(`/metafields/${id}.json`,'PUT',{ metafield:{ id, value: val, type:'json' } });
  }else{
    await setMeta(productId, ns, key, value);
  }
}
const sign = (url)=> String(url||'').replace(/^https?:\/\//,'').toLowerCase();

/* ====== Görseller: eksik ekle + (opsiyonel) temizlik ====== */
async function ensureImages(product, colorMap){
  const meta = await getMetaMap(product.id);
  const current = JSON.parse(meta.get('sync:images')?.value || '[]');

  const wantAll = uniq([].concat(...Array.from(colorMap.values()).map(x=>x.images||[])));
  const wantSet = new Set(wantAll.map(sign));

  const toAdd = wantAll.filter(u=>!current.includes(sign(u)));
  for(const src of toAdd){
    try{
      await rest(`/products/${product.id}/images.json`,'POST',{ image:{ src, alt:`SRC:${sign(src)}` } });
    }catch(e){ console.log('WARN image-add', src, String(e.message||e).slice(0,160)); }
  }

  let images = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];

  if(CLEANUP_IMAGES){
    const signed = images.map(im=>{
      const alt = im.alt||'';
      const s = alt.startsWith('SRC:') ? alt.slice(4) : null;
      return { ...im, sig:s };
    });
    const seen = new Set();
    for(const im of signed){
      if(!im.sig) continue;
      const should = wantSet.has(im.sig);
      const dup = seen.has(im.sig);
      if(!should || dup){
        try{ await rest(`/products/${product.id}/images/${im.id}.json`,'DELETE'); }catch{}
      }else{
        seen.add(im.sig);
      }
    }
    images = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];
  }

  const merged = CLEANUP_IMAGES ? Array.from(wantSet) : uniq(current.concat(wantAll.map(sign)));
  await updateMeta(product.id, meta, 'sync','images', merged);

  return images;
}

/* ====== Variant upsert ====== */
function keyVS(c,s){ return `${uc(c)}|${String(s||'Std')}`; }
function varPayload(v){
  return {
    option1: v.color,
    option2: v.size || 'Std',
    price  : to2(v.price),
    sku    : v.sku || undefined,
    barcode: v.barcode || undefined,
    inventory_management: 'shopify'
  };
}

async function upsertVariants(product, colorMap){
  const existing = product.variants||[];
  const byKey = new Map(existing.map(v=>[ keyVS(v.option1, v.option2), v ]));

  const wantKeys = new Set();
  for(const [, node] of colorMap.entries()){
    for(const v of (node.variants||[])){
      wantKeys.add(keyVS(v.color, v.size));
    }
  }

  const images = await ensureImages(product, colorMap);
  const imageIdByColor = new Map();
  for(const [col, node] of colorMap.entries()){
    const first = node.images?.[0];
    if(!first) continue;
    const sig = sign(first);
    const found = images.find(im => (im.alt||'').includes(`SRC:${sig}`));
    if(found) imageIdByColor.set(uc(col), found.id);
  }

  let totalQty = 0;

  for(const [col, node] of colorMap.entries()){
    for(const v of node.variants){
      const k = keyVS(v.color, v.size);
      totalQty += Number(v.qty||0);

      const ex = byKey.get(k);
      if(ex){
        try{
          await rest(`/variants/${ex.id}.json`,'PUT',{
            variant:{ id: ex.id, ...varPayload(v), image_id: imageIdByColor.get(uc(v.color)) || ex.image_id || null }
          });
          await setInventory(ex.inventory_item_id, v.qty);
        }catch(e){ console.log('WARN update variant', k, e.message); }
      }else{
        try{
          const add = await rest(`/products/${product.id}/variants.json`,'POST',{ variant: varPayload(v) });
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

  if(CLEANUP_VARIANTS){
    for(const ex of existing){
      const k = keyVS(ex.option1, ex.option2);
      if(!wantKeys.has(k)){
        try{
          if(VARIANT_DELETE){
            await rest(`/variants/${ex.id}.json`,'DELETE');
          }else{
            await rest(`/variants/${ex.id}.json`,'PUT',{ variant:{ id: ex.id, inventory_policy:'deny' } });
            await setInventory(ex.inventory_item_id, 0);
          }
        }catch(e){ console.log('WARN orphan variant', k, e.message); }
      }
    }
  }

  return { totalQty };
}

/* ====== Ürün oluştur/güncelle ====== */
async function createProduct(basePayload, seedVariant, firstImage, wantSizes){
  const options = wantSizes ? [ {name:'Renk'}, {name:'Beden'} ] : [ {name:'Renk'} ];
  const seed = { ...varPayload(seedVariant) };
  if(!wantSizes) delete seed.option2;

  const js = await rest(`/products.json`,'POST',{
    product:{
      ...basePayload,
      options,
      variants: [ seed ],
      images: firstImage ? [{ src:firstImage, alt:`SRC:${sign(firstImage)}` }] : []
    }
  });
  return js?.product;
}
async function updateProduct(id, payload){
  const js = await rest(`/products/${id}.json`,'PUT',{ product:{ id, ...payload } });
  return js?.product;
}

function pickSeedVariant(colorMap){
  for(const [, node] of colorMap.entries()){
    if(node.variants?.length) return node.variants[0];
  }
  return { color:'RENK', size:'Std', price:0, qty:0, sku:'', barcode:'' };
}
function pickFirstImage(colorMap){
  for(const [, node] of colorMap.entries()){
    const img = node.images?.[0];
    if(img) return img;
  }
  return null;
}
function hasRealSizes(colorMap){
  for(const [,node] of colorMap.entries()){
    if((node.variants||[]).some(v=> (v.size && v.size!=='Std'))) return true;
  }
  return false;
}

/* ====== MAIN ====== */
async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.error('Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL');
    process.exit(1);
  }

  console.log('XML okunuyor…');
  const xml = await (await fetch(SOURCE_URL)).text();
  const parsed = parseXML(xml);
  const groups = groupByFamily(parsed);

  if(DIAG){
    for(const g of groups){
      const colors = Array.from(g.colors.entries()).map(([c,n])=>{
        const sizes = uniq(n.variants.map(v=>v.size)).join('/');
        return `${c}(${n.variants.length}v; ${sizes})`;
      }).join(', ');
      console.log(`GRUP ${g.brand}|${g.family} -> ${colors}`);
    }
  }

  const index = await loadProductIndex();

  console.log('Model sayısı:', groups.length);
  let processed = 0;

  for(const g of groups){
    const tagModel = `model:${g.brand}|${g.family}`;

    const basePayload = {
      title: g.title,
      body_html: g.descHtml || '',
      vendor: g.brand,
      product_type: g.product_type,                      // Spor / Klasik / Bot
      tags: Array.from(g.tags||[]).join(', '),
      status: 'active'
    };

    const wantSizes = hasRealSizes(g.colors);
    const found = index.get(tagModel);
    let prod;
    if(!found){
      const seed = pickSeedVariant(g.colors);
      const firstImg = pickFirstImage(g.colors);
      prod = await createProduct(basePayload, seed, firstImg, wantSizes);
      if(prod?.variants?.[0]) await setInventory(prod.variants[0].inventory_item_id, seed.qty);
      await publishProduct(prod.id);
    }else{
      prod = await updateProduct(found.id, { ...basePayload });
      await publishProduct(found.id);
      prod = { ...found, ...prod };
    }

    const { totalQty } = await upsertVariants(prod, g.colors);

    // stok etiketi
    const tagSet = new Set((basePayload.tags||'').split(',').map(t=>t.trim()).filter(Boolean));
    if(totalQty>0){ tagSet.delete('satis:kapali'); tagSet.add('satis:acik'); }
    else          { tagSet.delete('satis:acik');   tagSet.add('satis:kapali'); }
    await updateProduct(prod.id, { tags: Array.from(tagSet).join(', ') });

    processed++;
    if(processed % BATCH_SIZE === 0) await sleep(800);
    console.log(`OK: ${clip(g.title)} | Toplam Stok: ${totalQty} | Tip: ${g.product_type}`);
  }

  console.log('Bitti. İşlenen aile:', processed);
}

main().catch(e=>{ console.error('HATA:', e.message||e); process.exit(1); });
