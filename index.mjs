import { XMLParser } from 'fast-xml-parser';

/* ====== Config (Actions Secrets) ====== */
const SHOP_DOMAIN   = process.env.SHOP_DOMAIN;      // ör: "shkx8d-wy"
const ACCESS_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION   = "2024-07";
const SOURCE_URL    = process.env.SOURCE_URL;
const PRIMARY_DOMAIN= process.env.PRIMARY_DOMAIN || "www.solederva.com";
const BATCH_SIZE    = Number(process.env.BATCH_SIZE || 50);

/* ====== Helpers ====== */
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
async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, { method, headers: headers(true), body: body ? JSON.stringify(body) : undefined });
  if(!res.ok){
    const t = await res.text().catch(()=>res.statusText);
    throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${t}`);
  }
  return await res.json().catch(()=> ({}));
}

/* ---------- Online Store publish (izin varsa) + fallback ---------- */
async function getOnlineStorePublicationId(){
  const pubs = await rest(`/publications.json`, 'GET');
  const pub = (pubs?.publications||[]).find(p=>/online/i.test(p.name));
  return pub?.id;
}
async function fallbackMarkWeb(productId){
  // read_publications izni yoksa burası devreye girer
  try {
    await rest(`/products/${productId}.json`,'PUT',{
      product:{ id: productId, status:'active', published_scope:'web' }
    });
  } catch(e){
    // bazı mağazalarda published_scope yazılamayabilir: hatayı yut
    console.log('PUBLISH-FALLBACK WARN:', String(e.message||e).slice(0,160));
  }
}
async function publishProduct(productId){
  try{
    const pubId = await getOnlineStorePublicationId(); // <-- 403 alabilir
    if(!pubId) throw new Error('No publication id');
    await rest(`/publications/${pubId}/publish.json`, 'POST', {
      publication: { publishable_id: productId, publishable_type: "product" }
    });
  }catch(e){
    // 403 veya başka bir hata: fallback’e düş
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
    await sleep(200);
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
  // Markayı başta göstermiyoruz; sonuna koyuyoruz
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

/* ---------- Görseller: sadece eksikleri yükle ---------- */
async function ensureImages(product, colorImagesMap){
  const metaMap = await getMetaMap(product.id);
  const current = JSON.parse(metaMap.get('sync:images')?.value || '[]');
  const wantAll = uniq([].concat(...Array.from(colorImagesMap.values()).map(x=>x.images||[])));
  const toAdd   = wantAll.filter(u => !current.includes(imageSign(u)));

  for(const src of toAdd){
    try{
      await rest(`/products/${product.id}/images.json`,'POST',{ image: { src, alt: `SRC:${imageSign(src)}` } });
      await sleep(350);
    }catch(e){
      console.log('WARN image', src, String(e.message||e).slice(0,160));
    }
  }

  const merged = uniq(current.concat(wantAll.map(imageSign)));
  await updateMeta(product.id, metaMap, 'sync','images', merged);

  const imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];
  return imagesFull;
}

/* ---------- Varyant idempotent ---------- */
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

  const imagesFull = await ensureImages(product, colorImagesMap);
  const imageIdByColor = new Map();
  for(const [col, node] of colorImagesMap.entries()){
    const first = node.images?.[0];
    if(!first) continue;
    const sig = imageSign(first);
    const found = imagesFull.find(im => (im.alt||'').includes(`SRC:${sig}`));
    if(found) imageIdByColor.set(uc(col), found.id);
  }

  for(const [col, node] of colorImagesMap.entries()){
    for(const v of node.variants){
      const sizeVal = v.size ? String(v.size) : 'Std';
      const k = keyVS(v.color, sizeVal);
      const ex = byKey.get(k);

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
          await sleep(200);
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
            await sleep(250);
          }
        }catch(e){
          if(!/already exists/i.test(e.message||'')) console.log('WARN add variant', k, e.message);
        }
      }
    }
  }
}

/* ---------- Ürün oluştur/güncelle ---------- */
async function createProduct(payload, seedVariant, firstImage){
  // options + EN AZ 1 varyant şart
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
      status: 'active' // ürün aktif
    };

    const found = index.get(tagModel);

    if(!found){
      const seed = pickSeedVariant(g.colors);
      const firstImg = pickFirstImage(g.colors);
      const p = await createProduct(productPayloadBase, seed, firstImg);
      if(!p) continue;
      if(p?.variants?.[0]) await setInventory(p.variants[0].inventory_item_id, seed.qty);
      await publishProduct(p.id);          // izin varsa publications, yoksa fallback web
      await upsertVariants(p, g.colors);   // idempotent varyant+görsel
      await sleep(300);
      console.log(`OK (Yeni): ${clip(title,80)}`);
    }else{
      const p = await updateProduct(found.id, { ...productPayloadBase, /* options dokunmadan */ });
      await publishProduct(found.id);      // izin yoksa fallback çalışır
      await upsertVariants({ ...found, ...p }, g.colors);
      await sleep(200);
      console.log(`OK (Güncel): ${clip(title,80)}`);
    }

    processed++;
    if(processed % BATCH_SIZE === 0) await sleep(1000);
  }

  console.log('Bitti. İşlenen model:', processed);
}

main().catch(e=>{ console.error('HATA:', e.message||e); process.exit(1); });
