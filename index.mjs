import { XMLParser } from 'fast-xml-parser';

/* ====== Config (Actions Secrets) ====== */
const SHOP_DOMAIN   = process.env.SHOP_DOMAIN;      // ör: "shkx8d-wy"
const ACCESS_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION   = "2024-07";
const SOURCE_URL    = process.env.SOURCE_URL;
const PRIMARY_DOMAIN= process.env.PRIMARY_DOMAIN || "www.solederva.com";
const BATCH_SIZE    = Number(process.env.BATCH_SIZE || 50);

/* ====== Yardımcılar ====== */
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
  const res = await fetch(url, {
    method,
    headers: headers(true),
    body: body ? JSON.stringify(body) : undefined
  });
  if(!res.ok){
    const t = await res.text().catch(()=>res.statusText);
    throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${t}`);
  }
  return await res.json().catch(()=> ({}));
}

/* ---------- Online Store’a otomatik yayın ---------- */
async function getOnlineStorePublicationId(){
  const pubs = await rest(`/publications.json`, 'GET');
  const pub = (pubs?.publications||[]).find(p=>/online/i.test(p.name));
  return pub?.id;
}
async function publishProduct(productId){
  const pubId = await getOnlineStorePublicationId();
  if(!pubId) return;
  await rest(`/publications/${pubId}/publish.json`, 'POST', {
    publication: {
      publishable_id: productId,
      publishable_type: "product"
    }
  });
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

/* ---------- Mevcut ürünleri bir defa yükle (idempotent) ---------- */
async function loadProductIndex(){
  // Map: modelTag -> {id, handle, tags[], images[], variants[]}
  const map = new Map();
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

/* ---------- XML okuma ve gruplama ---------- */
const COLOR_WORDS = [
  'SIYAH','BEYAZ','KAHVE','LACIVERT','TABA','GRI','ANTRASIT','SAX','BEYAZ/SIYAH','SIYAH/BEYAZ','BYZ','AT','KE'
];

function splitTitle(nameRaw){
  // Ör: "MN002 - RBT Loafer Verniciata Erkek Ayakkabı SIYAH"
  const s = uc(nameRaw||"").replace(/\s+/g,' ').trim();
  const parts = s.split(' ');
  // Son kelime renk ise ayır
  let color = parts[parts.length-1];
  if(!COLOR_WORDS.includes(color)) color = '';
  const base = color ? s.slice(0, s.lastIndexOf(' '+color)) : s;
  return { baseTitle: base, color };
}
function readableTitle(base, brand, mpn){
  // Ör: "Loafer Verniciata Erkek Ayakkabı – RBT MOOİEN MN002R25490"
  // base içinde baştaki kod/kısaltmaları temizleyip insanlar için daha net bırakıyoruz
  // "MN002 - RBT Loafer ..." -> "Loafer ..."
  const pretty = base.replace(/^MN\d+\s*-\s*/,'').trim();
  return `${pretty} – ${brand} ${mpn}`.replace(/\s+/g,' ').trim();
}

function parseXML(xml){
  const parser = new XMLParser({ ignoreAttributes:true, attributeNamePrefix:'', trimValues:true });
  const root = parser.parse(xml)||{};
  const list = [].concat(root?.Products?.Product||[]);
  // Her Product; varyantlar altında beden/renk ve price/quantity var
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
        const beden = (specs.find(s=>s?.name==='Beden')?.['#text'] || '').trim();
        return {
          color : renk,
          size  : beden,
          sku   : String(v.productCode||v.barcode||v.variantId||'').trim(),
          barcode: String(v.barcode||'').trim(),
          qty   : num(v.quantity),
          price : num(v.price) || price
        };
      });

    // Model grubunu oluşturmak için anahtar:
    // – finisaj (CST/RST/CBT gibi) baseTitle içinde durur; sadece renk ayrıldı.
    const modelKey  = `model:${brand}|${baseTitle}`; // TAG olarak da kullanacağız
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
    // renk -> {images, variants[]}
    const keyColor = it.colorDefault || 'STANDART';
    const node = g.colors.get(keyColor) || { images:[], variants:[] };
    node.images = uniq(node.images.concat(it.images));
    node.variants = node.variants.concat(it.variants.filter(v=>uc(v.color)===uc(keyColor)));
    g.colors.set(keyColor, node);
  }
  return Array.from(map.values());
}

/* ---------- Görsel imza/metafield ile idempotent yükleme ---------- */
async function getMetaMap(productId){
  const js = await rest(`/products/${productId}/metafields.json`,'GET');
  const map = new Map();
  for(const m of (js.metafields||[])) map.set(`${m.namespace}:${m.key}`, m);
  return map;
}
async function setMeta(productId, namespace, key, value){
  const payload = { metafield:{ namespace, key, value: JSON.stringify(value), type:'json' } };
  await rest(`/products/${productId}/metafields.json`,'POST', payload);
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

/* ---------- Resim ekleme (sadece eksikler) ---------- */
async function ensureImages(product, colorImagesMap){
  // metafield 'sync:images' -> ["cdn1.../a.jpg", ...]
  const metaMap = await getMetaMap(product.id);
  const current = JSON.parse(metaMap.get('sync:images')?.value || '[]');
  const wantAll = uniq([].concat(...Array.from(colorImagesMap.values()).map(x=>x.images||[])));
  const toAdd   = wantAll.filter(u => !current.includes(imageSign(u)));

  // yalnız eksikleri yükle
  const created = [];
  for(const src of toAdd){
    try{
      const imgRes = await rest(`/products/${product.id}/images.json`,'POST', {
        image: { src, alt: `SRC:${imageSign(src)}` }
      });
      created.push(imgRes?.image);
      await sleep(350);
    }catch(e){
      // zaman aşımı vs. sorunları yumuşat
      console.log('WARN image', src, String(e.message||e).slice(0,160));
    }
  }

  // güncel listeyi yaz
  const merged = uniq(current.concat(wantAll.map(imageSign)));
  await updateMeta(product.id, metaMap, 'sync','images', merged);

  // image_id eşlemesi için en güncel listeyi çek
  const imagesFull = (await rest(`/products/${product.id}/images.json`,'GET'))?.images||[];
  return imagesFull;
}

/* ---------- Varyant idempotent ekle/güncelle ---------- */
function keyVS(c,s){ return `${uc(c)}|${String(s||'')}`; }

async function upsertVariants(product, colorImagesMap){
  const existing = product.variants || [];
  const byKey = new Map(existing.map(v=>[ keyVS(v.option1, v.option2), v ]));

  // Her renk grubunun ilk görselini o rengin varyantlarına bağlayacağız
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
      const k = keyVS(v.color, v.size);
      const ex = byKey.get(k);
      if(ex){
        // güncelle (fiyat, stok, sku, barkod, image_id)
        try{
          await rest(`/variants/${ex.id}.json`,'PUT',{
            variant:{
              id: ex.id,
              price: to2(v.price),
              sku: v.sku || ex.sku,
              barcode: v.barcode || ex.barcode,
              option1: v.color,
              option2: v.size || null,
              image_id: imageIdByColor.get(uc(v.color)) || ex.image_id || null
            }
          });
          await setInventory(ex.inventory_item_id, v.qty);
          await sleep(200);
        }catch(e){ console.log('WARN update variant', k, e.message); }
      }else{
        // yeni varyant ekle
        try{
          const add = await rest(`/products/${product.id}/variants.json`, 'POST', {
            variant:{
              option1: v.color,
              option2: v.size || null,
              price: to2(v.price),
              sku: v.sku || undefined,
              barcode: v.barcode || undefined,
              inventory_management: 'shopify'
            }
          });
          const nv = add?.variant;
          if(nv){
            if(imageIdByColor.get(uc(v.color))){
              await rest(`/variants/${nv.id}.json`,'PUT',{
                variant:{ id:nv.id, image_id: imageIdByColor.get(uc(v.color)) }
              });
            }
            await setInventory(nv.inventory_item_id, v.qty);
            await sleep(250);
          }
        }catch(e){
          // En çok görülen hata: "variant already exists" -> sessiz geç
          if(!/already exists/i.test(e.message||'')) console.log('WARN add variant', k, e.message);
        }
      }
    }
  }
}

/* ---------- Ürün oluştur/güncelle ---------- */
async function createProduct(payload){
  const js = await rest(`/products.json`,'POST',{ product: payload });
  return js?.product;
}
async function updateProduct(id, payload){
  const js = await rest(`/products/${id}.json`,'PUT',{ product: { id, ...payload } });
  return js?.product;
}

/* ----- Ana iş ----- */
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
    // Title & handle
    const title  = g.title;
    const handleBase = (title.toLowerCase()
      .replace(/[^\w]+/g,'-')
      .replace(/-+/g,'-')
      .replace(/^-|-$/g,'') || 'urun').slice(0,80);

    const tagModel = g.modelKey; // ör: model:MOOİEN|MN002 - RBT ...
    const found = index.get(tagModel);

    const productPayloadBase = {
      title,
      body_html: '', // XML açıklamalarını istersen ekleyebiliriz
      vendor: g.brand,
      product_type: 'Ayakkabı',
      tags: g.tags.join(', '),
      options: ['Renk','Beden'],
      status: 'active' // vitrine çıkması için aktif
    };

    if(!found){
      // yeni ürün
      const p = await createProduct(productPayloadBase);
      if(!p) continue;
      await publishProduct(p.id); // Online Store’a yayınla
      // Görseller + varyantlar
      await upsertVariants(p, g.colors);
      await sleep(300);
      console.log(`OK (Yeni): ${clip(title,80)}`);
    }else{
      // güncelle + idempotent
      const p = await updateProduct(found.id, productPayloadBase);
      await publishProduct(found.id);
      // Görselleri sadece eksik olanları ekle, varyantları güncelle/ekle
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
