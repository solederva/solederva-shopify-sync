import { XMLParser } from 'fast-xml-parser';
import path from 'node:path';

/* ====== Config (Secrets veya ortam değişkenleri) ====== */
const SHOP_DOMAIN   = process.env.SHOP_DOMAIN;                 // örn: "shkx8d-wy"
const ACCESS_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN;        // shpat_...
const API_VERSION   = '2024-07';                               // sabit
const SOURCE_URL    = process.env.SOURCE_URL;                  // XML linki
const PRIMARY_DOMAIN= process.env.PRIMARY_DOMAIN || 'www.solederva.com';
const BATCH_SIZE    = Number(process.env.BATCH_SIZE || 50);    // ürün başı limit

/* ====== Küçük yardımcılar ====== */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const num   = (x)=>{ if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=>{ const n=num(x); return n>0 ? n.toFixed(2) : '0.00'; };
const uc    = (s)=> (String(s||'').toString().trim().toUpperCase());
const uniq  = (arr)=> Array.from(new Set(arr.filter(Boolean)));
const clip  = (s,m=90)=>{ s=String(s||'').trim(); return s.length>m ? s.slice(0,m-1)+'…' : s; };

function headers(json=true){
  const h = { 'X-Shopify-Access-Token': ACCESS_TOKEN };
  if(json) h['Content-Type']='application/json';
  return h;
}

async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, { method, headers: headers(true), body: body ? JSON.stringify(body) : undefined });
  if(!res.ok){
    const txt = await res.text().catch(()=> '');
    throw new Error(`Shopify ${method.toLowerCase()} ${path} -> ${res.status}: ${txt}`);
  }
  return await res.json().catch(()=> ({}));
}

/* ---------- Güvenli görsel yükleme (422/timeouts çözümü) ---------- */
function fixUrl(u){
  if(!u) return '';
  let s = String(u).trim();
  s = s.replace(/^http:\/\//i, 'https://');
  s = s.replace(/ /g, '%20');
  try { const uo = new URL(s); uo.protocol='https:'; s=uo.toString(); } catch(_) {}
  return s;
}

async function _postImageBySrc(productId, src, alt, variantIds=[]){
  const body = { image: { src, alt } };
  if(variantIds.length) body.image.variant_ids = variantIds;
  return await rest(`/products/${productId}/images.json`, 'POST', body);
}

async function _postImageByAttachment(productId, src, alt, variantIds=[]){
  const r = await fetch(src, { redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0' }});
  if(!r.ok) throw new Error(`download ${src} -> ${r.status}`);
  const buf = Buffer.from(await r.arrayBuffer()).toString('base64');
  const name = (()=>{ try{ return path.basename(new URL(src).pathname) || 'image.jpg'; }catch{ return 'image.jpg'; }})();
  const body = { image: { attachment: buf, filename: name, alt } };
  if(variantIds.length) body.image.variant_ids = variantIds;
  return await rest(`/products/${productId}/images.json`, 'POST', body);
}

async function safeUploadImage(productId, rawUrl, alt, variantIds=[]){
  const src = fixUrl(rawUrl);
  // 1) src ile dener
  try{
    await _postImageBySrc(productId, src, alt, variantIds);
    await sleep(400);
    return;
  }catch(err){
    const t=String(err);
    const recoverable = t.includes('Could not download image')
                     || t.includes('file not found')
                     || t.includes('timeout')
                     || t.includes('Failed to open stream')
                     || t.includes('404');
    if(!recoverable) throw err;
  }
  // 2) attachment ile
  await sleep(1200);
  await _postImageByAttachment(productId, src, alt, variantIds);
  await sleep(400);
}

/* ---------- Shopify yardımcıları ---------- */
async function getFirstLocationId(){
  const r = await rest('/locations.json', 'GET');
  const loc = (r.locations||[])[0];
  if(!loc) throw new Error('Location bulunamadı.');
  return loc.id;
}

async function findProductIdByHandle(handle){
  // GraphQL ile handle bul
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}/graphql.json`;
  const q = `
    query($q:String!){
      products(first:1, query:$q){
        edges{ node{ id handle variants(first:250){ edges{ node{ id sku inventoryItem{id} title }}}}}
      }
    }
  `;
  const body = { query: q, variables: { q: `handle:${handle}` } };
  const res = await fetch(url, { method:'POST', headers: headers(true), body: JSON.stringify(body) });
  if(!res.ok) throw new Error(`GraphQL products search -> ${res.status}`);
  const data = await res.json();
  const edge = data?.data?.products?.edges?.[0];
  if(!edge) return null;
  const node = edge.node;
  return {
    id: node.id.replace('gid://shopify/Product/',''),
    variants: (node.variants.edges||[]).map(e=>({
      id: e.node.id.replace('gid://shopify/ProductVariant/',''),
      sku: e.node.sku||'',
      title: e.node.title||'',
      inventoryItemId: e.node.inventoryItem?.id?.replace('gid://shopify/InventoryItem/','')
    }))
  };
}

async function createProduct(payload){
  const r = await rest('/products.json','POST',{ product: payload });
  return r.product;
}

async function updateProduct(productId,payload){
  const r = await rest(`/products/${productId}.json`,'PUT',{ product: { id: productId, ...payload }});
  return r.product;
}

async function createVariant(productId, v){
  const r = await rest(`/products/${productId}/variants.json`,'POST',{ variant: v });
  return r.variant;
}

async function updateVariant(variantId, v){
  const r = await rest(`/variants/${variantId}.json`,'PUT',{ variant: { id: variantId, ...v }});
  return r.variant;
}

async function setInventory(inventoryItemId, qty, locationId){
  await rest('/inventory_levels/set.json','POST',{
    location_id: locationId,
    inventory_item_id: inventoryItemId,
    available: Number(qty||0)
  });
}

/* ---------- XML okuma & modelleme ---------- */
const COLORS = ['SIYAH','BEYAZ','ANTRASIT','KAHVE','LACIVERT','TABA','GRI','SAX','SARI','KIRMIZI','MAVI','BEJ','YESIL'];
const COLOR_RE = new RegExp(`\\b(${COLORS.join('|')})\\b`, 'i');

function cleanHtml(html){ return String(html||'').trim(); }
function extractColorFromName(name){
  const m = (String(name||'').match(COLOR_RE)||[])[1];
  return m ? m.toUpperCase() : '';
}
function titleFromXmlName(brand, xmlName, code){
  // XML başlığındaki rengi sondan sil, brand+kod sona
  let base = String(xmlName||'').trim();
  base = base.replace(/\s*-\s*$/,'');
  // sonda renk kelimesini kırp
  base = base.replace(/\b(SIYAH|BEYAZ|ANTRASIT|KAHVE|LACIVERT|TABA|GRI|SAX|SARI|KIRMIZI|MAVI|BEJ|YESIL)\b\s*$/i,'').trim();
  return `${base} – ${brand} ${code}`.replace(/\s+/g,' ');
}

function handleFrom(brand, mpnOrCode){
  const s = `${brand}-${mpnOrCode}`.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');
  return s;
}

function parseXmlProducts(xmlText){
  const parser = new XMLParser({
    ignoreAttributes: false,
    cdataPropName: 'cdata',
    trimValues: true,
  });
  const j = parser.parse(xmlText);
  const items = j?.Products?.Product;
  if(!items) return [];
  const list = Array.isArray(items)? items : [items];

  const out = [];
  for(const p of list){
    const productCode = (p.Product_code?.cdata ?? p.Product_code ?? '').toString().trim();
    const mpn         = (p.Mpn ?? '').toString().trim() || productCode;
    const id          = (p.Product_id ?? '').toString().trim();
    const brand       = (p.Brand?.cdata ?? p.Brand ?? '').toString().trim() || 'MARKA';
    const name        = (p.Name?.cdata ?? p.Name ?? '').toString().trim();
    const desc        = (p.Description?.cdata ?? p.Description ?? '').toString();
    const price       = num(p.Price);
    const stock       = num(p.Stock);
    const images      = uniq([p.Image1,p.Image2,p.Image3,p.Image4,p.Image5].map(x=>x||'')).filter(Boolean);
    const variantsRaw = p.variants?.variant;
    const vlist       = Array.isArray(variantsRaw) ? variantsRaw : (variantsRaw ? [variantsRaw] : []);

    const variants = vlist.map((v,idx)=>{
      const renk  = String((v.spec||[]).find(s=> (s['@_name']||'').toLowerCase()==='renk')?.['#text'] || v.spec?.cdata || '').trim();
      const beden = String((v.spec||[]).find(s=> (s['@_name']||'').toLowerCase()==='beden')?.['#text'] || '').trim();
      const barcode = (v.barcode?.cdata ?? v.barcode ?? v.productCode?.cdata ?? '').toString().trim() || `${productCode}-${renk||'R'}-${beden||idx+1}`;
      const vqty = num(v.quantity ?? stock);
      const vpr  = num(v.price);
      return {
        sku: barcode,
        color: renk || extractColorFromName(name),
        size: beden || '',
        qty: vqty,
        price: vpr>0 ? vpr : price
      };
    });

    out.push({
      id, mpn, productCode, brand, name, desc, price, stock, images, variants
    });
  }
  return out;
}

/* ---------- Ürün birleştirme (aynı MPN -> tek Shopify ürün) ---------- */
function groupByModel(items){
  const map = new Map();
  for(const it of items.slice(0,BATCH_SIZE)){  // güvenli limit
    const key = it.mpn || it.productCode;
    if(!map.has(key)) map.set(key, { key, brand: it.brand, xmlName: it.name, code: it.productCode, desc: it.desc, nodes: [] });
    map.get(key).nodes.push(it);
  }
  return Array.from(map.values());
}

/* ---------- Sync akışı ---------- */
async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.error('Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL');
    process.exit(1);
  }

  console.log('XML okunuyor…');
  const xml = await fetch(SOURCE_URL, { redirect:'follow' }).then(r=>r.text());
  const products = parseXmlProducts(xml);
  const models   = groupByModel(products);
  console.log('Model sayısı:', models.length);

  const locId = await getFirstLocationId();

  for(const model of models){
    const { brand, xmlName, code } = model;
    const baseTitle = titleFromXmlName(brand, xmlName, code);
    const handle = handleFrom(brand, model.key);

    // Tüm varyantları (renk/beden) topla
    const colorSet = new Set();
    const sizeSet  = new Set();
    const allVariants = [];
    const colorImages = new Map(); // renk -> resim listesi

    for(const node of model.nodes){
      const col = extractColorFromName(node.name) || (node.variants[0]?.color || '');
      colorSet.add(uc(col));
      node.variants.forEach(v=>{
        sizeSet.add(String(v.size||''));
        allVariants.push({
          option1: uc(v.color||col||''),
          option2: String(v.size||''),
          price:   to2(v.price),
          sku:     v.sku,
          inventory_quantity: Number(v.qty||0),
          inventory_management: 'shopify'
        });
      });
      const imgs = uniq(node.images.map(fixUrl));
      if(imgs.length){
        const k=uc(col||extractColorFromName(node.name)||'');
        colorImages.set(k, uniq([...(colorImages.get(k)||[]), ...imgs]));
      }
    }

    const options = [];
    if(colorSet.size) options.push({ name: 'Renk', values: Array.from(colorSet) });
    if(sizeSet.size)  options.push({ name: 'Beden', values: Array.from(sizeSet) });

    // Var mı?
    const found = await findProductIdByHandle(handle);
    let productId;
    let variantMap = new Map(); // sku -> {id, inventoryItemId}

    if(!found){
      // oluştur
      const payload = {
        title: baseTitle,
        body_html: cleanHtml(model.desc),
        vendor: brand,
        handle,
        status: 'active',
        options,
        variants: allVariants.length ? allVariants : [{
          option1: 'STD', option2: 'STD',
          price: to2( model.nodes[0]?.price || 0 ),
          sku: model.nodes[0]?.variants?.[0]?.sku || `${handle}-STD`,
          inventory_quantity: Number(model.nodes[0]?.stock || 0),
          inventory_management: 'shopify'
        }]
      };
      const created = await createProduct(payload);
      productId = created.id;
      // variant map
      for(const v of created.variants){
        variantMap.set(v.sku||'', { id:String(v.id), inventoryItemId:String(v.inventory_item_id) });
      }
      console.log('OK: +', baseTitle, '| Varyant:', created.variants.length);
      await sleep(600);
    }else{
      // güncelle (varyant eşle, eksik ekle, fiyat/stok güncelle)
      productId = found.id;
      for(const v of found.variants){
        variantMap.set(v.sku||'', { id:String(v.id), inventoryItemId:String(v.inventoryItemId||'') });
      }

      // eksik varyantları ekle
      for(const v of allVariants){
        if(!variantMap.has(v.sku)){
          const createdV = await createVariant(productId, {
            option1: v.option1||'',
            option2: v.option2||'',
            price: v.price,
            sku: v.sku,
            inventory_management: 'shopify'
          });
          variantMap.set(createdV.sku||'', { id:String(createdV.id), inventoryItemId:String(createdV.inventory_item_id) });
          await sleep(500);
        }
      }
      // fiyat ve stok güncelle
      for(const v of allVariants){
        const got = variantMap.get(v.sku);
        if(got){
          await updateVariant(got.id, { price: v.price });
          await sleep(250);
          if(got.inventoryItemId){
            await setInventory(got.inventoryItemId, v.inventory_quantity, locId);
            await sleep(250);
          }
        }
      }
      // başlık/desc güncel
      await updateProduct(productId, { title: baseTitle, body_html: cleanHtml(model.desc), vendor: brand, handle });
      console.log('OK: ~', baseTitle, '| Varyant:', allVariants.length);
      await sleep(700);
    }

    // Renk görsellerini ilgili varyant(lar)a bağla
    for(const [color, imgs] of colorImages.entries()){
      // bu renge ait varyant id’leri
      const vids = [];
      for(const [sku, meta] of variantMap.entries()){
        if(sku && meta?.id){
          // sku’dan renk çıkarılamıyorsa seçenek adına bakmak için atlıyoruz; basit eşleme:
          if(allVariants.find(x=> x.sku===sku && uc(x.option1)===uc(color))) vids.push(Number(meta.id));
        }
      }
      for(const img of imgs){
        try{
          await safeUploadImage(productId, img, `${color} görseli`, vids);
        }catch(e){
          console.log('IMG SKIP:', clip(String(e), 180));
        }
      }
      await sleep(500);
    }
  }

  console.log('Bitti.');
}

/* ---------- Çalıştır ---------- */
main().catch(err=>{
  console.error(err);
  process.exit(1);
});
