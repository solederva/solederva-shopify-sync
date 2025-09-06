import { setTimeout as sleep } from 'timers/promises';
import { XMLParser } from 'fast-xml-parser';

const DOMAIN_IN = (process.env.SHOP_DOMAIN || '').trim();
const SHOP_DOMAIN = DOMAIN_IN.endsWith('.myshopify.com') ? DOMAIN_IN.replace('.myshopify.com','') : DOMAIN_IN;
const TOKEN = (process.env.SHOPIFY_ACCESS_TOKEN || '').trim();
const SOURCE_URL = (process.env.SOURCE_URL || '').trim();

if (!SHOP_DOMAIN || !TOKEN || !SOURCE_URL) {
  console.error('Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL');
  process.exit(1);
}

const API_BASE = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/2024-10`;
const HDRS = {
  'X-Shopify-Access-Token': TOKEN,
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'User-Agent': 'solederva-xml-sync/strict'
};

// Basit oran sınırlama
async function rest(method, path, body) {
  const url = `${API_BASE}${path}`;
  await sleep(350); // ~2-3 rps
  const res = await fetch(url, { method, headers: HDRS, body: body ? JSON.stringify(body) : undefined });
  if (res.status === 429) {
    const ra = Number(res.headers.get('Retry-After') || 1);
    await sleep((ra || 1) * 1000);
    return rest(method, path, body);
  }
  if (res.status >= 400) {
    const txt = await res.text();
    throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${txt}`);
  }
  if (res.status === 204) return {};
  return await res.json();
}

async function getLocations() {
  const data = await rest('GET', '/locations.json');
  if (!data.locations || !data.locations.length) throw new Error('Mağaza konumu bulunamadı.');
  return data.locations[0].id; // ilk konum
}

function normalizeImageUrl(u) {
  if (!u) return null;
  let s = String(u).trim();
  if (!s) return null;
  // http -> https (aynı CDN, güvenli)
  if (s.startsWith('http://')) s = 'https://' + s.slice(7);
  return s;
}

// XML okuma
async function loadXmlProducts() {
  const res = await fetch(SOURCE_URL, { redirect: 'follow' });
  const xml = await res.text();
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '',
    cdataTagName: 'cdata',
    processEntities: true,
    trimValues: true
  });
  const j = parser.parse(xml);

  const proots = j.Products?.Product || [];
  const items = Array.isArray(proots) ? proots : [proots];

  return items.map(p => {
    const name = pickText(p.Name);
    const brand = pickText(p.Brand);
    const productId = toStr(p.Product_id);
    const productCode = pickText(p.Product_code) || '';
    const price = num(p.Price);
    const desc = pickHTML(p.Description);
    const mainCategory = pickText(p.mainCategory);
    const category = pickText(p.category);
    const images = [p.Image1, p.Image2, p.Image3, p.Image4, p.Image5]
      .map(normalizeImageUrl)
      .filter(Boolean);

    // Varyantlar
    const vroot = p.variants?.variant || [];
    const vlist = Array.isArray(vroot) ? vroot : [vroot];

    const variants = vlist.map(v => {
      const specs = specsFromVariant(v);
      const renk = specs['Renk'] || '';
      const beden = specs['Beden'] || '';
      const vprice = num(v.price);
      const qty = num(v.quantity);
      const sku = pickText(v.productCode) || pickText(v.barcode) || toStr(v.variantId) || '';
      const barcode = pickText(v.barcode) || '';
      return { renk, beden, price: vprice > 0 ? vprice : price, qty, sku, barcode };
    });

    // Seçenek adlarını XML’de geçtiği haliyle çıkar (sıralı ve tekille)
    const optionNames = uniquePreserve(
      vlist.flatMap(v => Object.keys(specsFromVariant(v))).filter(Boolean)
    );
    // Eğer hiç spec yoksa tek seçenek de açmayacağız.
    return {
      handle: productHandle(productId, productCode, name),
      title: name,
      vendor: brand,
      product_type: category || mainCategory || '',
      body_html: desc,
      images,
      optionNames,
      variants,
    };
  });
}

function pickText(x) {
  if (x == null) return '';
  if (typeof x === 'string') return x.trim();
  if (typeof x === 'object') {
    // CDATA desteği
    if (x.cdata) return String(x.cdata).trim();
    if ('#text' in x) return String(x['#text']).trim();
    if ('text' in x) return String(x.text).trim();
  }
  return String(x).trim();
}
function pickHTML(x){ return pickText(x); }
function num(x){ const n = Number(String(x ?? '').replace(',','.')); return isFinite(n)? n : 0; }
function toStr(x){ return x==null ? '' : String(x).trim(); }
function uniquePreserve(arr){ const seen=new Set(); const out=[]; for(const a of arr){ if(!seen.has(a)){ seen.add(a); out.push(a);} } return out; }
function productHandle(productId, productCode, name){
  // Ürün başına tekil bir handle: öncelik Product_id → yoksa Product_code → yoksa Name
  const base = (productId || productCode || name || 'urun').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
  return `feed-${base}`.slice(0,80);
}
function specsFromVariant(v) {
  const specsNode = v.spec;
  const out = {};
  const arr = Array.isArray(specsNode) ? specsNode : (specsNode ? [specsNode] : []);
  for (const s of arr) {
    const name = (s?.name || '').toString().trim();
    const val = pickText(s);
    if (name) out[name] = val;
  }
  return out;
}

// Shopify yardımcıları
async function findProductByHandle(handle){
  const data = await rest('GET', `/products.json?handle=${encodeURIComponent(handle)}&fields=id,handle`);
  return (data.products && data.products[0]) ? data.products[0] : null;
}

async function createProduct(payload){
  const data = await rest('POST', '/products.json', { product: payload });
  return data.product;
}
async function updateProduct(id, payload){
  const data = await rest('PUT', `/products/${id}.json`, { product: { id, ...payload } });
  return data.product;
}
async function getVariants(productId){
  const data = await rest('GET', `/products/${productId}/variants.json`);
  return data.variants || [];
}
async function addVariant(productId, payload){
  const data = await rest('POST', `/products/${productId}/variants.json`, { variant: payload });
  return data.variant;
}
async function updateVariant(variantId, payload){
  const data = await rest('PUT', `/variants/${variantId}.json`, { variant: { id: variantId, ...payload } });
  return data.variant;
}
async function setInventory(inventory_item_id, location_id, available){
  await rest('POST', `/inventory_levels/set.json`, {
    location_id, inventory_item_id, available
  });
}

function makeProductPayload(item){
  // Shopify options
  const options = (item.optionNames && item.optionNames.length)
    ? item.optionNames.map(n => ({ name: n }))
    : [];

  // Shopify variants
  const variants = (item.variants.length ? item.variants : [{ price: item.price || 0, qty: 0, sku: '', barcode: '' }]).map(v => {
    const opt = [];
    if (options.length) {
      for (const o of options) {
        const oname = (o.name || '').toLowerCase();
        if (oname.includes('renk')) opt.push(v.renk || 'Std');
        else if (oname.includes('beden')) opt.push(v.beden || 'Std');
        else opt.push('Seçenek');
      }
    }
    return {
      option1: opt[0], option2: opt[1], option3: opt[2],
      price: v.price ?? 0,
      sku: v.sku || undefined,
      barcode: v.barcode || undefined,
      inventory_management: 'shopify',
      inventory_policy: 'deny',
      taxable: true
    };
  });

  // Görseller
  const images = (item.images || []).map(src => ({ src }));

  return {
    title: item.title,
    handle: item.handle,            // ilk yaratmada kullanılır
    vendor: item.vendor || undefined,
    body_html: item.body_html || undefined,
    product_type: item.product_type || undefined,
    status: 'active',
    options,
    variants,
    images
  };
}

async function upsertProductStrict(locId, item){
  const existing = await findProductByHandle(item.handle);
  if (!existing){
    // CREATE
    const payload = makeProductPayload(item);
    const created = await createProduct(payload);

    // Stok set
    const freshVariants = await getVariants(created.id);
    for (let i=0;i<freshVariants.length;i++){
      const fv = freshVariants[i];
      const src = item.variants[i] || item.variants[0] || {};
      await setInventory(fv.inventory_item_id, locId, Math.max(0, Number(src.qty || 0)));
    }
    console.log(`OK (Yeni): ${item.title} | Varyant: ${freshVariants.length}`);
    return;
  }

  // UPDATE (başlık/açıklama/vendor/type/images dokun; varyantları SKU/BARCODE ile eşle)
  const payload = {
    title: item.title,
    vendor: item.vendor || undefined,
    body_html: item.body_html || undefined,
    product_type: item.product_type || undefined,
    status: 'active'
  };
  await updateProduct(existing.id, payload);

  // Var olan varyantları çek
  const cur = await getVariants(existing.id);

  // Eşleştirme anahtarı: sku -> barcode -> (renk|beden)
  const indexCur = new Map();
  for (const v of cur){
    const key = v.sku?.trim() || v.barcode?.trim() || `${v.option1}|${v.option2}|${v.option3}`;
    indexCur.set(key, v);
  }

  // Hedef seçenek isimleri
  const options = (item.optionNames && item.optionNames.length) ? item.optionNames : [];
  // Her kaynak varyant için upsert
  for (const sv of item.variants){
    const key = (sv.sku || sv.barcode || `${sv.renk||'Std'}|${sv.beden||'Std'}|`).trim();
    const match = indexCur.get(key);
    const variantPayload = {
      price: sv.price ?? 0,
      barcode: sv.barcode || undefined,
      sku: sv.sku || undefined,
    };

    // Seçenekleri yeniden yaz
    if (options.length){
      const optVals = [];
      for (const o of options){
        const nm = (o || '').toLowerCase();
        if (nm.includes('renk')) optVals.push(sv.renk || 'Std');
        else if (nm.includes('beden')) optVals.push(sv.beden || 'Std');
        else optVals.push('Seçenek');
      }
      variantPayload.option1 = optVals[0];
      variantPayload.option2 = optVals[1];
      variantPayload.option3 = optVals[2];
    }

    if (match){
      // UPDATE
      const upd = await updateVariant(match.id, variantPayload);
      await setInventory(upd.inventory_item_id, locId, Math.max(0, Number(sv.qty || 0)));
    } else {
      // ADD
      const added = await addVariant(existing.id, variantPayload);
      await setInventory(added.inventory_item_id, locId, Math.max(0, Number(sv.qty || 0)));
    }
  }

  // Görseller (yalnızca eksik olanları ekle)
  if (item.images?.length){
    // üründeki mevcut görselleri çek
    const imgs = await rest('GET', `/products/${existing.id}/images.json`);
    const have = new Set((imgs.images||[]).map(i => i.src));
    for (const src of item.images){
      if (!have.has(src)){
        try {
          await rest('POST', `/products/${existing.id}/images.json`, { image: { src } });
          await sleep(250);
        } catch(e){
          console.warn('WARN image add:', e.message);
        }
      }
    }
  }

  console.log(`OK (Güncellendi): ${item.title}`);
}

// MAIN
async function main(){
  console.log('XML okunuyor…');
  const items = await loadXmlProducts();
  console.log('Model sayısı:', items.length);

  const locId = await getLocations();

  // Sade batch: sırayla işle (limitleri zorlamayalım)
  for (const it of items){
    try{
      await upsertProductStrict(locId, it);
    }catch(err){
      console.error('HATA:', err.message);
      // Devam et
    }
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
