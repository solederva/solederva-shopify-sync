<İÇERİK BAŞI>
// --- BURASI TAM DOSYA ---
// AŞAĞIDAKİ *** TÜM *** KODU YAPIŞTIR: (upsertVariant'lı sürüm)
import { XMLParser } from "fast-xml-parser";

/* ====== Config (GitHub Secrets) ====== */
const SHOP_DOMAIN  = process.env.SHOP_DOMAIN;             // ör: "shkx8d-wy"
const ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;    // shpat_...
const API_VERSION  = "2024-07";
const SOURCE_URL   = process.env.SOURCE_URL;              // XML canlı link
const PRIMARY_DOMAIN = process.env.PRIMARY_DOMAIN || "www.solederva.com";
const BATCH_SIZE   = Number(process.env.BATCH_SIZE || 50);

/* ===== Küçük yardımcılar ===== */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const num   = (x)=> { if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=> { const n=num(x); return n>0 ? n.toFixed(2) : "0.00"; };
const s     = (x)=> (x ?? "").toString().trim();
const uc    = (x)=> s(x).toUpperCase();
const uniq  = (arr)=> Array.from(new Set(arr.filter(Boolean)));
const clip  = (str,max=90)=>{ const s = (str||"").toString().trim(); return s.length>max ? s.slice(0,max-1)+"…" : s; };
const arr   = (x)=> Array.isArray(x) ? x : (x==null ? [] : [x]);

function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if(json) h["Content-Type"]="application/json";
  return h;
}

async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, { method, headers: headers(true), body: body? JSON.stringify(body) : undefined });
  if(!res.ok){
    const txt = await res.text().catch(()=>res.statusText);
    throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${txt}`);
  }
  return res.json().catch(()=> ({}));
}

/* ===== XML okuma & modelleme ===== */
async function readXML(){
  const res = await fetch(SOURCE_URL);
  const xml = await res.text();
  const parser = new XMLParser({ ignoreAttributes:false, cdataPropName:"__cdata" });
  const data = parser.parse(xml);
  const list = arr(data?.Products?.Product);

  const models = list.map(p=>{
    const mpn   = s(p?.Mpn);
    const brand = s(p?.Brand);
    const name  = s(p?.Name?.__cdata ?? p?.Name);
    const main  = s(p?.mainCategory?.__cdata ?? p?.mainCategory);
    const cat   = s(p?.category?.__cdata ?? p?.category);
    const price = num(p?.Price);
    const tax   = num(p?.Tax);
    const imgs  = uniq([s(p?.Image1),s(p?.Image2),s(p?.Image3),s(p?.Image4),s(p?.Image5)])
                   .filter(x=>/^https?:\/\//i.test(x));
    const desc  = s(p?.Description?.__cdata ?? p?.Description);

    const vars = arr(p?.variants?.variant).map(v=>{
      const specs = arr(v?.spec).reduce((o,it)=>{
        const n = uc(it?.["@_name"] || "");
        const val = s(it?.["#text"] ?? it?.__cdata ?? "");
        if(n==="RENK")  o.color = val;
        if(n==="BEDEN") o.size  = val;
        return o;
      }, {});
      const sku     = s(v?.productCode?.__cdata ?? v?.productCode);
      const barcode = s(v?.barcode?.__cdata ?? v?.barcode) || sku;
      const qty     = num(v?.quantity);
      const vprice  = num(v?.price)>0 ? num(v?.price) : price;
      return { sku, barcode, color:s(specs.color), size:s(specs.size), qty, price:vprice };
    });

    const { baseTitle, styleCode } = buildTitle(name, brand, mpn);
    return { mpn, brand, name, main, cat, price, tax, imgs, desc, baseTitle, styleCode, variants: vars };
  });

  return models;
}

function buildTitle(name, brand, mpn){
  const m = /^([A-Z0-9]+)\s*-\s*([A-Z]{3})\s*(.+)$/i.exec(name)||[];
  const codeFromName  = s(m[1]);
  const styleFromName = s(m[2]);
  let titleCore = s(m[3] || name);
  titleCore = titleCore.replace(/\b(SIYAH|BEYAZ|LACIVERT|KAHVE|GRI|ANTRASIT|SAX|MAVI|KREM)\b/gi,"").replace(/\s{2,}/g," ").trim();
  const style = styleFromName || "STD";
  const base  = `${titleCore} – ${style} ${uc(brand)} ${mpn || codeFromName}`.replace(/\s{2,}/g," ").trim();
  return { baseTitle: base, styleCode: style };
}

/* ===== Shopify yardımcıları ===== */
let LOCATION_ID = null;
async function getLocationId(){
  if(LOCATION_ID) return LOCATION_ID;
  const data = await rest(`/locations.json`,'GET');
  LOCATION_ID = data?.locations?.[0]?.id;
  return LOCATION_ID;
}

async function findProductByTitle(title){
  const q = encodeURIComponent(clip(title, 80));
  const data = await rest(`/products.json?title=${q}&limit=50`,'GET');
  const list = data?.products || [];
  return list.find(p => s(p.title) === s(title)) || null;
}

async function createProduct(model){
  const payload = {
    product: {
      title: model.baseTitle,
      body_html: model.desc || "",
      vendor: model.brand || "SoleDerva",
      product_type: model.main || "Ayakkabı",
      tags: [
        `brand:${model.brand}`,
        `mpn:${model.mpn}`,
        `from:xml`,
        (model.price>0 ? "satis:acik" : "satis:kapali")
      ].join(", "),
      options: [
        { name: "Renk", values: uniq(model.variants.map(v=>s(v.color))) },
        { name: "Beden", values: uniq(model.variants.map(v=>s(v.size)))  }
      ],
      images: model.imgs && model.imgs.length ? [{ src: model.imgs[0] }] : []
    }
  };
  const res = await rest(`/products.json`, 'POST', payload);
  return res?.product || null;
}

function mapVariantPayload(v){
  return {
    sku: s(v.sku),
    barcode: s(v.barcode),
    option1: s(v.color),
    option2: s(v.size),
    price: to2(v.price),
    inventory_management: "shopify"
  };
}

async function setInventory(inventory_item_id, qty){
  const loc = await getLocationId();
  if(!loc || !inventory_item_id) return;
  await rest(`/inventory_levels/set.json`, 'POST', {
    location_id: loc,
    inventory_item_id,
    available: num(qty)
  });
}

async function ensureOptionStrings(product, xmlVariantsByBarcode){
  const variants = product?.variants || [];
  const fixes = [];
  for(const v of variants){
    const by = xmlVariantsByBarcode.get(s(v.barcode)) || {};
    const want1 = s(by.color) || s(v.option1);
    const want2 = s(by.size)  || s(v.option2);
    if(want1 !== s(v.option1) || want2 !== s(v.option2)){
      fixes.push({ id: v.id, option1: want1, option2: want2 });
    }
  }
  for(const f of fixes){
    await rest(`/variants/${f.id}.json`, 'PUT', { variant: f });
    await sleep(250);
  }
}

async function upsertVariant(product, v){
  const variants = product?.variants || [];
  let found = variants.find(x => s(x.barcode)===s(v.barcode) && s(v.barcode)!=="")
           || variants.find(x => s(x.sku)===s(v.sku) && s(v.sku)!=="");
  if(!found){
    found = variants.find(x => s(x.option1)===s(v.color) && s(x.option2)===s(v.size));
  }

  if(found){
    const patch = {};
    if(to2(found.price) !== to2(v.price)) patch.price = to2(v.price);
    const want1 = s(v.color), want2 = s(v.size);
    if(s(found.option1)!==want1) patch.option1 = want1;
    if(s(found.option2)!==want2) patch.option2 = want2;
    if(Object.keys(patch).length){
      patch.id = found.id;
      await rest(`/variants/${found.id}.json`, 'PUT', { variant: patch });
      await sleep(200);
    }
    await setInventory(found.inventory_item_id, v.qty);
    return found.id;
  }else{
    const payload = { variant: mapVariantPayload(v) };
    const res = await rest(`/products/${product.id}/variants.json`, 'POST', payload).catch(async err=>{
      if(/already exists/i.test(String(err.message||""))){
        const fresh = await rest(`/products/${product.id}.json`, 'GET');
        return { variant: (fresh?.product?.variants||[]).find(x => s(x.option1)===s(v.color) && s(x.option2)===s(v.size)) };
      }
      throw err;
    });
    const created = res?.variant;
    if(created){
      await setInventory(created.inventory_item_id, v.qty);
      await sleep(200);
      return created.id;
    }
  }
  return null;
}

async function linkImages(product, model){
  const existing = (product.images||[]).map(i=>i.src);
  for(const u of (model.imgs||[])){
    if(!/^https?:\/\//i.test(u)) continue;
    if(existing.includes(u)) continue;
    try{
      await rest(`/products/${product.id}/images.json`, 'POST', { image: { src: u } });
      await sleep(250);
    }catch(e){}
  }
}

async function ensureProductOptions(product, model){
  const needUpdate =
    s(product.options?.[0]?.name) !== "Renk" ||
    s(product.options?.[1]?.name) !== "Beden";
  if(needUpdate){
    await rest(`/products/${product.id}.json`, 'PUT', {
      product: { id: product.id, options: [{ name:"Renk" }, { name:"Beden" }] }
    });
    await sleep(200);
  }
}

/* ===== Ana akış ===== */
async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.log("Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL");
    process.exit(1);
  }

  console.log("XML okunuyor…");
  const models = await readXML();
  console.log("Model sayısı:", models.length);

  const groups = models.reduce((m, p)=>{
    const key = s(p.mpn) || s(p.baseTitle);
    (m.get(key) || m.set(key, []).get(key)).push(p);
    return m;
  }, new Map());

  let done = 0;
  for(const [mpn, items] of groups){
    const primary = items[0];
    const merged = {
      mpn,
      brand   : primary.brand,
      main    : primary.main,
      cat     : primary.cat,
      desc    : primary.desc,
      imgs    : uniq(items.flatMap(i=>i.imgs)),
      price   : primary.price,
      baseTitle: primary.baseTitle,
      variants: items.flatMap(i=>i.variants)
    };

    let product = await findProductByTitle(merged.baseTitle);
    if(!product){
      product = await createProduct(merged);
      console.log("OK (Yeni):", merged.baseTitle, "| Varyant:", merged.variants.length);
    }else{
      console.log("OK (Güncel):", merged.baseTitle, "| Varyant:", merged.variants.length);
    }

    const xmlMap = new Map();
    for(const v of merged.variants){
      xmlMap.set(s(v.barcode) || s(v.sku), { color:s(v.color), size:s(v.size) });
    }

    await ensureProductOptions(product, merged);
    await ensureOptionStrings(product, xmlMap);
    await linkImages(product, merged);

    for(const v of merged.variants){
      await upsertVariant(product, v);
      await sleep(150);
    }

    const minPrice = Math.min(...merged.variants.map(v=>num(v.price)));
    const status = (minPrice>0 ? "active" : "draft");
    if(s(product.status)!==status){
      await rest(`/products/${product.id}.json`, 'PUT', { product: { id: product.id, status } });
    }

    if((++done)%BATCH_SIZE===0) await sleep(1500);
  }

  console.log("Bitti.");
}

main().catch(e=>{
  console.error("Hata:", e.message);
  process.exit(1);
});
