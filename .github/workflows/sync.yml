import { XMLParser } from "fast-xml-parser";

/* ====== ENV ====== */
const SHOP_DOMAIN  = process.env.SHOP_DOMAIN;          // örn: "shkx8d-wy"
const ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;  // shpat_...
const SOURCE_URL   = process.env.SOURCE_URL;            // XML linkin
const API_VERSION  = "2024-07";
const BATCH_SIZE   = Number(process.env.BATCH_SIZE || 50);

if (!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL) {
  console.error("Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL");
  process.exit(1);
}

/* ====== Helpers ====== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const num   = (x) => { if (x==null) return 0; const n = Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x) => num(x).toFixed(2);
const uc    = (s) => String(s ?? "").toString().trim().toUpperCase();
const uniq  = (arr) => Array.from(new Set(arr.filter(Boolean)));
const clip  = (s,m=70) => { s=String(s||'').trim(); return s.length>m ? s.slice(0,m-1)+'…' : s; };

function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if (json){ h["Content-Type"] = "application/json"; }
  return h;
}

async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, { method, headers: headers(true), body: body ? JSON.stringify(body) : undefined });
  if (!res.ok){
    const t = await res.text();
    throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${t}`);
  }
  return await res.json().catch(()=> ({}));
}

/* ====== Shopify utils ====== */

// İlk lokasyon id (stok güncellemesi için)
let _locationId = null;
async function getLocationId(){
  if (_locationId) return _locationId;
  const js = await rest(`/locations.json`);
  _locationId = js.locations?.[0]?.id;
  return _locationId;
}

// SKU ile product_id bul
async function findProductIdBySKU(sku){
  const js = await rest(`/variants.json?sku=${encodeURIComponent(sku)}`);
  const v  = js.variants?.[0];
  return v ? v.product_id : null;
}

// Ürünün mevcut varyantlarını sku -> variantId eşle
async function getVariantMap(productId){
  const js = await rest(`/products/${productId}.json`);
  const map = new Map();
  (js.product?.variants || []).forEach(v => map.set(String(v.sku||""), { id:v.id, inventory_item_id:v.inventory_item_id }));
  return map;
}

// Varyant ekle
async function addVariant(productId, v){
  const js = await rest(`/products/${productId}/variants.json`, "POST", { variant: v });
  return js.variant;
}

// Varyant fiyat güncelle
async function updateVariantPrice(variantId, price){
  await rest(`/variants/${variantId}.json`, "PUT", { variant: { id: variantId, price: to2(price) }});
}

// Stok ayarla
async function setInventory(inventoryItemId, qty){
  const loc = await getLocationId();
  if (!loc || !inventoryItemId) return;
  await rest(`/inventory_levels/set.json`, "POST", { location_id: loc, inventory_item_id: inventoryItemId, available: Number(qty||0) });
}

// Ürün oluştur
async function createProduct(payload){
  const js = await rest(`/products.json`, "POST", { product: payload });
  return js.product;
}

// Ürüne görsel ekle (hata yutar)
async function addImage(productId, src){
  try{
    await rest(`/products/${productId}/images.json`, "POST", { image: { src }});
  }catch(_){ /* görsel indirilemezse atla */ }
}

/* ====== XML parse ====== */
const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_"
});

function textOf(x){
  if (x==null) return "";
  if (typeof x === "string") return x;
  if (typeof x === "number") return String(x);
  return x["#text"] || x._ || x.value || "";
}

function specOf(v, name){
  const s = v.spec;
  if (!s) return "";
  const arr = Array.isArray(s) ? s : [s];
  for (const it of arr){
    const n = uc(it?.["@_name"] || it?.name || "");
    if (n === uc(name)) return textOf(it);
  }
  return "";
}

/* ====== DÖNÜŞÜM ====== */
function productFromXml(p){
  const basePrice = num(p.Price);
  const brand     = uc(textOf(p.Brand));
  const titleRaw  = textOf(p.Name);            // XML başlık
  const code      = textOf(p.Product_code) || textOf(p.Mpn) || textOf(p.Product_id);

  // SEO’lu başlık: “{XML başlık (renk kırpılabilir)} – {Marka} {Kod}”
  // (Renk sonda boşluk sonrası tek kelime ise kırp)
  let titleBase = titleRaw.trim();
  // son kelime renk gibi ise bırakıyoruz; (istenirse kırpılabilir)
  const title   = `${titleBase} – ${brand} ${code}`.replace(/\s+/g,' ').trim();

  // Varyantlar
  const vn = p.variants?.variant ? (Array.isArray(p.variants.variant) ? p.variants.variant : [p.variants.variant]) : [];
  const variants = [];
  const colorValues = [];
  const sizeValues  = [];

  for (const v of vn){
    const color = uc(specOf(v, "Renk")) || uc(specOf(v, "Color"));
    const size  = String(specOf(v, "Beden") || specOf(v, "Size") || "").trim();
    const qty   = num(v.quantity ?? p.Stock);
    const vPrice= num(v.price);                // bazıları 0.00 gelebilir
    const price = vPrice > 0 ? vPrice : basePrice;

    const sku   = String(textOf(v.productCode) || textOf(v.barcode) || "").trim();
    const barcode = String(textOf(v.barcode) || "").trim();

    if (!size && !color) continue;             // en azından biri olmalı

    variants.push({
      option1: color || "Tek Renk",
      option2: size  || "Tek Beden",
      price:   to2(price),
      sku:     sku || `${code}-${color||'R'}-${size||'B'}`,
      barcode: barcode || undefined,
      inventory_management: "shopify",
      inventory_quantity: Number(qty||0)
    });

    if (color) colorValues.push(color);
    if (size)  sizeValues.push(size);
  }

  // bazı ürünler varyantsız gelebilir
  if (variants.length === 0){
    variants.push({
      option1: "Default Title",
      price:   to2(basePrice || 0),
      sku:     String(code || titleBase).replace(/\s+/g,'-'),
      inventory_management: "shopify",
      inventory_quantity: Number(num(p.Stock)||0)
    });
  }

  const options = [];
  const colorsU = uniq(colorValues);
  const sizesU  = uniq(sizeValues);
  if (colorsU.length) options.push({ name: "Renk", values: colorsU });
  if (sizesU.length)  options.push({ name: "Beden", values: sizesU });

  const images = [];
  for (let i=1;i<=5;i++){
    const url = textOf(p[`Image${i}`] || "");
    if (url) images.push(url.replace(/^http:\/\//,'https://'));
  }

  return {
    code, brand, title, basePrice,
    category: [textOf(p.mainCategory), textOf(p.category), textOf(p.subCategory)].filter(Boolean).join(" > "),
    description_html: textOf(p.Description) || "",
    options,
    variants,
    images
  };
}

/* ====== Ana İş ====== */
async function main(){
  console.log("XML okunuyor…");
  const resp = await fetch(SOURCE_URL);
  const xml  = await resp.text();
  const root = parser.parse(xml);
  const items = root?.Products?.Product ? (Array.isArray(root.Products.Product) ? root.Products.Product : [root.Products.Product]) : [];
  if (!items.length){ console.log("XML boş."); return; }

  console.log("Model sayısı:", items.length);

  for (let i=0; i<items.length; i+=BATCH_SIZE){
    const chunk = items.slice(i, i+BATCH_SIZE);

    for (const p of chunk){
      const pr = productFromXml(p);

      // İlk SKU ile ürünü bul
      const firstSKU = pr.variants[0]?.sku;
      let productId  = firstSKU ? await findProductIdBySKU(firstSKU) : null;

      if (!productId){
        // oluştur
        const payload = {
          title: pr.title,
          vendor: pr.brand || "Tedarikçi",
          product_type: pr.category || "",
          body_html: pr.description_html,
          options: pr.options.length ? pr.options : undefined,
          variants: pr.variants.map(v => ({
            option1: v.option1, option2: v.option2,
            price: v.price, sku: v.sku, barcode: v.barcode,
            inventory_management: "shopify",
            inventory_quantity: v.inventory_quantity
          }))
        };
        const created = await createProduct(payload);
        productId = created.id;

        // görseller
        for (const src of pr.images){ await addImage(productId, src); await sleep(200); }

        console.log("OK (Yeni):", clip(pr.title, 80), "| Varyant:", pr.variants.length);
      }else{
        // güncelle: sku eşleşen varsa fiyat/stok güncelle, yoksa varyant ekle
        const map = await getVariantMap(productId);
        for (const v of pr.variants){
          const hit = map.get(String(v.sku||""));
          if (hit){
            // fiyat + stok
            await updateVariantPrice(hit.id, v.price);
            await setInventory(hit.inventory_item_id, v.inventory_quantity);
          }else{
            const added = await addVariant(productId, {
              option1: v.option1, option2: v.option2,
              price: v.price, sku: v.sku, barcode: v.barcode,
              inventory_management: "shopify",
              inventory_quantity: v.inventory_quantity
            });
            await setInventory(added.inventory_item_id, v.inventory_quantity);
            await sleep(200);
          }
        }
        // eksikse görselleri tamamla
        for (const src of pr.images){ await addImage(productId, src); await sleep(200); }

        console.log("OK (Güncel):", clip(pr.title, 80), "| Varyant:", pr.variants.length);
      }

      await sleep(300); // Shopify rate-limit nazik
    }
  }
}

main().catch(e=>{
  console.error(e);
  process.exit(1);
});
