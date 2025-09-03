import { XMLParser } from "fast-xml-parser";

/* ====== Config (Secrets üzerinden gelir) ====== */
const SHOP_DOMAIN   = process.env.SHOP_DOMAIN;
const ACCESS_TOKEN  = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION   = "2024-07";
const SOURCE_URL    = process.env.SOURCE_URL;
const PRIMARY_DOMAIN= process.env.PRIMARY_DOMAIN || "www.solederva.com";
const BATCH_SIZE    = Number(process.env.BATCH_SIZE || 50);   // GitHub'da geniş rahat

/* ====== Küçük yardımcılar ====== */
const sleep = (ms)=> new Promise(r=>setTimeout(r,ms));
const num   = (x)=> { if(x==null) return 0; const n=Number(String(x).replace(',','.')); return isNaN(n)?0:n; };
const to2   = (x)=> { const n=num(x); return n>0 ? n.toFixed(2) : "0.00"; };
const uc    = (s)=> (s||"").toString().trim().toUpperCase();
const uniq  = (arr)=> Array.from(new Set(arr.filter(Boolean)));
const clip  = (s,m)=> { s=String(s||"").trim(); return s.length>m? s.slice(0,m-1)+"…" : s; };

function headers(json=true){
  const h = { "X-Shopify-Access-Token": ACCESS_TOKEN };
  if(json) h["Content-Type"] = "application/json";
  return h;
}
async function rest(path, method="GET", body){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, {
    method, headers: headers(true),
    body: body ? JSON.stringify(body) : undefined
  });
  const txt = await res.text();
  if(res.ok) return txt? JSON.parse(txt) : {};
  throw new Error(`Shopify ${method} ${path} -> ${res.status}: ${txt}`);
}
async function gql(query, variables={}){
  const url = `https://${SHOP_DOMAIN}.myshopify.com/admin/api/${API_VERSION}/graphql.json`;
  const res = await fetch(url, {
    method: "POST",
    headers: headers(true),
    body: JSON.stringify({ query, variables })
  });
  const txt = await res.text();
  const out = txt? JSON.parse(txt) : {};
  if(res.ok && !out.errors) return out.data;
  throw new Error(`GraphQL ${res.status}: ${txt}`);
}
const isValidEAN = (x)=> /^\d{8}$/.test(String(x||"").trim()) || /^\d{12,14}$/.test(String(x||"").trim());

/* ====== XML okuma ====== */
async function fetchXML(){
  const res = await fetch(SOURCE_URL, { redirect:"follow" });
  if(!res.ok) throw new Error(`XML fetch ${res.status}`);
  const xml = await res.text();
  const parser = new XMLParser({
    ignoreAttributes: false,
    cdataPropName: "__cdata",
    trimValues: true
  });
  return parser.parse(xml);
}
function arrify(x){ return Array.isArray(x)? x : x!=null? [x] : []; }
function t(x){ return (x && typeof x==="object" && "__cdata" in x) ? String(x.__cdata||"").trim() : String(x||"").trim(); }

/* ====== Başlık/etiket/seo ====== */
function shortFromCode(code){
  const m = String(code||"").trim().match(/^([A-Za-z]+[0-9]+)/);
  return m? m[1] : String(code||"").trim();
}
const INTERNAL_TOKENS = ["CST","CBT","RBT","RST","FE"];
function extractInternalToken(name){
  const m = String(name||"").trim().match(/^([A-ZÇĞİÖŞÜ]{2,4})(?:\s+|[\s]*[-–][\s]*)/);
  return (m && INTERNAL_TOKENS.includes(m[1])) ? m[1] : "";
}
function extractStyleToken(name){
  const H = uc(name||" ");
  const PAIRS = [
    ["VERNICIATA","verniciata"], ["PELLE","pelle"],
    ["MAT","mat"], ["PARLAK","parlak"],
    ["SÜET","suet"], ["SUET","suet"],
    ["NUBUK","nubuk"], ["KETEN","keten"], ["CANVAS","keten"], ["MESH","mesh"]
  ];
  for(const [needle,key] of PAIRS){ if(H.includes(needle)) return key; }
  return "";
}
function moveBrandCodeToEnd(name, brand, code){
  const norm = (s)=> (s||"").toString().replace(/\s+/g," ").trim();
  let n = norm(name), b=norm(brand), c=norm(code), shortC=shortFromCode(c);
  const rx = (p)=> new RegExp("^"+p.replace(/[.*+?^${}()|[\]\\]/g,"\\$&")+"\\s*[-–:|]?\\s*","i");
  [b, shortC, (c!==shortC?c:"")].filter(Boolean).forEach(p=> n=n.replace(rx(p),"").trim() );
  const tok = extractInternalToken(n);
  if (tok) n = n.replace(/^([A-ZÇĞİÖŞÜ]{2,4})(?:\s+|[\s]*[-–][\s]*)/,"").trim();
  const tail = [tok,b,c].filter(Boolean).join(" ");
  return tail ? `${n} – ${tail}` : n;
}
function stripColorsFromName(name, colorSet){
  let n = (name||"").toString().replace(/\s+/g," ").trim();
  const tokens = Array.from(colorSet||[]).filter(Boolean).map(s=> s.replace(/[.*+?^${}()|[\]\\]/g,"\\$&") );
  if(!tokens.length) return n;
  const tail = new RegExp(`(?:\\s*[-–/|]\\s*)?(?:${tokens.join("|")})\\s*$`, "i");
  n = n.replace(tail,"").trim();
  const head = new RegExp(`^(?:${tokens.join("|")})\\s*(?:[-–/|]\\s*)?`, "i");
  n = n.replace(head,"").trim();
  return n.replace(/\s*[-–/|]\s*$/,"").replace(/\s{2,}/g," ").trim();
}
const seoTitle = (core, brand, code)=> clip(`${core} | ${[brand,code].filter(Boolean).join(" ")}`, 64);
const seoDesc  = (brand, core)=> clip(`${brand?brand+" ":""}${core} güncel renk ve numara seçenekleri. Hızlı kargo, kolay iade.`, 158);

function classifyTags({name,desc,mainCategory,category,subCategory}){
  const lc = (s)=> (s||"").toString().toLowerCase();
  const hay = lc([name,(desc||"").replace(/<[^>]*>/g," "),mainCategory,category,subCategory].join(" "));
  let productType="Erkek Ayakkabı";
  const has=(k)=> hay.includes(k);
  if(has("bot") || has("boot")) productType="Bot";
  else if(has("loafer") || has("mokasen")) productType="Loafer";
  else if(has("spor") || has("sneaker") || has("tenis")) productType="Spor Ayakkabı";
  else if(has("keten")) productType="Keten Ayakkabı";
  const display=[], machine=[];
  if(productType==="Bot") display.push("Bot");
  if(productType==="Loafer") display.push("Loafer");
  if(productType==="Spor Ayakkabı") display.push("Spor Ayakkabı");
  if(productType==="Keten Ayakkabı") display.push("Keten Ayakkabı");
  if(has("keten")) display.push("Keten"), machine.push("malzeme:keten");
  if(["deri","pelle","verniciata","nubuk"].some(k=>has(k))) display.push("Deri"), machine.push("malzeme:deri");
  if(["süet","suede"].some(k=>has(k))) display.push("Süet"), machine.push("malzeme:süet");
  if(has("vegan")) display.push("Vegan"), machine.push("ozellik:vegan");
  if(productType==="Bot") machine.push("tip:bot");
  if(productType==="Loafer") machine.push("tip:loafer");
  if(productType==="Spor Ayakkabı") machine.push("tip:spor");
  if(productType==="Keten Ayakkabı") machine.push("tip:keten");
  return { productType, tags: uniq(["xml-import","auto-sync",...display,...machine]) };
}

/* ====== Görsel URL kontrol (http/https fallback) ====== */
function encodeUrl(u){ return String(u||"").replace(/ /g,"%20"); }
async function isReachable(u){
  try{
    const r = await fetch(u, { method:"GET", redirect:"follow" });
    return r.ok;
  }catch{ return false; }
}
async function bestUrl(raw){
  if(!raw) return "";
  let u = encodeUrl(String(raw).trim());
  const cand = [];
  if(/^https?:\/\//i.test(u)) cand.push(u);
  else cand.push("https://"+u, "http://"+u);
  if(u.startsWith("http://")) cand.push("https://"+u.slice(7));
  if(u.startsWith("https://")) cand.push("http://"+u.slice(8));
  for(const c of uniq(cand)){ if(await isReachable(c)) return c; }
  return "";
}

/* ====== Shopify yardımcıları ====== */
async function getLocationId(){
  const j = await rest("/locations.json");
  if(!j.locations || !j.locations.length) throw new Error("No location found");
  return j.locations[0].id;
}
async function findProductIdByFirstSKU(sku){
  if(!sku) return null;
  const q = `
    query($q:String!){
      productVariants(first:1, query:$q){
        edges{ node{ sku product{ id tags } } }
      }
    }`;
  const data = await gql(q, { q: `sku:${sku}` });
  const edges = data?.productVariants?.edges || [];
  if(!edges.length) return null;
  const node = edges[0].node;
  const gid  = node.product.id;
  const id   = Number(String(gid).split("/").pop());
  return { productId:id, productTags: node.product.tags||"" };
}
async function createProduct(p){ return rest("/products.json","POST",{ product:p }); }
async function updateProduct(productId, patch){ return rest(`/products/${productId}.json`,"PUT",{ product:{ id:productId, ...patch } }); }
async function setInventory(inventoryItemId, qty, locationId){
  return rest("/inventory_levels/set.json","POST",{ location_id: locationId, inventory_item_id: inventoryItemId, available: Math.max(0, Number(qty||0)) });
}
async function updateVariant(variantId, patch){ return rest(`/variants/${variantId}.json`,"PUT",{ variant:{ id:variantId, ...patch } }); }
async function updateVariantPrice(variantId, price){
  const n=num(price); if(n<=0) return;
  return updateVariant(variantId, { price: to2(n) });
}
async function createImageWithVariants(productId, src, variantIds){
  return rest(`/products/${productId}/images.json`, "POST", { image: { src, variant_ids: variantIds }});
}
async function updateImageVariants(productId, imageId, variantIds){
  return rest(`/products/${productId}/images/${imageId}.json`, "PUT", { image: { id:imageId, variant_ids: variantIds }});
}

/* ====== Parse & grup ====== */
function parseAndGroup(doc){
  const items = arrify(doc?.Products?.Product);
  const groups = new Map();

  for(const p of items){
    const brand = t(p.Brand)||"";
    const pCode = t(p.Product_code)||"";
    const nameRaw= t(p.Name)||"";
    const descHtml= t(p.Description)||"";
    const priceTop= num(t(p.Price));
    const mainCategory=t(p.mainCategory);
    const category=t(p.category);
    const subCategory=t(p.subCategory);
    const images = uniq([t(p.Image1),t(p.Image2),t(p.Image3),t(p.Image4),t(p.Image5)]).filter(Boolean);

    const seri  = extractInternalToken(nameRaw);
    const style = extractStyleToken(nameRaw);
    const familyKey = `${brand}|${shortFromCode(pCode)}|${seri||style||"STD"}`;

    const vlist = arrify(p?.variants?.variant);

    for(let i=0;i<vlist.length;i++){
      const v=vlist[i];
      const specs = arrify(v?.spec);
      const renk  = t(specs.find(s=> (s?.["@_name"]||"").toLowerCase()==="renk")) || "";
      const beden = t(specs.find(s=> (s?.["@_name"]||"").toLowerCase()==="beden")) || "";
      let barcode = t(v.barcode); if(!isValidEAN(barcode)) barcode="";
      const qty = num(t(v.quantity));
      const pv  = num(t(v.price));
      const chosen = pv>0 ? pv : priceTop;
      const sku = barcode || `${pCode||"SKU"}_${renk||"R"}_${beden||("B"+(i+1))}`;

      const rec = {
        familyKey, brand, pCode, nameRaw, descHtml, priceTop,
        mainCategory, category, subCategory,
        renk, beden, sku, barcode,
        qty: (chosen>0 ? Math.max(0,qty) : 0),
        price: (chosen>0 ? to2(chosen) : "0.00"),
        images: images.slice(0)
      };
      if(!groups.has(familyKey)) groups.set(familyKey, []);
      groups.get(familyKey).push(rec);
    }
  }

  const out=[];
  for(const [key, arr] of groups.entries()){
    const first = arr[0];
    const colorSet = new Set(arr.map(a=> (a.renk||"").trim()).filter(Boolean));
    const nameNoColor = stripColorsFromName(first.nameRaw, colorSet);
    const displayTitle = moveBrandCodeToEnd(nameNoColor, first.brand, first.pCode);
    const core = displayTitle.split(" – ")[0];
    const cls = classifyTags({
      name: core, desc: arr.map(a=>a.descHtml).join("\n"),
      mainCategory:first.mainCategory, category:first.category, subCategory:first.subCategory
    });

    // Renk -> görseller
    const colorImages = new Map(); // UPPER renk -> Set(src)
    for(const a of arr){
      const ck = uc(a.renk||"");
      if(!colorImages.has(ck)) colorImages.set(ck, new Set());
      a.images.forEach(src=> colorImages.get(ck).add(src));
    }

    // Aynı Renk|Beden tekille
    const pickBetter = (A,B)=>{
      const sc = (r)=> (r.barcode?100:0) + (num(r.price)>0?10:0) + Math.min(5,Math.max(0,num(r.qty)));
      return sc(B)>sc(A)? B : A;
    };
    const ded = new Map();
    for(const a of arr){
      const k = `${uc(a.renk||"")}|${uc(a.beden||"")}`;
      ded.set(k, ded.has(k)? pickBetter(ded.get(k), a) : a);
    }

    out.push({
      familyKey: key,
      brand: first.brand,
      displayTitle,
      coreName: core,
      seoTitle: seoTitle(core, first.brand, shortFromCode(first.pCode)),
      seoDesc : seoDesc(first.brand, core),
      productType: cls.productType,
      tags: uniq([`family:${key}`, `marka:${first.brand}`, ...cls.tags]),
      colorImages,
      variants: Array.from(ded.values())
    });
  }
  return out;
}

/* ====== Renk → Medya bağlama ====== */
async function linkColorMedia(productId, productJson, group){
  const cvs  = productJson?.product?.variants || [];
  const imgs = productJson?.product?.images  || [];

  // Renk -> variantIds
  const colorToVid = new Map();
  for(const cv of cvs){
    const color = uc(cv.option1||"");
    if(!colorToVid.has(color)) colorToVid.set(color, []);
    colorToVid.get(color).push(cv.id);
  }

  // src -> [variantIds]
  const need = new Map();
  for(const [color,setSrc] of group.colorImages.entries()){
    const vids = colorToVid.get(color)||[];
    for(const raw of setSrc){
      const good = await bestUrl(raw);
      if(good){
        need.set(good, vids);
      }
    }
  }

  const haveSrc = new Set(imgs.map(im=>im.src));
  // Eksikleri yükle
  for(const [src, vids] of need.entries()){
    if(!haveSrc.has(src)){
      await createImageWithVariants(productId, src, vids);
      await sleep(400);
    }
  }

  // Yeniden çek
  const full = await rest(`/products/${productId}.json`,"GET");
  const imgs2 = full?.product?.images || [];
  const src2id = new Map(imgs2.map(im=>[im.src, im.id]));

  // image.variant_ids set + her varyant için image_id eşle
  for(const [src, vids] of need.entries()){
    const imgId = src2id.get(src);
    if(!imgId) continue;
    await updateImageVariants(productId, imgId, vids);
    await sleep(300);
  }

  // Her renk için ilk görseli varyant image_id olarak işaretle
  for(const [color,setSrc] of group.colorImages.entries()){
    const firstSrc = await (async ()=>{
      for(const raw of setSrc){
        const good = await bestUrl(raw);
        if(good) return good;
      }
      return "";
    })();
    if(!firstSrc) continue;
    const imgId = src2id.get(firstSrc);
    if(!imgId) continue;

    const vids = (colorToVid.get(color)||[]);
    for(const vid of vids){
      await updateVariant(vid, { image_id: imgId });
      await sleep(200);
    }
  }
}

/* ====== Ana akış ====== */
async function main(){
  if(!SHOP_DOMAIN || !ACCESS_TOKEN || !SOURCE_URL){
    console.error("Eksik ayar: SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN / SOURCE_URL");
    process.exit(1);
  }

  console.log("XML okunuyor…");
  const doc = await fetchXML();
  const groups = parseAndGroup(doc);
  console.log(`Model sayısı: ${groups.length}`);

  const locId = await getLocationId();

  // Hepsini tek koşuda yapıyoruz (GitHub Actions süresi geniş)
  let processed = 0;
  for(const g of groups.slice(0, BATCH_SIZE || groups.length)){
    const firstSku = g.variants.find(v=>v.sku)?.sku;
    let found = await findProductIdByFirstSKU(firstSku);
    let productId = null;
    if(found && found.productId){
      const tags = String(found.productTags||"");
      if(tags.includes(`family:${g.familyKey}`)) productId = found.productId;
    }

    const options = [{name:"Renk"},{name:"Beden"}];
    const vCreate = g.variants.map(v=>({
      sku: v.sku,
      price: to2(Math.max(num(v.price),1)),   // 0 fiyatla açma
      option1: v.renk || null,
      option2: v.beden || null,
      inventory_management: "shopify",
      barcode: v.barcode || null
    }));

    // Ürünü oluştur / güncelle
    if(!productId){
      const created = await createProduct({
        title: g.displayTitle,
        body_html: g.variants[0].descHtml || g.variants[0].descHtml?.replace(/<[^>]*>/g," ") || "",
        vendor: g.brand,
        product_type: g.productType,
        tags: g.tags.join(","),
        options,
        variants: vCreate
      });
      productId = created.product.id;
      await sleep(300);
    }else{
      await updateProduct(productId, {
        title: g.displayTitle,
        body_html: g.variants[0].descHtml || g.variants[0].descHtml?.replace(/<[^>]*>/g," ") || "",
        vendor: g.brand,
        product_type: g.productType,
        tags: g.tags.join(",")
      });
      await sleep(200);
    }

    // Ürün ayrıntılarını çek
    let full = await rest(`/products/${productId}.json`,"GET");
    const cvs  = full?.product?.variants || [];

    // Fiyat / stok (0 fiyat → stok 0 + ürün draft)
    let anyPrice = false;
    for(const cv of cvs){
      const m = g.variants.find(x=> x.sku===cv.sku) || g.variants.find(x=> uc(x.renk)===uc(cv.option1||"") && uc(x.beden)===uc(cv.option2||""));
      if(!m) continue;
      if(num(m.price)>0){ anyPrice = true; await updateVariantPrice(cv.id, m.price); await sleep(150); }
      await setInventory(cv.inventory_item_id, m.qty, locId); await sleep(150);
    }

    // Görselleri bağla (renk → media)
    await linkColorMedia(productId, full, g);

    // SEO + durum
    await updateProduct(productId, {
      metafields_global_title_tag: g.seoTitle,
      metafields_global_description_tag: g.seoDesc,
      status: anyPrice ? "active" : "draft"
    });
    await sleep(150);

    // Metafield: public url (isteğe bağlı)
    try{
      const handle = full?.product?.handle;
      if(handle){
        await rest(`/products/${productId}/metafields.json`,"POST",{ metafield:{
          namespace:"custom", key:"public_url", value:`https://${PRIMARY_DOMAIN}/products/${handle}`, type:"single_line_text_field"
        }});
      }
    }catch{}

    processed++;
    console.log(`OK: ${g.displayTitle} | Varyant: ${g.variants.length}`);
  }

  console.log(`Bitti. İşlenen model: ${processed}/${groups.length} (batch=${BATCH_SIZE})`);
}

main().catch(e=>{ console.error(e); process.exit(1); });
