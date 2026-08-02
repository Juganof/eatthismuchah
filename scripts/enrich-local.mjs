// scripts/enrich-local.mjs — lokaal producten ophalen voor de lokale D1-database.
//
// De productverrijking (src/ingest/enrich.ts) draait vanuit de Cloudflare
// Worker-runtime niet: Akamai blokkeert de TLS-vingerafdruk van workerd en ook
// Node's eigen HTTP-stack krijgt 403. Bewezen wél werkend op deze machine is
// `curl.exe` met een browser-User-Agent, de cookies van een eerdere GET en
// Origin/Referer. Dit script doet daarom precies dat: per recept de
// product-suggesties en voedingswaarden via curl.exe van www.ah.nl/gql halen
// en in de lokale D1-database (onder .wrangler/state/) schrijven, via de
//zelfde Store-klasse en parsers als de app zelf.
//
// Node kan de TS-modules uit src/ niet zonder meer laden: constructor-
// parameter-eigenschappen (src/db/queries.ts) vragen transform-ondersteuning,
// en de extensieloze relatieve imports vragen een resolve-hook. Het script
// start zichzelf daarom automatisch opnieuw op met --experimental-transform-
// types en registreert de hook zelf.

import { spawnSync } from "node:child_process";
import { createRequire, registerHooks } from "node:module";
import { mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";

const SCRIPT_URL = import.meta.url;

// Zonder --experimental-transform-types kan Node src/db/queries.ts niet laden
// (constructor-parameter-eigenschappen), dus starten we onszelf opnieuw op met
// de vlag. Een env-marker voorkomt een oneindige herstartlus als de vlag op
// deze Node niet (meer) bestaat.
if (process.features.typescript !== "transform") {
  if (process.env.ENRICH_LOCAL_TRANSFORMED === "1") {
    console.error("Kan de TS-modules uit src/ niet laden: --experimental-transform-types lijkt hier niet te werken.");
    process.exit(1);
  }
  const child = spawnSync(
    process.execPath,
    [
      "--experimental-transform-types",
      "--disable-warning=ExperimentalWarning",
      fileURLToPath(SCRIPT_URL),
      ...process.argv.slice(2),
    ],
    { stdio: "inherit", env: { ...process.env, ENRICH_LOCAL_TRANSFORMED: "1" } },
  );
  process.exit(child.status ?? 1);
}

// De TS-modules importeren we pas hierna, dynamisch: de resolve-hook hieronder
// moet eerst staan, want de src-modules gebruiken extensieloze relatieve
// imports ("../ah/types") die Node zonder hook niet kan vinden.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier.startsWith(".") && !/\.[a-zA-Z0-9]+$/.test(specifier)) {
      try {
        return nextResolve(specifier + ".ts", context);
      } catch {
        // geen .ts-buurman: laat de normale resolutie het proberen
      }
    }
    return nextResolve(specifier, context);
  },
});

const { Store } = await import("../src/db/queries.ts");
const { parseTradeItem } = await import("../src/ah/gql-nutrition.ts");

const require = createRequire(SCRIPT_URL);
const { DatabaseSync } = require("node:sqlite");

// ---------------------------------------------------------------------------
// De query-strings en URL's spiegelen de exports in src/ah/gql.ts
// (SUGGESTIONS_QUERY, NUTRITION_QUERY, BUNDLE_QUERY, GQL_URL, HOME_URL). Die
// module is vanuit Node niet te laden — parameter-eigenschappen én
// extensieloze imports — vandaar deze kopie; houd hem gelijk aan de bron.
// ---------------------------------------------------------------------------

const GQL_URL = "https://www.ah.nl/gql";
const HOME_URL = "https://www.ah.nl/";

const NUTRITION_QUERY = `query P($id: Int!) {
  product(id: $id) {
    __typename id title
    tradeItem { __typename gtin nutritions {
      __typename basisQuantity basisQuantityDescription preparationState servingSize servingSizeDescription
      nutrients { type name value }
    } }
  }
}`;

const BUNDLE_QUERY = `query P($id: Int!) {
  product(id: $id) { __typename id title virtualBundleProducts { product { id title } } }
}`;

const SUGGESTIONS_QUERY = `query recipeProductSuggestions($options: RecipeProductSuggestionV2Input!) {
  recipeProductSuggestionsV2(options: $options) {
    optional
    ingredient { id name quantityFloat quantityUnit }
    productSuggestion { id quantity proposer
      product { id title brand webPath salesUnitSize }
    }
  }
}`;

// Browser-identiteit zoals de webshop hem verwacht; de combinatie met
// curl.exe en cookies is de enige die Akamai hier doorlaat (zie README).
const BROWSER_HEADERS = [
  "-H",
  "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  "-H",
  "Accept-Language: nl-NL,nl;q=0.9",
  "-H",
  "Origin: https://www.ah.nl/",
  "-H",
  "Referer: https://www.ah.nl/",
  "-H",
  "x-client-platform-type: Web",
  "-H",
  "x-client-name: ah-allerhande",
  "-H",
  "x-client-version: 3.23.124",
];

// Werkmap voor cookies en tussentijdse body-bestanden; de cookie-jar blijft
// staan tussen runs, zodat de sessie van een vorige keer hergebruikt wordt.
const WORK_DIR = join(os.tmpdir(), "ah-enrich-local");
const COOKIE_JAR = join(WORK_DIR, "cookies.txt");
const BODY_IN = join(WORK_DIR, "body.json");
const BODY_OUT = join(WORK_DIR, "body-out.json");

const CURL_TIMEOUT_MS = 20_000;
// Rust tussen verzoeken aan ah.nl; Akamai reageert op tempo.
const PACE_MS = 1100;
// Wachten na een 403/429 voordat we het opnieuw proberen (max 2 pogingen).
const RETRY_MS = 4000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isRecord(v) {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** Recept-id ("R-R1202157") of webshop-id naar een positief geheel getal. */
function numOf(v) {
  if (typeof v === "number" && Number.isInteger(v) && v > 0) return v;
  if (typeof v === "string") {
    const m = v.replace(/^R-R/, "").match(/^\d+$/);
    if (m) return Number(m[0]);
  }
  return null;
}

function str(v) {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

// ---------------------------------------------------------------------------
// Lokale D1-database
// ---------------------------------------------------------------------------

/** Zoekt het echte D1-databasebestand onder .wrangler/state/ van de repo. */
function findLocalD1(rootDir) {
  const candidates = [];
  const walk = (dir) => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".sqlite")) candidates.push(full);
    }
  };
  walk(join(rootDir, ".wrangler"));

  // Miniflare zet naast het echte bestand ook lege metadata.sqlite-bestanden
  // neer; het D1-bestand zelf is veruit het grootste.
  return (
    candidates
      .filter((p) => !p.endsWith("metadata.sqlite"))
      .sort((a, b) => statSync(b).size - statSync(a).size)[0] ?? null
  );
}

// D1-achtig adapter-object over node:sqlite, precies zoals test/helpers/d1.ts:
// prepare/bind/first/all/run + batch/exec, zodat de echte Store-klasse er
// zonder wijzigingen op draait. De dev-server kan dezelfde database open
// hebben; busy_timeout zorgt dat we netjes wachten op een lock.
class LocalStatement {
  constructor(db, sql, params = []) {
    this.db = db;
    this.sql = sql;
    this.params = params;
  }

  bind(...params) {
    return new LocalStatement(this.db, this.sql, params.map(normalise));
  }

  async first(column) {
    const row = this.db.prepare(this.sql).get(...this.params);
    if (row === undefined) return null;
    const clean = plainify(row);
    return column ? (clean[column] ?? null) : clean;
  }

  async all() {
    const rows = this.db.prepare(this.sql).all(...this.params);
    return { results: rows.map(plainify), success: true, meta: {} };
  }

  async run() {
    const info = this.db.prepare(this.sql).run(...this.params);
    return {
      success: true,
      meta: { changes: Number(info.changes), last_row_id: Number(info.lastInsertRowid) },
    };
  }
}

/** node:sqlite weigert undefined en booleans; D1 accepteert beide. */
function normalise(value) {
  if (value === undefined) return null;
  if (typeof value === "boolean") return value ? 1 : 0;
  return value;
}

/** node:sqlite geeft null-prototype objecten en BigInts terug; maak ze gewoon. */
function plainify(row) {
  const out = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = typeof value === "bigint" ? Number(value) : value;
  }
  return out;
}

function openLocalD1(path) {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA busy_timeout=5000");
  return {
    prepare: (sql) => new LocalStatement(db, sql),
    async batch(statements) {
      const out = [];
      for (const statement of statements) out.push(await statement.run());
      return out;
    },
    async exec(sql) {
      db.exec(sql);
      return { count: 0, duration: 0 };
    },
    close: () => db.close(),
  };
}

// ---------------------------------------------------------------------------
// curl.exe-helper
// ---------------------------------------------------------------------------

function curl(args) {
  const res = spawnSync("curl.exe", args, {
    encoding: "utf8",
    timeout: CURL_TIMEOUT_MS,
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
  });
  if (res.error) throw new Error(`curl.exe starten mislukt: ${res.error.message}`);
  if (res.status !== 0) {
    throw new Error(`curl.exe eindigde met status ${res.status}: ${(res.stderr ?? "").slice(0, 400)}`);
  }
  return res.stdout ?? "";
}

/** Eén curl-aanroep mét het verplichte rustmoment erna (Akamai-tempo). */
async function pacedCurl(args) {
  const out = curl(args);
  await sleep(PACE_MS);
  return out;
}

/** Opent de sessie: één rustige GET op de homepage die de cookie-jar vult. */
async function ensureSession() {
  mkdirSync(WORK_DIR, { recursive: true });
  for (let attempt = 1; ; attempt++) {
    const code = await pacedCurl([
      "--compressed",
      "-s",
      "-c",
      COOKIE_JAR,
      "-H",
      "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
      "-H",
      "Accept-Language: nl-NL,nl;q=0.9",
      "-o",
      BODY_OUT,
      "-w",
      "%{http_code}",
      HOME_URL,
    ]);
    if (code === "200") return;
    if ((code === "403" || code === "429") && attempt < 2) {
      console.error(`  sessie-GET gaf ${code}; herkansing over ${RETRY_MS / 1000} s`);
      await sleep(RETRY_MS);
      continue;
    }
    throw new Error(`GET ${HOME_URL} -> ${code}`);
  }
}

/**
 * Eén POST op /gql via curl.exe, met de body als bestand (--data-binary) en de
 * cookie-jar mee. Retry bij 403/429: max 2 pogingen, met wachttijd ertussen.
 */
async function gqlPost(query, variables) {
  mkdirSync(WORK_DIR, { recursive: true });
  // fs.writeFileSync schrijft BOM-loos; met een BOM zou curl hem meesturen.
  writeFileSync(BODY_IN, JSON.stringify({ query, variables }), "utf8");

  const args = [
    "--compressed",
    "-s",
    "-b",
    COOKIE_JAR,
    "-c",
    COOKIE_JAR,
    "--data-binary",
    "@" + BODY_IN,
    "-H",
    "Content-Type: application/json;charset=UTF-8",
    "-H",
    "Accept: application/json",
    ...BROWSER_HEADERS,
    "-o",
    BODY_OUT,
    "-w",
    "%{http_code}",
    GQL_URL,
  ];

  for (let attempt = 1; ; attempt++) {
    const code = await pacedCurl(args);
    if (code === "200") {
      const body = readFileSync(BODY_OUT, "utf8");
      try {
        return JSON.parse(body);
      } catch (err) {
        throw new Error(`POST ${GQL_URL} leverde geen JSON: ${err.message}`);
      }
    }
    if ((code === "403" || code === "429") && attempt < 2) {
      console.error(`  POST ${GQL_URL} -> ${code}; herkansing over ${RETRY_MS / 1000} s`);
      await sleep(RETRY_MS);
      continue;
    }
    throw new Error(`POST ${GQL_URL} -> ${code}`);
  }
}

// ---------------------------------------------------------------------------
// Parsing en matchen — spiegelt src/ah/gql.ts (suggestionsForRecipe) en
// src/ingest/enrich.ts (matchSuggestionsToIngredients), inclusief de
// tokenizer uit src/nutrition/resolve.ts. De modules zelf zijn vanuit Node
// niet te laden; het gedrag is elders al door tests vastgelegd.
// ---------------------------------------------------------------------------

/** De respons van recipeProductSuggestionsV2 naar ProductSuggestion-vorm. */
function parseSuggestions(body) {
  const rows = body?.data?.recipeProductSuggestionsV2 ?? [];
  const out = [];
  for (const row of rows) {
    if (!isRecord(row)) continue;
    const ingredient = isRecord(row["ingredient"]) ? row["ingredient"] : null;
    const suggestion = isRecord(row["productSuggestion"]) ? row["productSuggestion"] : null;
    const product = suggestion && isRecord(suggestion["product"]) ? suggestion["product"] : null;
    const productId =
      product !== null && isRecord(product)
        ? String(numOf(product["id"] ?? suggestion?.["id"]) ?? "") || null
        : null;
    out.push({
      ingredientName: str(ingredient?.["name"]) ?? "",
      productId,
      productTitle: str(product?.["title"]) ?? null,
      salesUnitSize: str(product?.["salesUnitSize"]) ?? null,
      suggestedPackages: suggestion ? numOf(suggestion["quantity"]) : null,
    });
  }
  return out;
}

/** Stopwoorden voor het naam-matchen; spiegelt resolve.ts. */
const NOISE = new Set([
  "verse", "vers", "rijpe", "rijp", "grote", "groot", "kleine", "klein", "fijne", "fijn",
  "gesneden", "gesnipperde", "gesnipperd", "geraspte", "geraspt", "gehakte", "gehakt",
  "gepelde", "gepeld", "geschilde", "geschild", "blokjes", "reepjes", "plakjes", "ringen",
  "in", "van", "de", "het", "een", "of", "en", "met", "naar", "smaak", "extra",
  "biologische", "bio", "ongezouten", "gezouten", "magere", "volle", "halfvolle",
  "ah", "huismerk", "voor", "erbij", "eventueel", "optioneel", "stuks", "stuk",
]);

function tokenize(text) {
  return text
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ")
    .split(/[^a-zàâäçéèêëîïôöûüù]+/i)
    .map((t) => t.trim())
    .filter((t) => t.length > 2 && !NOISE.has(t));
}

/** Koppelt suggesties aan recept-regels, net als matchSuggestionsToIngredients. */
function matchSuggestionsToIngredients(ingredients, suggestions) {
  const out = new Array(ingredients.length).fill(null);

  const byKey = new Map();
  suggestions.forEach((s, i) => {
    const key = tokenize(s.ingredientName).join(" ");
    if (!key) return;
    const bucket = byKey.get(key);
    if (bucket) bucket.push(i);
    else byKey.set(key, [i]);
  });

  const used = new Set();
  ingredients.forEach((ing, index) => {
    const key = tokenize(ing.name).join(" ");
    const pool = key ? byKey.get(key) ?? [] : [];
    const pick = pool.find((i) => !used.has(i));
    if (pick !== undefined) {
      used.add(pick);
      out[index] = suggestions[pick];
      return;
    }
    if (index < suggestions.length && !used.has(index)) {
      used.add(index);
      out[index] = suggestions[index];
    }
  });
  return out;
}

// ---------------------------------------------------------------------------
// De verrijkingsflow zelf
// ---------------------------------------------------------------------------

/**
 * De voedingswaarde per 100 g van één product, met de bundel-fallback:
 * heeft een virtuele bundel (2-pack e.d.) geen tradeItem, dan volgen we de
 * eerste variant uit virtualBundleProducts — maximaal één stap dieper,
 * precies zoals productNutritionInner in src/ah/gql.ts.
 */
async function fetchProductNutrition(id, depth = 0) {
  if (id === null) return null;
  const body = await gqlPost(NUTRITION_QUERY, { id });
  const product = body?.data?.product;
  if (!isRecord(product)) return null;

  const parsed = parseTradeItem(product["tradeItem"]);
  if (parsed) return parsed;

  if (depth >= 1) return null;
  const bundle = (await gqlPost(BUNDLE_QUERY, { id }))?.data?.product?.["virtualBundleProducts"];
  if (Array.isArray(bundle) && bundle.length > 0 && isRecord(bundle[0])) {
    const variant = bundle[0]["product"];
    if (isRecord(variant)) {
      const variantId = numOf(variant["id"]);
      if (variantId !== null) return fetchProductNutrition(variantId, depth + 1);
    }
  }
  return null;
}

/**
 * Haalt voor één recept de suggesties op, koppelt ze aan de regels en bewaart
 * producten + koppelingen. Gooit nooit: mislukte verzoeken worden per regel
 * gerapporteerd, zoals enrichRecipeWithProducts dat ook doet.
 */
async function enrichOne(store, recipe) {
  const id = numOf(recipe.id);
  if (id === null) return { matched: 0, products: 0, cached: 0, errors: 0 };

  let suggestions;
  try {
    suggestions = parseSuggestions(
      await gqlPost(SUGGESTIONS_QUERY, {
        options: {
          recipeId: id,
          numberOfServings: recipe.servings > 0 ? recipe.servings : 4,
          productIdOverride: [],
          ingredientsToOverride: [],
        },
      }),
    );
  } catch (err) {
    console.error(`  suggesties mislukt: ${err.message}`);
    return { matched: 0, products: 0, cached: 0, errors: 1 };
  }

  const perIngredient = matchSuggestionsToIngredients(recipe.ingredients, suggestions);
  const seen = new Set();
  let matched = 0;
  let products = 0;
  let cached = 0;
  let errors = 0;

  for (const [index, ingredient] of recipe.ingredients.entries()) {
    const suggestion = perIngredient[index];
    if (!suggestion?.productId) continue;

    matched++;
    // De koppeling op naam kost niets en hangt de boodschappenlijst aan;
    // AH's eigen suggestie is per definitie de juiste: score 1.
    await store.putMatch(ingredient.name, suggestion.productId, 1);

    if (seen.has(suggestion.productId)) continue;
    seen.add(suggestion.productId);

    if (await store.getProduct(suggestion.productId)) {
      cached++;
      continue;
    }

    let nutrition = null;
    try {
      nutrition = await fetchProductNutrition(numOf(suggestion.productId));
    } catch (err) {
      errors++;
      console.error(`  product ${suggestion.productId} (${suggestion.productTitle ?? "?"}) mislukt: ${err.message}`);
      continue;
    }

    // Ook zonder voedingswaarde bewaren we het product: dan weten we dat we het
    // al opgehaald hebben en kost een volgend recept er geen verzoek aan.
    await store.putProduct({
      webshopId: suggestion.productId,
      title: suggestion.productTitle ?? suggestion.productId,
      salesUnitSize: suggestion.salesUnitSize,
      per100g: nutrition?.per100g ?? {},
    });
    products++;
  }

  return { matched, products, cached, errors };
}

// ---------------------------------------------------------------------------
// Opstart en uitvoer
// ---------------------------------------------------------------------------

function printUsage() {
  console.log(`Gebruik: node scripts/enrich-local.mjs [--help]

Haalt voor elk recept in de lokale D1-database de echte AH-producten op en
schrijft ze terug: voedingswaarde per 100 g (voor de planner) en de koppeling
per ingrediënt (voor de boodschappenlijst).

  --help   deze uitleg

Vanuit de Worker-runtime blokkeert Akamai /gql; dit script gebruikt daarom
curl.exe met een browser-sessie. Zie README, "Lokaal verrijken".

Eisen:
  - curl.exe moet op de PATH staan.
  - De lokale database moet bestaan: start minimaal één keer
    \`wrangler dev\` of draai \`npm run db:init:local\`.
    De dev-server mag gewoon blijven draaien; het script wacht op locks.

Het script houdt zelf ≥1 s rust tussen verzoeken aan ah.nl en kan een recept
overslaan waarvan alle ingrediënten al gekoppeld zijn.`);
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const repoRoot = join(dirname(fileURLToPath(SCRIPT_URL)), "..");
  const d1File = findLocalD1(repoRoot);
  if (d1File === null) {
    console.error("Geen lokale D1-database gevonden onder .wrangler/state/.");
    console.error("Start eerst `wrangler dev` (of draai `npm run db:init:local`) en probeer het daarna opnieuw.");
    process.exitCode = 1;
    return;
  }

  const store = new Store(openLocalD1(d1File));
  const startedAt = Date.now();
  const totals = { processed: 0, skipped: 0, matched: 0, products: 0, cached: 0, errors: 0 };

  try {
    console.log(`Database: ${d1File}`);
    await ensureSession();

    const ids = await store.allRecipeIds();
    if (ids.length === 0) {
      console.log("Geen recepten in de database; niets te verrijken.");
      return;
    }

    for (const recipeId of ids) {
      const recipe = await store.getRecipe(recipeId);
      if (!recipe) {
        console.log(`${recipeId}: recept niet gevonden (overgeslagen)`);
        continue;
      }

      // Overslaan zodra álle ingrediënten al een koppeling hebben; dan heeft
      // dit recept niets meer te zoeken (matchMap geeft alleen niet-nul-links).
      const names = recipe.ingredients.map((i) => i.name);
      const known = await store.matchMap(names);
      const open = names.filter((n) => !(n in known));
      if (open.length === 0) {
        totals.skipped++;
        console.log(`${recipe.id}: alle ${names.length} ingrediënten al gekoppeld (overgeslagen)`);
        continue;
      }

      totals.processed++;
      const line = await enrichOne(store, recipe);
      totals.matched += line.matched;
      totals.products += line.products;
      totals.cached += line.cached;
      totals.errors += line.errors;
      const suffix = line.errors > 0 ? `, ${line.errors} fout(en)` : "";
      console.log(
        `${recipe.id}: ${line.matched}/${recipe.ingredients.length} ingrediënten gekoppeld, ` +
          `${line.products} producten nieuw, ${line.cached} uit cache${suffix}`,
      );
    }
  } finally {
    const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `\nKlaar in ${duration} s: ${totals.processed} recepten verrijkt, ${totals.skipped} overgeslagen, ` +
        `${totals.matched} koppelingen, ${totals.products} producten nieuw, ` +
        `${totals.cached} uit cache, ${totals.errors} fout(en).`,
    );
    store.db.close();
  }
}

await main();
