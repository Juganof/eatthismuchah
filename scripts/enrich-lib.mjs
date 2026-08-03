// scripts/enrich-lib.mjs — gedeelde logica voor lokale productverrijking.
//
// Achterna: de productverrijking (src/ingest/enrich.ts) draait vanuit de
// Cloudflare Worker-runtime niet: Akamai blokkeert de TLS-vingerafdruk van
// workerd en ook Node's eigen HTTP-stack krijgt 403. Bewezen wél werkend op
// deze machine is `curl.exe` met een browser-User-Agent, de cookies van een
// eerdere GET en Origin/Referer. Deze module bundelt daarom precies die
// aanpak: de lokale D1-database vinden, een Store erop openen (node:sqlite
// met een D1-adapter) en één recept verrijken via curl.exe op
// www.ah.nl/gql — suggesties, voedingswaarden en koppelingen.
//
// Eénmalige run: scripts/enrich-local.mjs. Continu: scripts/enrich-watch.mjs.
// Beide importeren eerst scripts/ts-bootstrap.mjs (transform-vlag +
// resolve-hook). De src-modules hieronder worden daarom dynamisch geïmporteerd:
// ESM linkt (en parst) het statische import-graf vóórdat ook maar één
// module-body draait, dus een statische TS-import zou in de parent-process
// (zonder transform-vlag) al stuklopen voordat de herstart in ts-bootstrap
// aan de beurt is. Dynamische imports lossen pas tijdens de evaluatie op, en
// dan staan de vlag en de hook al klaar. Onder vitest regelt de Vite-resolver
// het laden van TS zelf, dus daar werkt dit patroon ook.
//
// De query-strings, de matcher en de vrijstellingslijst komen rechtstreeks
// uit src/ — één bron van waarheid, met het gedrag door tests vastgelegd in
// test/gql.test.ts, test/enrich.test.ts en test/gql-nutrition.test.ts. Alleen
// het ontleden van de suggestie-respons (parseSuggestions) staat hier nog als
// spiegel van de rijen-parsing in suggestionsForRecipe (src/ah/gql.ts), want
// dat is daar geen aparte export. De zoekrespons-parser (parseSearchResults)
// hoort alleen bij deze lokale flow: de app zelf zoekt nooit losse producten.

import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import os from "node:os";

const { Store } = await import("../src/db/queries.ts");
const { parseTradeItem } = await import("../src/ah/gql-nutrition.ts");
const { matchSuggestionsToIngredients } = await import("../src/ingest/enrich.ts");
const { isNutritionFree, tokenize } = await import("../src/nutrition/resolve.ts");
const { BUNDLE_QUERY, GQL_URL, HOME_URL, NUTRITION_QUERY, SEARCH_QUERY, SUGGESTIONS_QUERY } = await import("../src/ah/gql.ts");

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite");

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
//
// De map is per proces (pid), NIET gedeeld: draaien er twee watchers tegelijk
// (twee start-app.bat-vensters), dan zouden ze elkaar's body-bestanden
// overschrijven en kreeg het ene recept de GraphQL-query van het andere —
// en daarmee de product-suggesties van een ander recept. Zo'n kruisbestuiving
// legde "pistachenoten -> AH Halfvolle melk" vast.
const WORK_DIR = join(os.tmpdir(), "ah-enrich-local-" + process.pid);
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
export function findLocalDb(rootDir) {
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

/**
 * Opent de lokale D1-database en geeft er een Store op terug (busy_timeout
 * 5000). De dev-server mag dezelfde database gewoon open hebben; een lock
 * waarop we langer dan die 5 seconden moeten wachten gooit. Sluiten kan met
 * `store.db.close()` — de adapter hangt onder dezelfde sleutel aan de Store.
 */
export function openStore(dbPath) {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA busy_timeout=5000");
  return new Store({
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
  });
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
export async function ensureSession() {
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
 * Eén POST op /gql via curl.exe, met de body als bestand (--data-binary) en
 * de cookie-jar mee. Retry bij 403/429: max 2 pogingen, met wachttijd
 * ertussen. Geeft de geparseerde JSON-respons terug.
 */
export async function curlJson(query, variables) {
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

/**
 * De curl-sessie als één object, zodat een functie hem als geheel kan
 * meekrijgen: eerst ensureSession() (één rustige GET), daarna per verzoek
 * gqlPost(query, variables).
 */
export function curlContext() {
  return { ensureSession, gqlPost: curlJson };
}

// ---------------------------------------------------------------------------
// Parsing en matchen
// ---------------------------------------------------------------------------

/**
 * De respons van recipeProductSuggestionsV2 naar ProductSuggestion-vorm.
 * Spiegel van de rijen-parsing in suggestionsForRecipe (src/ah/gql.ts), die
 * in test/gql.test.ts door tests is vastgelegd.
 *
 * Een 200-respons mét GraphQL-fouten of zonder data telt hier als fout: die
 * moet het recept niet "klaar" maken — alleen een geldige, lege lijst
 * betekent écht "AH heeft geen producten voor dit recept".
 */
function parseSuggestions(body) {
  const rows = body?.data?.recipeProductSuggestionsV2;
  if (!Array.isArray(rows)) {
    const detail = Array.isArray(body?.errors)
      ? body.errors.map((e) => (isRecord(e) ? e["message"] : null)).filter(Boolean).join("; ") || "onbekende fout"
      : "geen recipeProductSuggestionsV2 in de respons";
    throw new Error(`suggesties zonder bruikbare data: ${detail}`);
  }
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

/**
 * De zoekrespons (productSearch { products { id title salesUnitSize } }) naar
 * dezelfde ProductSuggestion-vorm als parseSuggestions, zodat de rest van de
 * flow er niet over hoeft te weten welke bron een koppeling leverde.
 *
 * Een 200-respons zonder products-lijst is geen resultaat én geen fout —
 * behalve wanneer de GraphQL-respons expliciete errors bevat: die telt als
 * fout, zodat een recept niet ten onrechte "klaar" wordt gevinkt terwijl de
 * zoekroute kapot is.
 */
export function parseSearchResults(body) {
  const search = isRecord(body?.data?.productSearch) ? body.data.productSearch : null;
  const rows = search ? search["products"] : null;
  if (!Array.isArray(rows)) {
    if (Array.isArray(body?.errors) && body.errors.length > 0) {
      const detail =
        body.errors.map((e) => (isRecord(e) ? e["message"] : null)).filter(Boolean).join("; ") ||
        "onbekende fout";
      throw new Error(`zoekrespons zonder bruikbare data: ${detail}`);
    }
    return [];
  }
  const out = [];
  for (const row of rows) {
    if (!isRecord(row)) continue;
    out.push({
      ingredientName: str(row["title"]) ?? "",
      productId: String(numOf(row["id"]) ?? "") || null,
      productTitle: str(row["title"]) ?? null,
      salesUnitSize: str(row["salesUnitSize"]) ?? null,
      suggestedPackages: null,
    });
  }
  return out;
}

// ---------------------------------------------------------------------------
// De verrijkingsflow zelf
// ---------------------------------------------------------------------------

// Bovengrens op het aantal zoekquery's per recept: elke zoekactie kost een
// verzoek aan /gql, en een recept met veel losse regels zou anders het hele
// tempo-budget van een ronde opslurpen. Regels die hierdoor niet aan bod
// komen, blijven simpelweg open.
const MAX_ZOEKOPDRACHTEN = 5;

/**
 * Zoekt één product voor een ingrediëntnaam via de webshop-zoekquery
 * (SEARCH_QUERY, dezelfde query als de handmatige zoekbalk van de app) en
 * geeft de eerste hit terug — AH rankt zelf. De naam wordt genormaliseerd
 * zoals de app dat doet (tokenize uit src/nutrition/resolve.ts): "verse
 * basilicum" zoekt dus "basilicum". Null als er geen hit is of de naam niets
 * zoekbaars oplevert. Gooit alleen bij netwerkfouten of een expliciete
 * foutrespons; de aanroeper telt dat per regel als fout.
 */
async function searchFallback(curlCtx, ingredientName) {
  const query = tokenize(ingredientName).join(" ");
  if (!query) return null;
  const body = await curlCtx.gqlPost(SEARCH_QUERY, { input: { query } });
  const first = parseSearchResults(body)[0];
  if (!first?.productId) return null;
  return first;
}

/**
 * De voedingswaarde per 100 g van één product, met de bundel-fallback:
 * heeft een virtuele bundel (2-pack e.d.) geen tradeItem, dan volgen we de
 * eerste variant uit virtualBundleProducts — maximaal één stap dieper,
 * precies zoals productNutritionInner in src/ah/gql.ts.
 */
async function fetchProductNutrition(curlCtx, id, depth = 0) {
  if (id === null) return null;
  const body = await curlCtx.gqlPost(NUTRITION_QUERY, { id });
  const product = body?.data?.product;
  if (!isRecord(product)) return null;

  const parsed = parseTradeItem(product["tradeItem"]);
  if (parsed) return parsed;

  if (depth >= 1) return null;
  const bundle = (await curlCtx.gqlPost(BUNDLE_QUERY, { id }))?.data?.product?.["virtualBundleProducts"];
  if (Array.isArray(bundle) && bundle.length > 0 && isRecord(bundle[0])) {
    const variant = bundle[0]["product"];
    if (isRecord(variant)) {
      const variantId = numOf(variant["id"]);
      if (variantId !== null) return fetchProductNutrition(curlCtx, variantId, depth + 1);
    }
  }
  return null;
}

/**
 * De niet-vrije ingrediënten van een recept die nog geen productkoppeling
 * hebben. Vrije ingrediënten (water, zout, peper — zie isNutritionFree in
 * src/nutrition/resolve.ts) leveren geen voedingswaarde op en hoeven dus
 * nooit verrijkt te worden; een recept dat alleen daarop "open" staat is
 * klaar. Opgeslagen null-koppelingen ("geen match") tellen niet mee, want
 * matchMap geeft alleen niet-nul-links terug.
 */
export async function openIngredientNames(store, recipe) {
  const names = recipe.ingredients.map((i) => i.name).filter((n) => !isNutritionFree(n));
  const known = await store.matchMap(names);
  return names.filter((n) => !(n in known));
}

// ---------------------------------------------------------------------------
// Klaar-markering
// ---------------------------------------------------------------------------

/**
 * Een recept krijgt een "klaar"-markering zodra een verrijkingspoging zonder
 * fouten afliep — ook als sommige ingrediënten ongematcht blijven (AH heeft
 * niet voor elke regel een product). Elke ronde opnieuw proberen zou die
 * recepten voor altijd blijven bevragen zonder iets op te leveren. Een poging
 * mét fouten (403, netwerk) krijgt géén markering en komt dus netjes terug in
 * een volgende ronde. De markering staat in app_state en verdwijnt dus met
 * de wisknop mee (wipe wist app_state ook).
 */

const DONE_KEY = "enrich:done:";

export async function isRecipeDone(store, recipeId) {
  return (await store.getState(DONE_KEY + recipeId)) === "1";
}

export async function markRecipeDone(store, recipeId) {
  await store.setState(DONE_KEY + recipeId, "1");
}

/**
 * Of een recept een verrijkingsronde nodig heeft: nog niet klaar gemarkeerd
 * én met minstens één niet-vrij ingrediënt zonder koppeling. Dit is de
 * selectie die de watcher per ronde maakt.
 */
export async function needsEnrichment(store, recipe) {
  if (await isRecipeDone(store, recipe.id)) return false;
  return (await openIngredientNames(store, recipe)).length > 0;
}

/**
 * Verrijkt één recept: de suggesties ophalen, koppelen aan de regels en
 * producten + koppelingen bewaren. Regels waarvoor AH geen suggestie geeft,
 * worden via de webshop-zoekquery alsnog gekoppeld (max MAX_ZOEKOPDRACHTEN
 * zoekacties per recept). Slaat het recept over (null) zodra alle
 * niet-vrije ingrediënten al een koppeling hebben. Gooit nooit: mislukte
 * verzoeken worden per regel gerapporteerd, zoals enrichRecipeWithProducts
 * dat ook doet.
 *
 * Retourneert een samenvatting { gekoppeld, nieuw, cached, fouten } of null
 * als er niets te doen was.
 */
export async function enrichOneRecipe(store, curlCtx, recipe) {
  if ((await openIngredientNames(store, recipe)).length === 0) return null;

  const id = numOf(recipe.id);
  if (id === null) return { gekoppeld: 0, nieuw: 0, cached: 0, fouten: 0 };

  let suggestions;
  try {
    suggestions = parseSuggestions(
      await curlCtx.gqlPost(SUGGESTIONS_QUERY, {
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
    return { gekoppeld: 0, nieuw: 0, cached: 0, fouten: 1 };
  }

  const perIngredient = matchSuggestionsToIngredients(recipe.ingredients, suggestions);
  const seen = new Set();
  let gekoppeld = 0;
  let nieuw = 0;
  let cached = 0;
  let fouten = 0;
  let zoekopdrachten = 0;

  for (const [index, ingredient] of recipe.ingredients.entries()) {
    let suggestion = perIngredient[index];
    let score = 1;
    // Geen suggestie van AH? Zoek het product zelf via de webshop-zoekquery
    // en koppel de eerste hit. Vrije ingrediënten (water, zout, peper)
    // worden nooit gezocht. De koppeling krijgt score 0.8: geen AH's eigen
    // recept-suggestie maar een zoekresultaat.
    if (!suggestion?.productId && !isNutritionFree(ingredient.name) && zoekopdrachten < MAX_ZOEKOPDRACHTEN) {
      zoekopdrachten++;
      try {
        suggestion = await searchFallback(curlCtx, ingredient.name);
        if (suggestion) score = 0.8;
      } catch (err) {
        fouten++;
        console.error(`  zoek '${ingredient.name}' mislukt: ${err.message}`);
      }
    }
    if (!suggestion?.productId) continue;

    gekoppeld++;
    // De koppeling op naam kost niets en hangt de boodschappenlijst aan;
    // AH's eigen suggestie is per definitie de juiste: score 1.
    await store.putMatch(ingredient.name, suggestion.productId, score);

    if (seen.has(suggestion.productId)) continue;
    seen.add(suggestion.productId);

    if (await store.getProduct(suggestion.productId)) {
      cached++;
      continue;
    }

    let nutrition = null;
    try {
      nutrition = await fetchProductNutrition(curlCtx, numOf(suggestion.productId));
    } catch (err) {
      fouten++;
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
    nieuw++;
  }

  return { gekoppeld, nieuw, cached, fouten };
}
