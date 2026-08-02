import type { Nutrients, Product, RawIngredient, Recipe } from "./types";
import { isKnownUnit } from "../nutrition/units";
import { resetPace, sharedPace } from "./pace";
import { deepFind, extractEmbeddedJson } from "./scrape";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const AUTH_URL = "https://api.ah.nl/mobile-auth/v1/auth/token/anonymous";
const PRODUCT_SEARCH_URL = "https://api.ah.nl/mobile-services/product/search/v2";
const PRODUCT_DETAIL_URL = "https://api.ah.nl/mobile-services/product/detail/v4/fir";
const PRODUCT_PAGE_URL = "https://www.ah.nl/producten/product/wi";
const PRODUCT_SEARCH_PAGE = "https://www.ah.nl/producten/zoeken";
const SITE_BASE = "https://www.ah.nl";
const RECIPE_BASE = `${SITE_BASE}/allerhande`;

/**
 * De JSON-zoekdienst van Allerhande gaf in de praktijk 404 op élke aanroep, en
 * dan alsnog de HTML-pagina ophalen kost twee verzoeken in plaats van één. Uit
 * een budget van veertig is dat te duur om per zoekterm te herhalen, dus na de
 * eerste misser slaat de rest van dit isolate hem over.
 */
let recipeSearchJsonDead = false;

/**
 * De mobiele API vraagt eerst een anoniem token op, en dat verzoek kreeg in de
 * praktijk 403 terug — waarna élke productzoekopdracht faalde en er dus geen
 * enkel ingredient meer gekoppeld kon worden. De website zelf doet het gewoon,
 * dus na zo'n weigering gaat de rest van dit isolate via www.ah.nl.
 */
let productApiDead = false;

/**
 * Een worker mag maar een beperkt aantal uitgaande verzoeken doen per aanroep
 * (op het gratis plan 50). Ging dat op, dan brak Cloudflare de ronde midden in
 * een recept af met "Too many subrequests" — een fout per resterend recept, en
 * geen enkele aanwijzing dat het aan óns budget lag en niet aan AH. De client
 * telt daarom zelf mee en stopt netjes vóór de grens.
 */
export class SubrequestBudgetError extends Error {
  constructor(public readonly used: number) {
    super(`subrequest-budget bereikt na ${used} verzoeken aan ah.nl`);
    this.name = "SubrequestBudgetError";
  }
}

export const isBudgetError = (err: unknown): err is SubrequestBudgetError =>
  err instanceof SubrequestBudgetError || /subrequest/i.test(err instanceof Error ? err.message : "");

/**
 * Zet de module-brede vlaggen terug (welke AH-endpoints als dood gelden). Alleen
 * voor tests: die draaien in één isolate, en een vlag die van de ene test in de
 * andere lekt levert een raadselachtige mislukking op.
 */
export function resetEndpointState(): void {
  recipeSearchJsonDead = false;
  productApiDead = false;
  resetPace();
}

/** Wat er gescraped werd, zodat het archief doorzoekbaar blijft per soort. */
export type ScrapeKind = "recipe" | "recipe_search" | "product" | "product_search";

export interface RawScrape {
  kind: ScrapeKind;
  /** Recept-id, webshop-id of zoekterm. */
  ref: string;
  url: string;
  status: number;
  body: string;
}

/**
 * AH exposes no public API, so every endpoint here is the one its own apps use and
 * can change without notice. Each call therefore degrades to HTML scraping rather
 * than throwing, and `probe()` reports which paths are currently alive.
 */
export class AhClient {
  private token: string | null = null;

  /**
   * `onRaw` krijgt elke response binnen voordat er geparsed wordt. Daar hangt de
   * archivering aan: gaat het parsen daarna stuk, dan is de payload toch bewaard.
   * Fouten uit de callback worden ingeslikt — archiveren mag een scrape nooit
   * laten mislukken.
   */
  private readonly minIntervalMs: number;
  private readonly maxRetries: number;
  private readonly backoffMs: number;
  private readonly maxRequests: number;
  /** Verzoeken die deze client gedaan heeft; de teller achter het budget. */
  private requests = 0;

  constructor(
    private readonly userAgent: string,
    private readonly onRaw?: (raw: RawScrape) => void,
    options: {
      minIntervalMs?: number;
      maxRetries?: number;
      backoffMs?: number;
      maxRequests?: number;
      /** Sla de (dode) JSON-zoekdienst over zonder hem eerst te proberen. */
      skipRecipeJsonSearch?: boolean;
    } = {},
  ) {
    // 700 ms is genoeg om onder de botbescherming te blijven en houdt een ingest
    // van 20 recepten nog binnen een halve minuut. In tests staat het op 0.
    this.minIntervalMs = options.minIntervalMs ?? 700;
    this.maxRetries = options.maxRetries ?? 2;
    this.backoffMs = options.backoffMs ?? 1500;
    // 40 is de veilige marge onder de 50 van het gratis plan: er moeten ook nog
    // een auth-aanroep en de laatste herkansingen in passen.
    this.maxRequests = options.maxRequests ?? 40;
    // Een isolate leeft één cron-ronde; de kennis dat dit endpoint dood is,
    // komt daarom van buiten (uit app_state) in plaats van elke ronde opnieuw
    // met een 404 betaald te worden.
    if (options.skipRecipeJsonSearch) recipeSearchJsonDead = true;
  }

  /** Hoeveel verzoeken deze client al gedaan heeft, en hoeveel er nog mogen. */
  get budget(): { used: number; max: number } {
    return { used: this.requests, max: this.maxRequests };
  }

  private record(raw: RawScrape): void {
    if (!this.onRaw) return;
    try {
      this.onRaw(raw);
    } catch {
      // archivering is nooit belangrijker dan het antwoord zelf
    }
  }

  private async authHeaders(): Promise<Record<string, string>> {
    if (!this.token) {
      const res = await fetch(AUTH_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": this.userAgent,
          "X-Application": "AHWEBSHOP",
        },
        body: JSON.stringify({ clientId: "appie" }),
      });
      if (!res.ok) throw new Error(`AH anonymous auth failed: ${res.status}`);
      const body = (await res.json()) as { access_token?: string };
      if (!body.access_token) throw new Error("AH auth response had no access_token");
      this.token = body.access_token;
    }
    return {
      Authorization: `Bearer ${this.token}`,
      "User-Agent": this.userAgent,
      "X-Application": "AHWEBSHOP",
      Accept: "application/json",
    };
  }

  /** Retries once without a cached token, so an expired token self-heals. */
  private async apiGet(url: string, archive?: { kind: ScrapeKind; ref: string }): Promise<unknown> {
    for (let attempt = 0; attempt < 2; attempt++) {
      await this.pace();
      const res = await fetch(url, { headers: await this.authHeaders() });
      if (res.status === 401 && attempt === 0) {
        this.token = null;
        continue;
      }
      // Lees als tekst, niet als JSON: ook een onparseerbaar antwoord hoort in
      // het archief, en dat is juist het geval waarin je het nodig hebt.
      const text = await res.text();
      if (archive) this.record({ ...archive, url, status: res.status, body: text });
      if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
      return JSON.parse(text);
    }
    throw new Error(`GET ${url} failed after re-auth`);
  }

  /**
   * ah.nl staat achter Akamai's botbescherming. Die kijkt niet naar het totale
   * aantal verzoeken maar naar het tempo: drie pagina's binnen een tiende
   * seconde levert 403's op, terwijl dezelfde pagina's rustig achter elkaar
   * gewoon binnenkomen. Vandaar een minimale tussenpoos tussen twee verzoeken
   * en een herkansing met oplopende wachttijd als het tóch misgaat.
   */
  private async pace(): Promise<void> {
    if (this.requests >= this.maxRequests) throw new SubrequestBudgetError(this.requests);
    this.requests++;
    await sharedPace(this.minIntervalMs);
  }

  private async htmlGet(url: string, archive?: { kind: ScrapeKind; ref: string }): Promise<string> {
    let lastStatus = 0;

    for (let attempt = 0; attempt < this.maxRetries + 1; attempt++) {
      await this.pace();
      const res = await fetch(url, {
        headers: {
          "User-Agent": this.userAgent,
          Accept: "text/html,application/xhtml+xml",
          "Accept-Language": "nl-NL,nl;q=0.9",
        },
      });
      const text = await res.text();
      // Elke poging het archief in, ook de geblokkeerde: dat is precies hoe we
      // erachter kwamen dat het om tempo ging en niet om de inhoud.
      if (archive) this.record({ ...archive, url, status: res.status, body: text });
      if (res.ok) return text;

      lastStatus = res.status;
      // 403 en 429 zijn "te snel", geen "bestaat niet": die zijn het proberen waard.
      const worthRetrying = res.status === 403 || res.status === 429 || res.status >= 500;
      if (!worthRetrying || attempt === this.maxRetries) break;
      await sleep(this.backoffMs * 2 ** attempt);
    }

    throw new Error(`GET ${url} -> ${lastStatus}`);
  }

  // ---------------------------------------------------------------- products

  async searchProducts(query: string, size = 10): Promise<Product[]> {
    if (!productApiDead) {
      try {
        return await this.searchProductsApi(query, size);
      } catch (err) {
        if (err instanceof SubrequestBudgetError) throw err;
        // Alleen het weigeren van het token is reden om over te stappen. Een
        // 403 of 429 op de zoekopdracht zelf is Akamai's tempo-blokkade, en die
        // treft de website net zo hard: dat hoort een blokkade te blijven, niet
        // stilletjes "geen product gevonden" te worden.
        if (!/auth failed/.test(err instanceof Error ? err.message : "")) throw err;
        productApiDead = true;
      }
    }
    return await this.searchProductsHtml(query, size);
  }

  private async searchProductsApi(query: string, size: number): Promise<Product[]> {
    // Ask for a few extras because AH places virtual multi-packs first for some
    // searches; those cannot supply a meaningful per-100g nutrition record.
    const upstreamSize = size + 10;
    const url = `${PRODUCT_SEARCH_URL}?query=${encodeURIComponent(query)}&size=${upstreamSize}`;
    const body = (await this.apiGet(url, { kind: "product_search", ref: query })) as {
      products?: unknown[];
    };
    const products = Array.isArray(body.products) ? body.products : [];
    return products
      .map((p) => toProductStub(p))
      .filter((p): p is Product => p !== null)
      .slice(0, size);
  }

  /**
   * Dezelfde zoekopdracht, maar via de gewone webshoppagina. De paginastate
   * bevat de producten als objecten; leest die niet, dan blijven de links naar
   * productpagina's over — het webshop-id staat in de URL en de titel in de slug,
   * en meer heeft de matcher niet nodig om te scoren.
   */
  private async searchProductsHtml(query: string, size: number): Promise<Product[]> {
    const html = await this.htmlGet(
      `${PRODUCT_SEARCH_PAGE}?query=${encodeURIComponent(query)}`,
      { kind: "product_search", ref: query },
    );
    const embedded = collectProducts(extractEmbeddedJson(html));
    const found = embedded.length > 0 ? embedded : parseProductLinks(html);
    return found.slice(0, size);
  }

  /** Full product record including nutrition, which search results omit. */
  async getProduct(webshopId: string): Promise<Product | null> {
    const body = await this.apiGet(`${PRODUCT_DETAIL_URL}/${encodeURIComponent(webshopId)}`, {
      kind: "product",
      ref: webshopId,
    });
    const card = deepFind(body, (v) => isRecord(v) && "webshopId" in v && "title" in v);
    const stub = toProductStub(card ?? body);
    if (!stub) return null;
    let per100g = parseNutrition(body);
    // The v4 mobile detail response stopped including nutrition in 2026. The
    // server-rendered product page still contains a labelled per-100g table.
    if (per100g.kcal === undefined || per100g.protein === undefined) {
      const html = await this.htmlGet(`${PRODUCT_PAGE_URL}${encodeURIComponent(webshopId)}`, {
        kind: "product",
        ref: webshopId,
      });
      per100g = parseNutritionHtml(html);
    }
    return { ...stub, webshopId, per100g };
  }

  /**
   * Alleen de voedingswaarde per 100 g, van de server-rendered productpagina.
   *
   * De v4-detailaanroep levert die sinds 2026 niet meer, dus die is voor een
   * product uit een zoekresultaat pure verspilling: titel en verpakking staan al
   * in het zoekresultaat, en de voedingswaarde komt tóch van de HTML-pagina.
   * Twee verzoeken per product werd er zo één — bij een recept met vijftien
   * ingredienten scheelt dat vijftien verzoeken uit een budget van veertig.
   */
  async getProductNutrition(webshopId: string): Promise<Nutrients> {
    const html = await this.htmlGet(`${PRODUCT_PAGE_URL}${encodeURIComponent(webshopId)}`, {
      kind: "product",
      ref: webshopId,
    });
    return parseNutritionHtml(html);
  }

  // ----------------------------------------------------------------- recipes

  /**
   * Allerhande search. The JSON service is tried first; if AH has moved it we fall
   * back to pulling the embedded Next.js state out of the search page HTML.
   */
  async searchRecipes(query: string, size = 20): Promise<Recipe[]> {
    const jsonUrl = `${RECIPE_BASE}/service/search/recipes?searchTerm=${encodeURIComponent(query)}&size=${size}`;
    if (!recipeSearchJsonDead) {
      try {
        const body = await this.apiGet(jsonUrl, { kind: "recipe_search", ref: query });
        const found = collectRecipes(body);
        if (found.length > 0) return found.slice(0, size);
        recipeSearchJsonDead = true;
      } catch (err) {
        if (err instanceof SubrequestBudgetError) throw err;
        recipeSearchJsonDead = true;
      }
    }
    const html = await this.htmlGet(
      `${RECIPE_BASE}/recepten-zoeken?query=${encodeURIComponent(query)}`,
      { kind: "recipe_search", ref: query },
    );
    const embedded = collectRecipes(extractEmbeddedJson(html));
    return (embedded.length > 0 ? embedded : parseRecipeCards(html)).slice(0, size);
  }

  /** Recipe detail. Ingredient lists only ever appear in the embedded page state. */
  async getRecipe(id: string): Promise<Recipe | null> {
    const html = await this.htmlGet(`${RECIPE_BASE}/recept/${encodeURIComponent(id)}`, {
      kind: "recipe",
      ref: id,
    });
    const found = collectRecipes(extractEmbeddedJson(html));
    // The detail page embeds the current recipe plus "related recipe" cards that have
    // no ingredients; the real one is whichever carries an ingredient list.
    return found.find((r) => r.ingredients.length > 0) ?? found[0] ?? null;
  }

  /** Reports which upstream endpoints still respond, for diagnosing breakage. */
  async probe(): Promise<Record<string, string>> {
    const out: Record<string, string> = {};
    const step = async (name: string, fn: () => Promise<unknown>) => {
      try {
        const v = await fn();
        out[name] = `ok (${Array.isArray(v) ? v.length : typeof v})`;
      } catch (err) {
        out[name] = `FAIL: ${err instanceof Error ? err.message : String(err)}`;
      }
    };
    await step("auth", () => this.authHeaders());
    await step("productSearch", () => this.searchProducts("kipfilet", 3));
    await step("recipeSearch", () => this.searchRecipes("kip", 3));
    return out;
  }
}

// -------------------------------------------------------------------- parsing

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function num(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    // Verwijzingen binnen de flight-stream ("$21:props:children:...") zijn geen
    // getallen maar zien er wel zo uit: hieronder zou dat 21 opleveren. Zo werd
    // een borrelhapje voor 4 personen een recept van "21 porties".
    if (v.startsWith("$")) return null;
    // AH writes values like "12,5 g" and "1.234 kJ".
    const m = v.replace(",", ".").match(/-?\d+(\.\d+)?/);
    if (m) return Number(m[0]);
  }
  return null;
}

function str(v: unknown): string | null {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

export function toProductStub(v: unknown): Product | null {
  if (!isRecord(v)) return null;
  // Virtual multi-packs have no single per-100g nutrition table. Matching an
  // ingredient to one would make the planner silently calculate with zeroes.
  if (v["isVirtualBundle"] === true || (Array.isArray(v["bundleItems"]) && v["bundleItems"].length > 0)) {
    return null;
  }
  const id = v["webshopId"] ?? v["id"];
  const title = str(v["title"]) ?? str(v["name"]);
  if (id === undefined || id === null || !title) return null;
  return {
    webshopId: String(id),
    title,
    salesUnitSize: str(v["salesUnitSize"]) ?? str(v["unitSize"]),
    per100g: {},
  };
}

/** Maps AH's Dutch nutrient labels onto our keys. Order matters: longest match wins. */
const NUTRIENT_LABELS: [RegExp, keyof Nutrients][] = [
  [/verzadigd/i, "fat"], // saturated fat is a sub-row; skip it below
  [/energie|calorie|kcal/i, "kcal"],
  [/eiwit|protein/i, "protein"],
  [/koolhydra|carbohydr/i, "carbs"],
  [/vezel|fibre|fiber/i, "fiber"],
  [/vet|fat/i, "fat"],
];

/**
 * Pulls per-100g nutrition out of a product detail payload. AH has shipped several
 * shapes over the years, so this walks the whole object looking for label/value
 * pairs rather than following a fixed path.
 */
export function parseNutrition(body: unknown): Nutrients {
  const out: Nutrients = {};
  const seen = new Set<string>();

  const visit = (v: unknown): void => {
    if (Array.isArray(v)) {
      v.forEach(visit);
      return;
    }
    if (!isRecord(v)) return;

    const label = str(v["name"]) ?? str(v["label"]) ?? str(v["nutrientName"]);
    const rawValue = v["value"] ?? v["valuePer100g"] ?? v["amount"] ?? v["quantity"];
    const value = num(rawValue);

    if (label && value !== null) {
      // "waarvan verzadigde vetzuren" etc. are sub-rows of a macro we already have.
      const isSubRow = /waarvan|verzadigd|suiker|zout|natrium/i.test(label);
      if (!isSubRow) {
        for (const [re, key] of NUTRIENT_LABELS) {
          if (key === "fat" && /verzadigd/i.test(label)) break;
          if (!re.test(label)) continue;
          if (seen.has(key)) break;
          // Energy is listed as kJ first, then kcal, and AH puts the unit in the
          // label on some payloads and in the value on others — check both.
          const withUnit = `${label} ${String(rawValue)}`;
          if (key === "kcal" && /kj/i.test(withUnit) && !/kcal/i.test(withUnit)) break;
          out[key] = key === "kcal" ? kcalValue(rawValue, label) ?? value : value;
          seen.add(key);
          break;
        }
      }
    }
    Object.values(v).forEach(visit);
  };

  visit(body);
  return out;
}

/** Reads AH's current server-rendered "Per 100 Gram" nutrition table. */
export function parseNutritionHtml(html: string): Nutrients {
  const table = html.match(
    /<table[^>]*data-testid="nutrition-table"[^>]*>([\s\S]*?)<\/table>/i,
  )?.[1];
  if (!table) return {};

  const rows: { name: string; value: string }[] = [];
  for (const match of table.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...(match[1] ?? "").matchAll(/<td\b[^>]*>([\s\S]*?)<\/td>/gi)].map((m) =>
      htmlText(m[1] ?? ""),
    );
    if (cells.length >= 2) rows.push({ name: cells[0]!, value: cells[1]! });
  }
  return parseNutrition({ rows });
}

function kcalValue(raw: unknown, label: string): number | null {
  const text = `${label} ${String(raw)}`.replace(",", ".");
  const match = text.match(/(\d+(?:\.\d+)?)\s*kcal/i);
  return match?.[1] ? Number(match[1]) : null;
}

function htmlText(value: string): string {
  return decodeHtml(value.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim());
}

/** Alles in een payload dat een product kan zijn, ontdaan van dubbelen. */
export function collectProducts(root: unknown): Product[] {
  const out: Product[] = [];
  const seen = new Set<string>();

  const visit = (v: unknown): void => {
    if (Array.isArray(v)) {
      v.forEach(visit);
      return;
    }
    if (!isRecord(v)) return;
    if ("webshopId" in v && ("title" in v || "name" in v)) {
      const stub = toProductStub(v);
      if (stub && !seen.has(stub.webshopId)) {
        seen.add(stub.webshopId);
        out.push(stub);
      }
    }
    Object.values(v).forEach(visit);
  };

  visit(root);
  return out;
}

/**
 * De laatste terugval: de links naar productpagina's in de HTML. De slug is de
 * titel met streepjes ("ah-magere-kwark"), wat voor de matcher net zo bruikbaar
 * is als de echte titel — die vergelijkt toch op losse woorden.
 */
export function parseProductLinks(html: string): Product[] {
  const out: Product[] = [];
  const seen = new Set<string>();
  for (const match of html.matchAll(/\/producten\/product\/wi(\d+)\/([a-z0-9-]+)/gi)) {
    const id = match[1];
    const slug = match[2];
    if (!id || !slug || seen.has(id)) continue;
    seen.add(id);
    out.push({
      webshopId: id,
      title: slug.replace(/-/g, " ").trim(),
      salesUnitSize: null,
      per100g: {},
    });
  }
  return out;
}

/**
 * Recognises anything in a payload that looks like a recipe and normalises it.
 *
 * Eén receptpagina bevat hetzelfde recept twee keer, en elk van beide weet iets
 * dat de ander niet weet. De paginastate (`self.__next_f`) heeft de rijke
 * ingredienten: naam als {singular, plural}, de eenheid apart, de productlink.
 * De `ld+json` daarentegen is de enige plek waar AH's eigen voedingswaarde per
 * portie staat. Ze worden daarom samengevoegd in plaats van dat de tweede
 * weggegooid wordt — dat laatste kostte precies die voedingswaarde, en daarmee
 * de enige cijfers die een recept met courgette of limoen nog konden redden.
 *
 * Samenvoegen gaat op het genormaliseerde id: de paginastate schrijft `1202636`
 * waar de ld+json `R-R1202636` schrijft, dus op de ruwe waarde zouden het twee
 * verschillende recepten lijken.
 */
export function collectRecipes(root: unknown): Recipe[] {
  const out: Recipe[] = [];
  const byId = new Map<string, Recipe>();

  const visit = (v: unknown): void => {
    if (Array.isArray(v)) {
      v.forEach(visit);
      return;
    }
    if (!isRecord(v)) return;

    const url = str(v["href"]) ?? str(v["url"]);
    const rawId =
      str(v["id"]) ??
      (typeof v["id"] === "number" ? String(v["id"]) : null) ??
      str(v["@id"]);
    const id = rawId ?? recipeIdFromUrl(url);
    const title = str(v["title"]) ?? str(v["name"]);
    const isSchemaRecipe = v["@type"] === "Recipe";
    const looksLikeRecipe =
      id !== null &&
      title !== null &&
      (isSchemaRecipe || "ingredients" in v || "servings" in v || "href" in v);

    if (looksLikeRecipe) {
      const recipeId = id.startsWith("R-R") ? id : `R-R${id}`;
      const found: Recipe = {
        id: recipeId,
        title,
        url: absoluteAhUrl(url) ?? `${RECIPE_BASE}/recept/${recipeId}`,
        servings: servingsOf(v),
        imageUrl: findImageUrl(v),
        ingredients: parseIngredients(v["ingredients"] ?? v["recipeIngredient"]),
        keywords: parseKeywords(v["keywords"] ?? v["tags"] ?? v["recipeCategory"], v["classifications"]),
        nutritionPerServing: parseNutritionLd(v["nutrition"]),
      };
      const existing = byId.get(recipeId);
      if (existing) mergeRecipe(existing, found);
      else {
        byId.set(recipeId, found);
        out.push(found);
      }
    }
    Object.values(v).forEach(visit);
  };

  visit(root);
  return out;
}

/**
 * Vult in `into` aan wat er nog niet stond. Wie het eerst kwam houdt gelijk: die
 * komt uit de paginastate en is rijker. Alleen een leeg veld wordt overgenomen,
 * zodat een schrale kaart nooit een volledig recept kan uitkleden.
 */
function mergeRecipe(into: Recipe, from: Recipe): void {
  if (into.ingredients.length === 0 && from.ingredients.length > 0) {
    into.ingredients = from.ingredients;
  }
  if (!into.nutritionPerServing && from.nutritionPerServing) {
    into.nutritionPerServing = from.nutritionPerServing;
  }
  if ((into.keywords?.length ?? 0) === 0 && (from.keywords?.length ?? 0) > 0) {
    into.keywords = from.keywords;
  }
  if (!into.imageUrl && from.imageUrl) into.imageUrl = from.imageUrl;
}

/**
 * Het aantal porties waar de ingredienthoeveelheden bij horen. De paginastate
 * zet dat in `serving.number` ("voor hoeveel personen"), de ld+json in
 * `recipeYield`. Levert geen van beide een bruikbaar getal, dan is 4 de aanname
 * die AH zelf ook het vaakst hanteert.
 */
function servingsOf(v: Record<string, unknown>): number {
  const serving = v["serving"];
  const candidates = [
    num(v["servings"]),
    isRecord(serving) ? num(serving["number"]) : null,
    num(v["recipeYield"]),
    num(deepFind(v["servings"], (x) => typeof x === "number")),
  ];
  for (const candidate of candidates) {
    if (candidate !== null && candidate > 0) return candidate;
  }
  return 4;
}

/** Parses the server-rendered recipe cards used by AH's current App Router pages. */
export function parseRecipeCards(html: string): Recipe[] {
  const out: Recipe[] = [];
  const seen = new Set<string>();
  for (const match of html.matchAll(/<a\b([^>]*\bdata-testid="recipe-card"[^>]*)>/gi)) {
    const attrs = match[1] ?? "";
    const href = attrs.match(/\bhref="([^"]+)"/i)?.[1];
    const titleAttr = attrs.match(/\btitle="([^"]+)"/i)?.[1];
    const id = recipeIdFromUrl(href ?? null);
    if (!href || !titleAttr || !id || seen.has(id)) continue;
    seen.add(id);
    out.push({
      id,
      title: decodeHtml(titleAttr.replace(/^Recept:\s*/i, "")).trim(),
      url: absoluteAhUrl(href) ?? `${RECIPE_BASE}/recept/${id}`,
      servings: 4,
      imageUrl: null,
      ingredients: [],
    });
  }
  return out;
}

/**
 * Maakt er een link van die buiten ah.nl ook werkt.
 *
 * De paginastate schrijft `href` als pad ("/allerhande/recept/R-R1202683/..."),
 * en zo'n pad in de UI zetten laat de browser hem oplossen tegen ónze eigen
 * worker-URL: dan opent "Bereiding op ah.nl" een 404 op workers.dev. Dat is
 * precies wat er gebeurde.
 */
export function absoluteAhUrl(url: string | null): string | null {
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  return url.startsWith("/") ? `${SITE_BASE}${url}` : null;
}

function recipeIdFromUrl(url: string | null): string | null {
  const match = url?.match(/\/recept\/(R-R\d+)/i);
  return match?.[1]?.toUpperCase() ?? null;
}

function decodeHtml(value: string): string {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_, hex: string) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, decimal: string) => String.fromCodePoint(Number(decimal)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;|&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function findImageUrl(v: Record<string, unknown>): string | null {
  const img = deepFind(
    v["images"] ?? v["image"],
    (x) => typeof x === "string" && /^https?:\/\/.*\.(jpg|jpeg|png|webp)/i.test(x),
  );
  return typeof img === "string" ? img : null;
}

/**
 * AH's eigen labels. Het `keywords`-veld is één string met komma's:
 * "gezond, vooraf te maken, brood/sandwiches, tussendoortje, grillen".
 * Schuine strepen scheiden varianten van hetzelfde label, dus die splitsen we ook.
 */
export function parseKeywords(v: unknown, extra?: unknown): string[] {
  const out = new Set<string>();

  const add = (value: unknown): void => {
    if (typeof value !== "string") return;
    for (const part of value.split("/")) {
      const clean = part.trim().toLowerCase();
      if (clean) out.add(clean);
    }
  };

  for (const source of [v, extra]) {
    if (typeof source === "string") {
      source.split(",").forEach(add);
      continue;
    }
    if (!Array.isArray(source)) continue;
    for (const item of source) {
      // De paginastate schrijft ze als {key: "menugang", value: "borrelhapje"};
      // daar is de waarde het label. De ld+json geeft gewoon strings.
      if (isRecord(item)) add(item["value"]);
      else add(item);
    }
  }
  return [...out];
}

/**
 * De voedingswaarde die AH zelf op de receptpagina zet, per portie. De waarden
 * staan er als tekst met eenheid en toelichting ("135 kcal energie", "8 g vet"),
 * vandaar dat `num()` het getal eruit vist.
 *
 * Alleen `kcal` wordt als verplicht gezien: zonder calorieën is het blok
 * onbruikbaar voor de planner en kun je beter terugvallen op de producten.
 */
export function parseNutritionLd(v: unknown): Nutrients | null {
  if (!isRecord(v)) return null;
  const out: Nutrients = {};
  const fields: [string, keyof Nutrients][] = [
    ["calories", "kcal"],
    ["proteinContent", "protein"],
    ["carbohydrateContent", "carbs"],
    ["fatContent", "fat"],
    ["fiberContent", "fiber"],
  ];
  for (const [field, key] of fields) {
    const value = num(v[field]);
    if (value !== null) out[key] = value;
  }
  return out.kcal === undefined ? null : out;
}

export function parseIngredients(v: unknown): RawIngredient[] {
  if (!Array.isArray(v)) return [];
  const out: RawIngredient[] = [];
  for (const item of v) {
    if (typeof item === "string") {
      out.push(parseIngredientText(item));
      continue;
    }
    if (!isRecord(item)) continue;
    // AH schrijft de naam als {singular, plural}: het enkelvoud is wat je in een
    // productzoekopdracht wilt, en `deepFind` zou net zo goed "__typename"
    // kunnen teruggeven, dus dat veld eerst met naam en toenaam.
    const nameField = item["name"];
    const name =
      str(nameField) ??
      (isRecord(nameField) ? str(nameField["singular"]) ?? str(nameField["plural"]) : null) ??
      str(item["description"]) ??
      str(deepFind(nameField, (x) => typeof x === "string"));
    if (!name) continue;

    const q = item["quantity"] ?? item["amount"];
    const quantity = num(isRecord(q) ? (q["amount"] ?? q["value"]) : q);
    const unit = unitOf(item);
    // "2 el milde olijfolie" staat er ook als kant-en-klare regel. Ontbreekt de
    // hoeveelheid of de eenheid in de losse velden, dan is die regel de bron:
    // zonder eenheid werd van "2 el olijfolie" een naamloze 2 en viel de
    // omrekening naar grammen terug op een schatting.
    if (quantity === null || unit === null) {
      const text = str(item["text"]);
      if (text) {
        const parsed = parseIngredientText(text);
        out.push({
          name: name.toLowerCase(),
          quantity: quantity ?? parsed.quantity,
          unit: unit ?? parsed.unit,
          productId: productIdFrom(item),
        });
        continue;
      }
    }

    out.push({
      name: name.toLowerCase(),
      quantity,
      unit,
      productId: productIdFrom(item),
    });
  }
  return out;
}

/**
 * AH hangt aan een receptregel zijn eigen webshopproduct — dat is wat de
 * "bestel de ingrediënten"-knop gebruikt. Het veld heet niet overal hetzelfde,
 * dus we kijken naar de plekken waar het door de jaren heen gestaan heeft, en
 * accepteren alleen een numeriek webshop-id (dat is wat de detail-API wil).
 */
export function productIdFrom(item: Record<string, unknown>): string | null {
  const direct =
    item["productId"] ??
    item["webshopId"] ??
    item["shoppableProductId"] ??
    item["defaultProductId"];
  const nested = isRecord(item["product"])
    ? (item["product"]["webshopId"] ?? item["product"]["id"])
    : undefined;
  const first = Array.isArray(item["products"]) && isRecord(item["products"][0])
    ? ((item["products"][0] as Record<string, unknown>)["webshopId"] ??
       (item["products"][0] as Record<string, unknown>)["id"])
    : undefined;

  for (const candidate of [direct, nested, first]) {
    if (typeof candidate === "number" && Number.isInteger(candidate) && candidate > 0) {
      return String(candidate);
    }
    if (typeof candidate === "string" && /^\d+$/.test(candidate.trim())) return candidate.trim();
  }
  return null;
}

/**
 * De eenheid van een receptregel. AH noemt het veld `quantityUnit` en schrijft
 * het als {singular, plural} ("el"/"el", "stuk"/"stuks"); oudere payloads
 * gebruikten `unit`, als string of als object met `name`.
 */
function unitOf(item: Record<string, unknown>): string | null {
  const raw = item["quantityUnit"] ?? item["unit"];
  if (!raw) return null;
  if (typeof raw === "string") return str(raw);
  if (!isRecord(raw)) return null;
  return str(raw["singular"]) ?? str(raw["name"]) ?? str(raw["plural"]);
}

/**
 * Parses a free-text line like "250 g kipfilet" or "2 el olijfolie". The word right
 * after the quantity is only taken as a unit when it actually is one — otherwise
 * ("3 rijpe bananen") it's an adjective and belongs in the name, with no unit.
 */
export function parseIngredientText(text: string): RawIngredient {
  const m = text.trim().toLowerCase().match(/^(\d+(?:[.,]\d+)?)\s*(.*)$/);
  if (!m || !m[1] || !m[2]) return { name: text.trim().toLowerCase(), quantity: null, unit: null };

  const quantity = Number(m[1].replace(",", "."));
  const rest = m[2].trim();
  const wordMatch = rest.match(/^([a-z]+\.?)\s+(.+)$/);
  if (wordMatch && wordMatch[1] && wordMatch[2] && isKnownUnit(wordMatch[1])) {
    return { quantity, unit: wordMatch[1], name: wordMatch[2].trim() };
  }
  return { quantity, unit: null, name: rest };
}
