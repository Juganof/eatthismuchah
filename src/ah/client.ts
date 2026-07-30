import type { Nutrients, Product, RawIngredient, Recipe } from "./types";
import { deepFind, extractEmbeddedJson } from "./scrape";

const AUTH_URL = "https://api.ah.nl/mobile-auth/v1/auth/token/anonymous";
const PRODUCT_SEARCH_URL = "https://api.ah.nl/mobile-services/product/search/v2";
const PRODUCT_DETAIL_URL = "https://api.ah.nl/mobile-services/product/detail/v4/fir";
const PRODUCT_PAGE_URL = "https://www.ah.nl/producten/product/wi";
const RECIPE_BASE = "https://www.ah.nl/allerhande";

/**
 * AH exposes no public API, so every endpoint here is the one its own apps use and
 * can change without notice. Each call therefore degrades to HTML scraping rather
 * than throwing, and `probe()` reports which paths are currently alive.
 */
export class AhClient {
  private token: string | null = null;

  constructor(private readonly userAgent: string) {}

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
  private async apiGet(url: string): Promise<unknown> {
    for (let attempt = 0; attempt < 2; attempt++) {
      const res = await fetch(url, { headers: await this.authHeaders() });
      if (res.status === 401 && attempt === 0) {
        this.token = null;
        continue;
      }
      if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
      return await res.json();
    }
    throw new Error(`GET ${url} failed after re-auth`);
  }

  private async htmlGet(url: string): Promise<string> {
    const res = await fetch(url, {
      headers: {
        "User-Agent": this.userAgent,
        Accept: "text/html,application/xhtml+xml",
        "Accept-Language": "nl-NL,nl;q=0.9",
      },
    });
    if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
    return await res.text();
  }

  // ---------------------------------------------------------------- products

  async searchProducts(query: string, size = 10): Promise<Product[]> {
    // Ask for a few extras because AH places virtual multi-packs first for some
    // searches; those cannot supply a meaningful per-100g nutrition record.
    const upstreamSize = size + 10;
    const url = `${PRODUCT_SEARCH_URL}?query=${encodeURIComponent(query)}&size=${upstreamSize}`;
    const body = (await this.apiGet(url)) as { products?: unknown[] };
    const products = Array.isArray(body.products) ? body.products : [];
    return products
      .map((p) => toProductStub(p))
      .filter((p): p is Product => p !== null)
      .slice(0, size);
  }

  /** Full product record including nutrition, which search results omit. */
  async getProduct(webshopId: string): Promise<Product | null> {
    const body = await this.apiGet(`${PRODUCT_DETAIL_URL}/${encodeURIComponent(webshopId)}`);
    const card = deepFind(body, (v) => isRecord(v) && "webshopId" in v && "title" in v);
    const stub = toProductStub(card ?? body);
    if (!stub) return null;
    let per100g = parseNutrition(body);
    // The v4 mobile detail response stopped including nutrition in 2026. The
    // server-rendered product page still contains a labelled per-100g table.
    if (per100g.kcal === undefined || per100g.protein === undefined) {
      const html = await this.htmlGet(`${PRODUCT_PAGE_URL}${encodeURIComponent(webshopId)}`);
      per100g = parseNutritionHtml(html);
    }
    return { ...stub, webshopId, per100g };
  }

  // ----------------------------------------------------------------- recipes

  /**
   * Allerhande search. The JSON service is tried first; if AH has moved it we fall
   * back to pulling the embedded Next.js state out of the search page HTML.
   */
  async searchRecipes(query: string, size = 20): Promise<Recipe[]> {
    const jsonUrl = `${RECIPE_BASE}/service/search/recipes?searchTerm=${encodeURIComponent(query)}&size=${size}`;
    try {
      const body = await this.apiGet(jsonUrl);
      const found = collectRecipes(body);
      if (found.length > 0) return found.slice(0, size);
    } catch {
      // fall through to HTML
    }
    const html = await this.htmlGet(
      `${RECIPE_BASE}/recepten-zoeken?query=${encodeURIComponent(query)}`,
    );
    const embedded = collectRecipes(extractEmbeddedJson(html));
    return (embedded.length > 0 ? embedded : parseRecipeCards(html)).slice(0, size);
  }

  /** Recipe detail. Ingredient lists only ever appear in the embedded page state. */
  async getRecipe(id: string): Promise<Recipe | null> {
    const html = await this.htmlGet(`${RECIPE_BASE}/recept/${encodeURIComponent(id)}`);
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

/** Recognises anything in a payload that looks like a recipe and normalises it. */
export function collectRecipes(root: unknown): Recipe[] {
  const out: Recipe[] = [];
  const seenIds = new Set<string>();

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

    if (looksLikeRecipe && !seenIds.has(id)) {
      const recipeId = id.startsWith("R-R") ? id : `R-R${id}`;
      seenIds.add(id);
      out.push({
        id: recipeId,
        title,
        url: url ?? `${RECIPE_BASE}/recept/${recipeId}`,
        servings:
          num(v["servings"]) ??
          num(v["recipeYield"]) ??
          num(deepFind(v["servings"], () => true)) ??
          4,
        imageUrl: findImageUrl(v),
        ingredients: parseIngredients(v["ingredients"] ?? v["recipeIngredient"]),
      });
    }
    Object.values(v).forEach(visit);
  };

  visit(root);
  return out;
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
      url: href.startsWith("http") ? href : `https://www.ah.nl${href}`,
      servings: 4,
      imageUrl: null,
      ingredients: [],
    });
  }
  return out;
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

export function parseIngredients(v: unknown): RawIngredient[] {
  if (!Array.isArray(v)) return [];
  const out: RawIngredient[] = [];
  for (const item of v) {
    if (typeof item === "string") {
      out.push(parseIngredientText(item));
      continue;
    }
    if (!isRecord(item)) continue;
    const name =
      str(item["name"]) ??
      str(item["description"]) ??
      str(deepFind(item["name"], (x) => typeof x === "string"));
    if (!name) continue;
    const q = item["quantity"] ?? item["amount"];
    out.push({
      name: name.toLowerCase(),
      quantity: num(isRecord(q) ? (q["amount"] ?? q["value"]) : q),
      unit: str(isRecord(item["unit"]) ? item["unit"]["name"] : item["unit"]),
    });
  }
  return out;
}

/** Parses a free-text line like "250 g kipfilet" or "2 el olijfolie". */
export function parseIngredientText(text: string): RawIngredient {
  const m = text
    .trim()
    .toLowerCase()
    .match(/^(\d+(?:[.,]\d+)?)\s*([a-z]+\.?)?\s+(.*)$/);
  if (!m || !m[1] || !m[3]) return { name: text.trim().toLowerCase(), quantity: null, unit: null };
  return {
    quantity: Number(m[1].replace(",", ".")),
    unit: m[2] ?? null,
    name: m[3].trim(),
  };
}
