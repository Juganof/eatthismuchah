import { SubrequestBudgetError } from "./client";
import { parseTradeItem, type TradeItemNutrition } from "./gql-nutrition";
import { sharedPace } from "./pace";

export { SubrequestBudgetError };

/**
 * De GraphQL-route naar AH's eigen productgegevens.
 *
 * De webshop laadt zijn voedingswaarden client-side op uit `https://www.ah.nl/gql`
 * (Apollo; het endpoint is anoniem, introspectie staat uit). Twee queries zijn
 * hier van belang:
 *
 *   - `recipeProductSuggestionsV2` — dezelfde data als de knop "bestel de
 *     ingrediënten": per recept-regel het webshopproduct dat AH zelf voorstelt.
 *   - `product(id) { tradeItem { nutritions } }` — de voedingswaarde per 100 g.
 *     Virtuele bundels (2-packs e.d.) hebben geen tradeItem; hun fysieke
 *     variantproduct staat in `virtualBundleProducts`.
 *   - `productSearch` ("recipeVagueSuggestionsSearch") — de zoekquery achter
 *     de zoekbalk: een losse ingrediëntnaam opzoeken wanneer de suggestie-
 *     query voor een regel geen product voorstelt.
 *
 * Het endpoint is onofficieel en kan zonder aankondiging veranderen. Deze
 * client gooit daarom fouten in hetzelfde formaat als `AhClient` (`-> 403`),
 * zodat de bestaande blokkade-herkenning ze vangt.
 */

export const GQL_URL = "https://www.ah.nl/gql";
export const HOME_URL = "https://www.ah.nl/";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Eén product-suggestie voor een recept-regel, zoals AH hem voorstelt. */
export interface ProductSuggestion {
  /** De regel zoals AH hem noemt, bijv. "biologische kikkererwten". */
  ingredientName: string;
  /** Webshop-id van het voorgestelde product; null als AH niets voorstelt. */
  productId: string | null;
  productTitle: string | null;
  salesUnitSize: string | null;
  /**
   * Hoeveel verpakkingen AH voor deze regel voorstelt. Geldt voor het recept
   * zoals geschreven, niet voor geschaalde plannen — zie de boodschappenlijst.
   */
  suggestedPackages: number | null;
}

export const NUTRITION_QUERY = `query P($id: Int!) {
  product(id: $id) {
    __typename id title
    tradeItem { __typename gtin nutritions {
      __typename basisQuantity basisQuantityDescription preparationState servingSize servingSizeDescription
      nutrients { type name value }
    } }
  }
}`;

export const BUNDLE_QUERY = `query P($id: Int!) {
  product(id: $id) { __typename id title virtualBundleProducts { product { id title } } }
}`;

export const SUGGESTIONS_QUERY = `query recipeProductSuggestions($options: RecipeProductSuggestionV2Input!) {
  recipeProductSuggestionsV2(options: $options) {
    optional
    ingredient { id name quantityFloat quantityUnit }
    productSuggestion { id quantity proposer
      product { id title brand webPath salesUnitSize }
    }
  }
}`;

export const SEARCH_QUERY = `query recipeVagueSuggestionsSearch($input: ProductSearchInput!) {
  productSearch(input: $input) {
    products { id title brand webPath salesUnitSize }
  }
}`;

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function numOf(v: unknown): number | null {
  if (typeof v === "number" && Number.isInteger(v) && v > 0) return v;
  if (typeof v === "string") {
    const m = v.replace(/^R-R/, "").match(/^\d+$/);
    if (m) return Number(m[0]);
  }
  return null;
}

function str(v: unknown): string | null {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

export class GqlClient {
  private readonly minIntervalMs: number;
  private readonly maxRetries: number;
  private readonly backoffMs: number;
  private readonly maxRequests: number;
  /** Verzoeken die deze client gedaan heeft; de teller achter het budget. */
  private requests = 0;
  /** Cookie-waarde uit de eerste GET; zonder sessie weigert het endpoint 403. */
  private cookie: string | null = null;

  constructor(
    private readonly userAgent: string,
    options: {
      minIntervalMs?: number;
      maxRetries?: number;
      backoffMs?: number;
      maxRequests?: number;
    } = {},
  ) {
    this.minIntervalMs = options.minIntervalMs ?? 700;
    this.maxRetries = options.maxRetries ?? 1;
    this.backoffMs = options.backoffMs ?? 1500;
    this.maxRequests = options.maxRequests ?? 40;
  }

  get budget(): { used: number; max: number } {
    return { used: this.requests, max: this.maxRequests };
  }

  /**
   * Eén GET op de webshop om de sessie te openen. Het gql-endpoint weigert
   * anders met 403; een rustige homepage-pagina is daarvoor genoeg.
   */
  private async ensureSession(): Promise<void> {
    if (this.cookie !== null) return;
    await this.pace();
    const res = await fetch(HOME_URL, {
      headers: {
        "User-Agent": this.userAgent,
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "nl-NL,nl;q=0.9",
      },
    });
    if (!res.ok) throw new Error(`GET ${HOME_URL} -> ${res.status}`);
    const cookies = typeof res.headers.getSetCookie === "function" ? res.headers.getSetCookie() : [];
    const parts = cookies.length > 0 ? cookies : [res.headers.get("set-cookie") ?? ""];
    const values = parts
      .map((c) => c.split(";")[0]!.trim())
      .filter((c) => c !== "" && !c.toLowerCase().startsWith("path="));
    this.cookie = values.length > 0 ? values.join("; ") : "";
  }

  private async pace(): Promise<void> {
    if (this.requests >= this.maxRequests) throw new SubrequestBudgetError(this.requests);
    this.requests++;
    await sharedPace(this.minIntervalMs);
  }

  /** Eén POST op /gql, met herkansing tegen tempo-blokkades. */
  private async gql(query: string, variables: Record<string, unknown>, referer: string): Promise<unknown> {
    for (let attempt = 0; attempt < this.maxRetries + 1; attempt++) {
      await this.pace();
      const res = await fetch(GQL_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json;charset=UTF-8",
          Accept: "application/json",
          "Accept-Encoding": "gzip, deflate, br, zstd",
          "Accept-Language": "nl-NL,nl;q=0.9",
          "User-Agent": this.userAgent,
          Origin: "https://www.ah.nl/",
          Referer: referer,
          "x-client-platform-type": "Web",
          "x-client-name": "ah-allerhande",
          "x-client-version": "3.23.124",
          "x-correlation-id": `ssr-gql-${Math.random().toString(36).slice(2, 18)}`,
          ...(this.cookie ? { Cookie: this.cookie } : {}),
        },
        body: JSON.stringify({ query, variables }),
      });
      if (res.ok) return res.json();
      if (res.status !== 403 && res.status !== 429 && res.status < 500) {
        throw new Error(`POST ${GQL_URL} -> ${res.status}`);
      }
      if (attempt === this.maxRetries) throw new Error(`POST ${GQL_URL} -> ${res.status}`);
      await sleep(this.backoffMs * 2 ** attempt);
    }
    throw new Error(`POST ${GQL_URL} failed`);
  }

  /**
   * De product-suggesties voor één recept, zoals de "bestel de ingrediënten"-
   * knop ze toont. `recipeId` is het recept-id ("R-R1202157") of het nummer.
   */
  async suggestionsForRecipe(recipeId: string | number, servings: number): Promise<ProductSuggestion[]> {
    const id = numOf(recipeId);
    if (id === null) return [];
    await this.ensureSession();
    const body = (await this.gql(
      SUGGESTIONS_QUERY,
      {
        options: {
          recipeId: id,
          numberOfServings: servings > 0 ? servings : 4,
          productIdOverride: [],
          ingredientsToOverride: [],
        },
      },
      `https://www.ah.nl/allerhande/recept/R-R${id}`,
    )) as { data?: { recipeProductSuggestionsV2?: unknown[] } };

    const rows = body?.data?.recipeProductSuggestionsV2 ?? [];
    const out: ProductSuggestion[] = [];
    for (const row of rows) {
      if (!isRecord(row)) continue;
      const ingredient = isRecord(row["ingredient"]) ? row["ingredient"] : null;
      const suggestion = isRecord(row["productSuggestion"]) ? row["productSuggestion"] : null;
      const product = suggestion && isRecord(suggestion["product"]) ? suggestion["product"] : null;
      const productId =
        product !== null && isRecord(product)
          ? (String(numOf(product["id"] ?? suggestion?.["id"]) ?? "") || null)
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
   * De voedingswaarde per 100 g van één product. Heeft een virtuele bundel geen
   * tradeItem, dan volgen we de eerste variant uit `virtualBundleProducts`.
   */
  async productNutrition(productId: number | string): Promise<TradeItemNutrition | null> {
    const id = numOf(productId);
    if (id === null) return null;
    await this.ensureSession();
    return this.productNutritionInner(id);
  }

  private async productNutritionInner(id: number, seen = new Set<number>()): Promise<TradeItemNutrition | null> {
    if (seen.has(id)) return null; // een variant die naar zichzelf wijst is geen variant
    seen.add(id);

    const body = (await this.gql(
      NUTRITION_QUERY,
      { id },
      "https://www.ah.nl/producten/product/wi" + id,
    )) as { data?: { product?: Record<string, unknown> } };
    const product = body?.data?.product;
    if (!isRecord(product)) return null;

    const parsed = parseTradeItem(product["tradeItem"]);
    if (parsed) return parsed;

    // Virtuele bundel: heeft geen eigen voedingswaarde. Het fysieke
    // variantproduct staat in virtualBundleProducts, dus dat vragen we apart op.
    const bundleBody = (await this.gql(
      BUNDLE_QUERY,
      { id },
      "https://www.ah.nl/producten/product/wi" + id,
    )) as { data?: { product?: Record<string, unknown> } };
    const bundle = bundleBody?.data?.product?.["virtualBundleProducts"];
    if (Array.isArray(bundle) && bundle.length > 0 && isRecord(bundle[0])) {
      const variant = bundle[0]["product"];
      if (isRecord(variant)) {
        const variantId = numOf(variant["id"]);
        if (variantId !== null) return this.productNutritionInner(variantId, seen);
      }
    }
    return null;
  }
}
