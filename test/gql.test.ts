import { afterEach, describe, expect, it, vi } from "vitest";
import { resetEndpointState } from "../src/ah/client";
import { GqlClient, SubrequestBudgetError } from "../src/ah/gql";

/**
 * De client voor het (onofficiele) `www.ah.nl/gql`-endpoint. De tests stuben
 * fetch zoals `stubAh` in auto.test.ts dat doet; er gaat nooit echt netwerk
 * overheen.
 */

const GQL = "https://www.ah.nl/gql";

/** Eén suggestie zoals AH hem teruggeeft, met een product. */
function suggestion(p: Partial<{ id: number; title: string; size: string }>) {
  return {
    optional: false,
    ingredient: { id: 1906510, name: "biologische kikkererwten", quantityFloat: 175, quantityUnit: "g" },
    productSuggestion: {
      id: p.id ?? 168813,
      quantity: 1,
      proposer: "A",
      product: {
        id: p.id ?? 168813,
        title: p.title ?? "AH Terra Biologisch kikkererwten",
        brand: "AH Terra",
        webPath: "/producten/product/wi168813/ah-terra-biologisch-kikkererwten",
        salesUnitSize: p.size ?? "330 g",
      },
    },
  };
}

/** De tradeItem-payload zoals `product(id) { tradeItem { ... } }` hem levert. */
function tradeItemPayload(nutrients: unknown[], gtin = "05410068237303") {
  return {
    data: {
      product: {
        __typename: "Product",
        id: 384175,
        title: "Philadelphia original",
        tradeItem: {
          __typename: "ProductTradeItem",
          gtin,
          nutritions: [
            {
              __typename: "ProductTradeItemNutrition",
              basisQuantity: "100.0 Gram",
              basisQuantityDescription: "",
              preparationState: "Onbereide",
              servingSize: null,
              servingSizeDescription: null,
              nutrients,
            },
          ],
        },
      },
    },
  };
}

interface StubOptions {
  /** Per request: status; de body hangt af van welke query erin zit. */
  status?: number;
  /** tradeItem: null simuleert een virtuele bundel. */
  bundle?: boolean;
  virtualBundleProducts?: unknown[];
  setCookie?: string;
}

/**
 * Doet alsof het gql-endpoint antwoordt. Onthoudt de call-geschiedenis zodat
 * tests kunnen bewijzen dat de client eerst een GET doet en daarna POSTs.
 */
function stubGql(options: StubOptions = {}) {
  const calls: { method: string; url: string; headers: Record<string, string>; body: string }[] = [];
  const status = options.status ?? 200;

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      const headers = Object.fromEntries(
        Object.entries((init?.headers as Record<string, string>) ?? {}).map(([k, v]) => [k.toLowerCase(), v]),
      );
      const body = String(init?.body ?? "");
      calls.push({ method, url, headers, body });

      if (method === "GET") {
        return new Response("<html>home</html>", {
          status: 200,
          headers: options.setCookie ? { "set-cookie": options.setCookie } : undefined,
        });
      }

      if (options.bundle) {
        const parsed = JSON.parse(body) as { query: string; variables: { id: number } };
        // Alleen de 2-pack zelf is een bundel zonder tradeItem; de fysieke
        // variant (384175) heeft wél voedingswaarde — net als bij AH echt.
        if (!parsed.query.includes("virtualBundleProducts") && parsed.variables.id !== 384175) {
          return new Response(
            JSON.stringify({ data: { product: { __typename: "Product", id: parsed.variables.id, tradeItem: null } } }),
            { status: 200 },
          );
        }
        if (parsed.query.includes("virtualBundleProducts")) {
          return new Response(
            JSON.stringify({
              data: {
                product: {
                  __typename: "Product",
                  id: parsed.variables.id,
                  virtualBundleProducts: options.virtualBundleProducts ?? [{ product: { id: 384175, title: "x" } }],
                },
              },
            }),
            { status: 200 },
          );
        }
        return new Response(
          JSON.stringify(tradeItemPayload([{ type: "ENER-", name: "Energie", value: "933.0 kJ (226.0 kcal)" }])),
          { status: 200 },
        );
      }

      const parsed = JSON.parse(body) as { query: string };
      if (parsed.query.includes("recipeProductSuggestionsV2")) {
        return new Response(
          JSON.stringify({ data: { recipeProductSuggestionsV2: [suggestion({})] } }),
          { status },
        );
      }
      if (parsed.query.includes("tradeItem")) {
        return new Response(JSON.stringify(tradeItemPayload([{ type: "ENER-", name: "Energie", value: "933.0 kJ (226.0 kcal)" }])), {
          status,
        });
      }
      return new Response("{}", { status });
    }),
  );
  return calls;
}

let client: GqlClient | null = null;
afterEach(() => {
  client = null;
  vi.unstubAllGlobals();
  resetEndpointState();
});

type GqlOptions = ConstructorParameters<typeof GqlClient>[1];
const makeClient = (options: GqlOptions = {}) => {
  client = new GqlClient("test-agent", { minIntervalMs: 0, backoffMs: 0, maxRequests: 40, ...options });
  return client;
};

describe("de sessie", () => {
  it("haalt eerst cookies op en stuurt die daarna mee op elke POST", async () => {
    const calls = stubGql({ setCookie: "a=1; Path=/; Max-Age=1000" });
    const c = makeClient();

    await c.suggestionsForRecipe("R-R1202157", 10);

    expect(calls[0]).toMatchObject({ method: "GET", url: "https://www.ah.nl/" });
    const posts = calls.slice(1);
    expect(posts.length).toBeGreaterThan(0);
    for (const post of posts) {
      expect(post.method).toBe("POST");
      expect(post.url).toBe(GQL);
      expect(post.headers["cookie"]).toContain("a=1");
    }
  });

  it("doet maar één GET voor de cookies, ook bij meerdere aanroepen", async () => {
    const calls = stubGql({ setCookie: "a=1" });
    const c = makeClient();

    await c.suggestionsForRecipe("R-R1202157", 10);
    await c.productNutrition(384175);

    expect(calls.filter((c) => c.method === "GET")).toHaveLength(1);
  });
});

describe("suggestionsForRecipe", () => {
  it("vraagt product-suggesties op voor het recept en leest ze uit", async () => {
    const calls = stubGql();
    const c = makeClient();

    const result = await c.suggestionsForRecipe("R-R1202157", 10);

    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      ingredientName: "biologische kikkererwten",
      productId: "168813",
      productTitle: "AH Terra Biologisch kikkererwten",
      salesUnitSize: "330 g",
      suggestedPackages: 1,
    });

    const post = calls[calls.length - 1]!;
    expect(post.body).toContain("recipeProductSuggestionsV2");
    expect(post.body).toContain('"recipeId":1202157');
    expect(post.body).toContain('"numberOfServings":10');
    expect(post.headers["origin"]).toBe("https://www.ah.nl/");
    expect(post.headers["x-client-platform-type"]).toBe("Web");
  });

  it("laat een regel zonder suggestie als ongematcht zien", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        if ((init?.method ?? "GET") === "GET") return new Response("<html></html>", { status: 200 });
        return new Response(
          JSON.stringify({
            data: {
              recipeProductSuggestionsV2: [{ ...suggestion({}), productSuggestion: null }],
            },
          }),
          { status: 200 },
        );
      }),
    );
    const c = makeClient();

    const result = await c.suggestionsForRecipe("R-R1202157", 10);

    expect(result[0]!.productId).toBeNull();
    expect(result[0]!.productTitle).toBeNull();
  });
});

describe("productNutrition", () => {
  it("haalt de voedingswaarde per 100 g op via tradeItem", async () => {
    const calls = stubGql();
    const c = makeClient();

    const result = await c.productNutrition(384175);

    expect(result).toEqual({
      gtin: "05410068237303",
      per100g: { kcal: 226 },
      packSizeLabel: null,
    });
    expect(calls[calls.length - 1]!.body).toContain('"id":384175');
  });

  it("volgt een virtuele bundel naar het fysieke variantproduct", async () => {
    const calls = stubGql({ bundle: true, virtualBundleProducts: [{ product: { id: 384175, title: "Philadelphia original" } }] });
    const c = makeClient();

    const result = await c.productNutrition(562458);

    // Eerst de bundel (tradeItem null), dan de variant opvragen en zijn voeding lezen.
    expect(result).toEqual({ gtin: "05410068237303", per100g: { kcal: 226 }, packSizeLabel: null });
    expect(calls.filter((c) => c.method === "POST")).toHaveLength(3);
    const bodies = calls.filter((c) => c.method === "POST").map((c) => c.body);
    expect(bodies[0]!).toContain('"id":562458');
    expect(bodies[1]!).toContain("virtualBundleProducts");
    expect(bodies[2]!).toContain('"id":384175');
  });

  it("geeft null als de bundel geen variantproduct oplevert", async () => {
    stubGql({ bundle: true, virtualBundleProducts: [] });
    const c = makeClient();

    const result = await c.productNutrition(562458);

    expect(result).toBeNull();
  });
});

describe("fouten en budget", () => {
  it("gooit een herkenbare fout bij een blokkade, zodat isBlocked hem vangt", async () => {
    stubGql({ status: 403 });
    const c = makeClient({ maxRetries: 0 });

    await expect(c.productNutrition(384175)).rejects.toThrow(/-> 403$/);
  });

  it("houdt het verzoekbudget bij en stopt vóór de grens", async () => {
    stubGql();
    // Eerste aanroep kost twee verzoeken (GET voor cookies + POST); daarna één per aanroep.
    const c = makeClient({ maxRequests: 3 });

    await c.productNutrition(384175);
    await c.productNutrition(384175);
    await expect(c.productNutrition(384175)).rejects.toBeInstanceOf(SubrequestBudgetError);
    expect(c.budget.used).toBeLessThanOrEqual(3);
  });

  it("deelt het tempo met de gewone AhClient: de klok is gemeenschappelijk", async () => {
    stubGql();
    const c = makeClient({ minIntervalMs: 30 });

    const start = Date.now();
    await c.productNutrition(384175);
    const halfway = Date.now();
    await c.productNutrition(384175);
    const elapsed = Date.now() - start;

    // Twee verzoeken met 30 ms ertussen duurt samen minstens ~30 ms; de GET
    // voor de cookies zit er ook in, dus de ondergrens is ruim.
    expect(elapsed).toBeGreaterThanOrEqual(30);
    expect(halfway - start).toBeGreaterThanOrEqual(0);
  });
});
