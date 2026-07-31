import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient, resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { calibrateToTotal, lookupIngredientMatch } from "../src/nutrition/resolve";
import type { ResolvedRecipe } from "../src/ah/types";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * `calibrateToTotal` is the fix for a recipe whose ingredients are individually
 * matched (so the solver *can* shift kwark up and banaan down) but whose summed
 * nutrition disagrees with AH's own, more trustworthy per-recipe figure — it
 * rescales every ingredient by the same per-macro factor so the sum lines up,
 * without flattening the shape between ingredients.
 */

const recipe: ResolvedRecipe["recipe"] = {
  id: "R-R1",
  title: "Kwark met banaan",
  url: "https://www.ah.nl/allerhande/recept/R-R1",
  servings: 1,
  imageUrl: null,
  ingredients: [],
};

function resolvedFixture(): ResolvedRecipe {
  return {
    recipe,
    ingredients: [
      {
        raw: { name: "kwark", quantity: 150, unit: "g" },
        grams: 150,
        product: { webshopId: "1", title: "AH Kwark", salesUnitSize: null, per100g: {} },
        gramsSource: "explicit",
        matchScore: 1,
        nutrients: { kcal: 90, protein: 15, carbs: 4, fat: 0.5 },
      },
      {
        raw: { name: "banaan", quantity: 1, unit: null },
        grams: 120,
        product: { webshopId: "2", title: "AH Bananen", salesUnitSize: null, per100g: {} },
        gramsSource: "piece",
        matchScore: 1,
        nutrients: { kcal: 100, protein: 1, carbs: 23, fat: 0.3 },
      },
    ],
    total: { kcal: 190, protein: 16, carbs: 27, fat: 0.8 },
  };
}

describe("calibrateToTotal", () => {
  it("rescales every ingredient so the sum matches the trusted total", () => {
    const calibrated = calibrateToTotal(resolvedFixture(), { kcal: 380, protein: 32, carbs: 54, fat: 1.6 });
    expect(calibrated.total.kcal).toBeCloseTo(380, 0);
    expect(calibrated.total.protein).toBeCloseTo(32, 0);
    const kwark = calibrated.ingredients.find((i) => i.raw.name === "kwark")!;
    expect(kwark.nutrients.kcal).toBeCloseTo(180, 0); // 90 * (380/190)
  });

  it("keeps the relative shape between ingredients intact", () => {
    const calibrated = calibrateToTotal(resolvedFixture(), { kcal: 380, protein: 32, carbs: 54, fat: 1.6 });
    const kwark = calibrated.ingredients.find((i) => i.raw.name === "kwark")!;
    const banaan = calibrated.ingredients.find((i) => i.raw.name === "banaan")!;
    // Kwark levert nog steeds veruit het meeste eiwit, ondanks de ijking.
    expect(kwark.nutrients.protein!).toBeGreaterThan(banaan.nutrients.protein!);
  });

  it("clamps an extreme mismatch instead of scaling unboundedly", () => {
    const calibrated = calibrateToTotal(resolvedFixture(), { kcal: 1900 }); // 10x
    const kwark = calibrated.ingredients.find((i) => i.raw.name === "kwark")!;
    // Factor is capped at 2x, not 10x: 90 * 2 = 180.
    expect(kwark.nutrients.kcal).toBeCloseTo(180, 0);
  });

  it("leaves a macro alone when either side is unknown", () => {
    const calibrated = calibrateToTotal(resolvedFixture(), { kcal: 380 });
    const kwark = calibrated.ingredients.find((i) => i.raw.name === "kwark")!;
    // Geen doelwaarde voor eiwit: factor blijft 1, dus ongewijzigd.
    expect(kwark.nutrients.protein).toBe(15);
  });
});

/** Stubt de hele AH-keten die lookupIngredientMatch nodig heeft. */
function stubAhProducts(options: { searchFails?: boolean } = {}) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/mobile-auth/")) {
        return new Response(JSON.stringify({ access_token: "test-token" }), { status: 200 });
      }
      if (url.includes("/product/search/")) {
        if (options.searchFails) return new Response("Access Denied", { status: 403 });
        return new Response(
          JSON.stringify({ products: [{ webshopId: 123, title: "AH Kwark", salesUnitSize: "500 g" }] }),
          { status: 200 },
        );
      }
      if (url.includes("/product/detail/")) {
        return new Response(
          JSON.stringify({
            webshopId: 123,
            title: "AH Kwark",
            salesUnitSize: "500 g",
            nutritionalInformation: [
              { name: "Energie (kcal)", value: "60 kcal" },
              { name: "Eiwitten", value: "10 g" },
            ],
          }),
          { status: 200 },
        );
      }
      return new Response("{}", { status: 200 });
    }),
  );
}

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
  vi.unstubAllGlobals();
  resetEndpointState();
});

describe("lookupIngredientMatch", () => {
  it("matches, stores the product, and remembers the match", async () => {
    stubAhProducts();
    db = createTestDb();
    const store = new Store(db);
    const client = new AhClient("test", undefined, { minIntervalMs: 0, backoffMs: 0 });

    const result = await lookupIngredientMatch("kwark", client, store);

    expect(result.outcome).toBe("matched");
    expect(result.product?.webshopId).toBe("123");
    expect(await store.getMatch("kwark")).toEqual({ webshopId: "123", score: expect.any(Number) });
  });

  it("rethrows on a blocked search instead of swallowing it, so enrichment can count it", async () => {
    stubAhProducts({ searchFails: true });
    db = createTestDb();
    const store = new Store(db);
    const client = new AhClient("test", undefined, { minIntervalMs: 0, backoffMs: 0, maxRetries: 0 });

    await expect(lookupIngredientMatch("kwark", client, store)).rejects.toThrow();
    // Geen negatieve match onthouden voor iets dat gewoon een blokkade was.
    expect(await store.getMatch("kwark")).toBeUndefined();
  });
});

describe("terugval op de webshoppagina als de mobiele API dichtzit", () => {
  it("zoekt via www.ah.nl zodra het anonieme token geweigerd wordt", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        calls.push(url);
        // Dit is wat de echte app overkwam: het token werd geweigerd, waarna
        // elke productzoekopdracht faalde en niets meer gekoppeld werd.
        if (url.includes("/mobile-auth/")) return new Response("Access Denied", { status: 403 });
        if (url.includes("/producten/zoeken")) {
          return new Response(
            '<a href="/producten/product/wi123456/ah-magere-kwark">kwark</a>',
            { status: 200 },
          );
        }
        if (url.includes("/producten/product/wi")) {
          return new Response(
            '<table data-testid="nutrition-table"><tr><td>Energie</td><td>57 kcal</td></tr>' +
              "<tr><td>Eiwitten</td><td>10 g</td></tr></table>",
            { status: 200 },
          );
        }
        return new Response("{}", { status: 200 });
      }),
    );

    const db = createTestDb();
    try {
      const store = new Store(db);
      const client = new AhClient("test", undefined, { minIntervalMs: 0, maxRetries: 0 });
      const result = await lookupIngredientMatch("magere kwark", client, store);

      expect(result.outcome).toBe("matched");
      expect(result.product?.webshopId).toBe("123456");
      expect(result.product?.per100g.kcal).toBe(57);
      expect(calls.some((c) => c.includes("/producten/zoeken"))).toBe(true);
    } finally {
      db.close();
    }
  });
});
