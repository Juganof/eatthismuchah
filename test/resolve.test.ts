import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient, resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { firstIncomplete, isNutritionFree, lookupIngredientMatch } from "../src/nutrition/resolve";
import type { ResolvedRecipe } from "../src/ah/types";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * Twee dingen liggen hier vast: dat een ingredientnaam bij het juiste product
 * uitkomt en dat dat onthouden wordt, en de regel die bepaalt of een recept
 * compleet genoeg is om opgeslagen te worden.
 */

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
  vi.unstubAllGlobals();
  resetEndpointState();
});

/** Doet alsof AH één product kent: "AH Magere kwark". */
function stubAhProducts(options: { searchFails?: boolean } = {}) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/mobile-auth/")) {
        return new Response(JSON.stringify({ access_token: "test-token" }), { status: 200 });
      }
      if (url.includes("/product/search/")) {
        // 403 op de zoekopdracht zelf is Akamai's tempo-blokkade, geen "niets
        // gevonden": die hoort door te slaan naar de aanroeper.
        if (options.searchFails) return new Response("Access Denied", { status: 403 });
        return new Response(
          JSON.stringify({ products: [{ webshopId: 123, title: "AH Magere kwark" }] }),
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
}

/** Eén opgelost ingredient, met of zonder product erachter. */
function ingredient(name: string, per100g: Record<string, number> | null) {
  return {
    raw: { name, quantity: 100, unit: "g" },
    grams: 100,
    product: per100g ? { webshopId: "1", title: name, salesUnitSize: null, per100g } : null,
    gramsSource: "explicit" as const,
    matchScore: per100g ? 1 : 0,
    nutrients: per100g ?? {},
  };
}

const resolvedWith = (...ingredients: ReturnType<typeof ingredient>[]): ResolvedRecipe => ({
  recipe: {
    id: "R-R1",
    title: "Testrecept",
    url: "https://www.ah.nl/allerhande/recept/R-R1",
    servings: 1,
    imageUrl: null,
    ingredients: ingredients.map((i) => i.raw),
  },
  ingredients,
  total: {},
});

describe("isNutritionFree", () => {
  it("herkent ingredienten die geen voedingswaarde hébben", () => {
    expect(isNutritionFree("water")).toBe(true);
    expect(isNutritionFree("200 ml kraanwater")).toBe(true);
    expect(isNutritionFree("een snuf zout")).toBe(true);
    expect(isNutritionFree("versgemalen zwarte peper")).toBe(true);
  });

  it("laat zich niet vangen door een woord dat er alleen op lijkt", () => {
    // Anders zou "peperoni" als nul tellen en het recept stilletjes te laag uitkomen.
    expect(isNutritionFree("peperoni")).toBe(false);
    expect(isNutritionFree("kokoswater")).toBe(false);
    expect(isNutritionFree("water met citroen")).toBe(false);
    expect(isNutritionFree("zoute pinda's")).toBe(false);
  });
});

describe("firstIncomplete", () => {
  it("noemt het ingredient waarop het recept sneuvelt", () => {
    const resolved = resolvedWith(
      ingredient("havermout", { kcal: 375 }),
      ingredient("middelgroot scharrelei", null),
    );
    expect(firstIncomplete(resolved)).toBe("middelgroot scharrelei");
  });

  it("vindt niets mis aan een recept met alleen echte cijfers en water", () => {
    const resolved = resolvedWith(
      ingredient("havermout", { kcal: 375 }),
      ingredient("water", null),
    );
    expect(firstIncomplete(resolved)).toBeNull();
  });

  it("keurt een product zonder calorieen af, want dat is geen cijfer", () => {
    const resolved = resolvedWith(ingredient("mysterie", {}));
    expect(firstIncomplete(resolved)).toBe("mysterie");
  });
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
