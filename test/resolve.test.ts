import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient, resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import {
  fillFromRecipeTotal,
  firstIncomplete,
  isNutritionFree,
  lookupIngredientMatch,
} from "../src/nutrition/resolve";
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
function stubAhProducts(options: { searchFails?: boolean; titles?: string[] } = {}) {
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
        const titles = options.titles ?? ["AH Magere kwark"];
        return new Response(
          JSON.stringify({
            products: titles.map((title, i) => ({ webshopId: 123 + i, title })),
          }),
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
    nutrientSource: per100g ? ("product" as const) : ("onbekend" as const),
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
  source: "products",
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

describe("fillFromRecipeTotal", () => {
  /** Hetzelfde recept, maar mét de voedingswaarde die AH er zelf bij zet. */
  const withAhNutrition = (
    resolved: ResolvedRecipe,
    perServing: Record<string, number>,
    servings = 1,
  ): ResolvedRecipe => ({
    ...resolved,
    recipe: { ...resolved.recipe, servings, nutritionPerServing: perServing },
  });

  it("vult het gat met wat AH's totaal nog niet verklaart", () => {
    // 100 g havermout is 375 kcal; AH zegt 500 voor het hele recept, dus de
    // courgette waar geen product bij te vinden was is de resterende 125.
    const resolved = withAhNutrition(
      resolvedWith(ingredient("havermout", { kcal: 375 }), ingredient("courgette", null)),
      { kcal: 500 },
    );
    expect(fillFromRecipeTotal(resolved.recipe, resolved.ingredients)).toBe(true);
    expect(resolved.ingredients[1]!.nutrients.kcal).toBeCloseTo(125);
    expect(resolved.ingredients[1]!.nutrientSource).toBe("geschat");
    // En daarmee is het recept bruikbaar in plaats van afgekeurd.
    expect(firstIncomplete(resolved)).toBeNull();
  });

  it("verdeelt het restant naar gewicht over de ontbrekende regels", () => {
    const light = ingredient("peterselie", null);
    const heavy = ingredient("kip", null);
    heavy.grams = 300; // 100 g peterselie tegen 300 g kip
    const resolved = withAhNutrition(resolvedWith(light, heavy), { kcal: 400 });

    fillFromRecipeTotal(resolved.recipe, resolved.ingredients);
    expect(light.nutrients.kcal).toBeCloseTo(100);
    expect(heavy.nutrients.kcal).toBeCloseTo(300);
  });

  it("rekent AH's cijfers per portie om naar het hele recept", () => {
    const resolved = withAhNutrition(resolvedWith(ingredient("courgette", null)), { kcal: 250 }, 4);
    fillFromRecipeTotal(resolved.recipe, resolved.ingredients);
    expect(resolved.ingredients[0]!.nutrients.kcal).toBeCloseTo(1000);
  });

  it("trekt niets af als de producten AH's totaal al overschrijden", () => {
    const resolved = withAhNutrition(
      resolvedWith(ingredient("havermout", { kcal: 375 }), ingredient("courgette", null)),
      { kcal: 300 },
    );
    fillFromRecipeTotal(resolved.recipe, resolved.ingredients);
    expect(resolved.ingredients[0]!.nutrients.kcal).toBe(375);
    expect(resolved.ingredients[1]!.nutrients.kcal).toBeUndefined();
  });

  it("doet niets zonder voedingswaarde van AH, en houdt het oordeel streng", () => {
    const resolved = resolvedWith(
      ingredient("havermout", { kcal: 375 }),
      ingredient("courgette", null),
    );
    expect(fillFromRecipeTotal(resolved.recipe, resolved.ingredients)).toBe(false);
    expect(firstIncomplete(resolved)).toBe("courgette");
  });

  it("laat een recept waarvan alles al matcht met rust", () => {
    const resolved = withAhNutrition(resolvedWith(ingredient("havermout", { kcal: 375 })), {
      kcal: 500,
    });
    expect(fillFromRecipeTotal(resolved.recipe, resolved.ingredients)).toBe(false);
    expect(resolved.ingredients[0]!.nutrients.kcal).toBe(375);
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

describe("als de volledige naam niets oplevert", () => {
  it("zoekt nog één keer op de kernnaam", async () => {
    const queries: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("/mobile-auth/")) {
          return new Response(JSON.stringify({ access_token: "test-token" }), { status: 200 });
        }
        if (url.includes("/product/search/")) {
          const query = new URL(url).searchParams.get("query") ?? "";
          queries.push(query);
          // Zoals AH zich gedraagt: op de hele receptregel niets bruikbaars, op
          // het kernwoord wel.
          const products = query === "scharrelei"
            ? [{ webshopId: 5, title: "AH Scharreleieren" }]
            : [{ webshopId: 9, title: "AH Roomboter" }];
          return new Response(JSON.stringify({ products }), { status: 200 });
        }
        if (url.includes("/producten/product/wi")) {
          return new Response(
            '<table data-testid="nutrition-table"><tr><td>Energie</td><td>139 kcal</td></tr>' +
              "<tr><td>Eiwitten</td><td>12 g</td></tr></table>",
            { status: 200 },
          );
        }
        return new Response("{}", { status: 200 });
      }),
    );

    db = createTestDb();
    const store = new Store(db);
    const client = new AhClient("test", undefined, { minIntervalMs: 0 });

    const result = await lookupIngredientMatch("middelgroot scharrelei", client, store);

    expect(queries).toEqual(["middelgroot scharrelei", "scharrelei"]);
    expect(result.outcome).toBe("matched");
    expect(result.product?.title).toBe("AH Scharreleieren");
  });

  it("legt vast wat het zag toen er niets matchte", async () => {
    stubAhProducts({ searchFails: false, titles: ["Snelfilterkoffie", "Koffiebonen"] });
    db = createTestDb();
    const store = new Store(db);
    const client = new AhClient("test", undefined, { minIntervalMs: 0 });

    await lookupIngredientMatch("sjalot", client, store);

    // Zonder deze regel is een afgekeurd recept een dood spoor: je ziet wel
    // dát het misging, niet waaróp.
    const logs = await store.recentLogs(10);
    const miss = logs.find((l) => l.message.includes("geen match"));
    expect(miss).toBeDefined();
    expect(JSON.parse(miss!.detail!)).toMatchObject({
      kandidaten: 2,
      titels: expect.arrayContaining(["Snelfilterkoffie"]),
    });
  });
});
