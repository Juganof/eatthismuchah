import { afterEach, describe, expect, it, vi } from "vitest";
import { resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { enrichRecipeWithProducts, matchSuggestionsToIngredients } from "../src/ingest/enrich";
import { ingestComplete } from "../src/ingest/pipeline";
import type { Recipe } from "../src/ah/types";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * De enrich-stap: na het opslaan van een recept de echte AH-producten per
 * ingrediënt ophalen (via /gql) en bewaren, zodat de boodschappenlijst links
 * krijgt en de planner per regel gemeten voedingswaarde heeft.
 */

const RECIPE: Recipe = {
  id: "R-R1202157",
  title: "Bulgursalade met kikkererwten",
  url: "https://www.ah.nl/allerhande/recept/R-R1202157",
  servings: 4,
  imageUrl: null,
  ingredients: [
    { name: "biologische kikkererwten", quantity: 175, unit: "g" },
    { name: "verse basilicum", quantity: 5, unit: "g" },
  ],
};

const SUGGESTIONS_BODY = {
  data: {
    recipeProductSuggestionsV2: [
      {
        optional: false,
        ingredient: { id: 1906510, name: "biologische kikkererwten", quantityFloat: 175, quantityUnit: "g" },
        productSuggestion: {
          id: 168813,
          quantity: 1,
          proposer: "A",
          product: {
            id: 168813,
            title: "AH Terra Biologisch kikkererwten",
            brand: "AH Terra",
            webPath: "/producten/product/wi168813/ah-terra-biologisch-kikkererwten",
            salesUnitSize: "330 g",
          },
        },
      },
      {
        optional: false,
        ingredient: { id: 1906511, name: "verse basilicum", quantityFloat: 5, quantityUnit: "g" },
        productSuggestion: {
          id: 611642,
          quantity: 1,
          proposer: "A",
          product: {
            id: 611642,
            title: "AH Basilicum",
            brand: "AH",
            webPath: "/producten/product/wi611642/ah-basilicum",
            salesUnitSize: "60 g",
          },
        },
      },
    ],
  },
};

const NUTRITION_BODY = (id: number) => ({
  data: {
    product: {
      __typename: "Product",
      id,
      title: "product",
      tradeItem: {
        __typename: "ProductTradeItem",
        gtin: "0",
        nutritions: [
          {
            __typename: "ProductTradeItemNutrition",
            basisQuantity: "100.0 Gram",
            basisQuantityDescription: "",
            preparationState: "Onbereide",
            servingSize: null,
            servingSizeDescription: null,
            nutrients: [
              { type: "ENER-", name: "Energie", value: "504.0 kJ (120.0 kcal)" },
              { type: "PRO-", name: "Eiwitten", value: "6.5 g" },
            ],
          },
        ],
      },
    },
  },
});

/** Antwoordt als www.ah.nl + /gql, en onthoudt welke verzoeken er kwamen. */
function stubAhAndGql(options: { status?: number; noSuggestions?: boolean } = {}) {
  const calls: string[] = [];
  const status = options.status ?? 200;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      calls.push(url);
      if ((init?.method ?? "GET") === "GET") {
        return new Response("<html></html>", { status: 200 });
      }
      const parsed = JSON.parse(String(init?.body)) as { query: string; variables: { id?: number } };
      if (parsed.query.includes("recipeProductSuggestionsV2")) {
        if (options.noSuggestions) {
          return new Response(JSON.stringify({ data: { recipeProductSuggestionsV2: [] } }), { status });
        }
        return new Response(JSON.stringify(SUGGESTIONS_BODY), { status });
      }
      const id = parsed.variables.id ?? 0;
      return new Response(JSON.stringify(NUTRITION_BODY(id)), { status });
    }),
  );
  return calls;
}

const nutritionPage = (kcal: number, eiwit: number) =>
  `<html><table data-testid="nutrition-table">
     <tr><td>Energie</td><td>${kcal} kcal</td></tr>
     <tr><td>Eiwitten</td><td>${eiwit} g</td></tr>
   </table></html>`;

const SEARCH_PAGE = (ids: string[]) =>
  `<html>${ids
    .map(
      (id) =>
        `<a data-testid="recipe-card" href="https://www.ah.nl/allerhande/recept/${id}/iets" title="Recept: ${id}"></a>`,
    )
    .join("")}</html>`;

const recipePage = (id: string, ingredients: string[]) =>
  `<html><script type="application/ld+json">${JSON.stringify({
    "@type": "Recipe",
    name: "Testrecept " + id,
    url: `https://www.ah.nl/allerhande/recept/${id}/iets`,
    recipeYield: "2",
    keywords: "ontbijt",
    recipeIngredient: ingredients,
    nutrition: { calories: "300 kcal energie", proteinContent: "20 g eiwit" },
  })}</script></html>`;

/** Volledige ronde: zoekpagina + receptpagina's + /gql. */
function stubFullRound(suggestionsBody: unknown = SUGGESTIONS_BODY) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if ((init?.method ?? "GET") === "GET") {
        if (url.includes("/recepten-zoeken")) {
          return new Response(SEARCH_PAGE(["R-R101", "R-R102"]), { status: 200 });
        }
        if (url.includes("/allerhande/recept/")) {
          const id = url.match(/recept\/(R-R\d+)/)?.[1] ?? "R-R1";
          return new Response(recipePage(id, ["100 g havermout", "200 g kwark"]), { status: 200 });
        }
        return new Response("<html></html>", { status: 200 });
      }
      const parsed = JSON.parse(String(init?.body)) as { query: string };
      if (parsed.query.includes("recipeProductSuggestionsV2")) {
        return new Response(JSON.stringify(suggestionsBody), { status: 200 });
      }
      return new Response(JSON.stringify(NUTRITION_BODY(99)), { status: 200 });
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

function envFor(extra: Record<string, string> = {}): { DB: TestDb; AH_USER_AGENT: string } {
  db = createTestDb();
  return { DB: db, AH_USER_AGENT: "test", ...extra };
}

describe("matchSuggestionsToIngredients", () => {
  const suggestions = [
    { ingredientName: "biologische kikkererwten", productId: "168813", productTitle: "x", salesUnitSize: "330 g", suggestedPackages: 1 },
    { ingredientName: "verse basilicum", productId: "611642", productTitle: "x", salesUnitSize: "60 g", suggestedPackages: 1 },
  ];

  it("koppelt suggesties aan regels op genormaliseerde naam", () => {
    const result = matchSuggestionsToIngredients(RECIPE.ingredients, suggestions);

    expect(result[0]!.productId).toBe("168813");
    expect(result[1]!.productId).toBe("611642");
  });

  it("valt terug op de volgorde als de naam nergens op lijkt", () => {
    const shifted = [
      { ...suggestions[1]!, ingredientName: "iets heel anders" },
      { ...suggestions[0]!, ingredientName: "ook niks herkenbaars" },
    ];
    const result = matchSuggestionsToIngredients(RECIPE.ingredients, shifted);

    expect(result[0]!.productId).toBe(shifted[0]!.productId);
    expect(result[1]!.productId).toBe(shifted[1]!.productId);
  });

  it("laat een regel zonder suggestie leeg", () => {
    const result = matchSuggestionsToIngredients(RECIPE.ingredients, []);

    expect(result).toEqual([null, null]);
  });
});

describe("enrichRecipeWithProducts", () => {
  it("slaat producten en koppelingen op, en telt wat het deed", async () => {
    const calls = stubAhAndGql();
    const env = envFor();
    const store = new Store(env.DB);

    const result = await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 40, backoffMs: 0, minIntervalMs: 0 });

    expect(result).toMatchObject({ matched: 2, products: 2, cached: 0, errors: [] });

    // Producten met voedingswaarde in de database.
    const kikkererwten = await store.getProduct("168813");
    expect(kikkererwten?.title).toBe("AH Terra Biologisch kikkererwten");
    expect(kikkererwten?.salesUnitSize).toBe("330 g");
    expect(kikkererwten?.per100g).toEqual({ kcal: 120, protein: 6.5 });

    // Koppelingen voor de boodschappenlijst.
    expect(await store.matchMap(["biologische kikkererwten", "verse basilicum"])).toEqual({
      "biologische kikkererwten": "168813",
      "verse basilicum": "611642",
    });

    // Eén sessie-GET, één suggestie-verzoek, twee product-verzoeken.
    expect(calls.filter((u) => u === "https://www.ah.nl/")).toHaveLength(1);
    expect(calls.filter((u) => u.includes("/gql"))).toHaveLength(3);
  });

  it("hergebruikt producten die al in de database staan", async () => {
    const calls = stubAhAndGql();
    const env = envFor();
    const store = new Store(env.DB);

    await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 40, backoffMs: 0, minIntervalMs: 0 });
    const callsAfterFirst = calls.length;
    const result = await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 40, backoffMs: 0, minIntervalMs: 0 });

    expect(result).toMatchObject({ matched: 2, products: 0, cached: 2, errors: [] });
    expect(calls.length - callsAfterFirst).toBe(2); // alleen sessie-GET + suggesties
  });

  it("slaat een recept zonder suggesties rustig over", async () => {
    stubAhAndGql({ noSuggestions: true });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 40, backoffMs: 0, minIntervalMs: 0 });

    expect(result).toMatchObject({ matched: 0, products: 0, errors: [] });
    expect(await store.countRecipes()).toBe(0); // niets weggeschreven
  });

  it("stopt netjes op het verzoekbudget en gooit niet", async () => {
    stubAhAndGql();
    const env = envFor();
    const store = new Store(env.DB);

    // Budget 2: sessie-GET + suggesties passen er net in; de producten niet meer.
    const result = await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 2, backoffMs: 0, minIntervalMs: 0 });

    expect(result.errors).toHaveLength(0);
    expect(result.matched).toBe(2); // koppelingen op naam zijn lokaal
    expect(await store.matchMap(["biologische kikkererwten"])).toEqual({
      "biologische kikkererwten": "168813",
    });
  });

  it("meldt een blokkade van /gql als fout zonder te crashen", async () => {
    stubAhAndGql({ status: 403 });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await enrichRecipeWithProducts(env, store, RECIPE, { maxRequests: 40, backoffMs: 0, minIntervalMs: 0 });

    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.matched).toBe(0);
  });
});

describe("de ingest-ronde met verrijken aan", () => {
  it("vult producten en koppelingen aan bij elk opgeslagen recept", async () => {
    stubFullRound();
    const env = envFor();

    await ingestComplete(env, ["kip"], 5, { minIntervalMs: 0, backoffMs: 0, maxRequests: 40 }, { perRun: 5 });

    const store = new Store(env.DB);
    expect(await store.countRecipes()).toBe(2);
    // Producten en koppelingen uit de /gql-suggesties. De mock-suggesties heten
    // "biologische kikkererwten"/"verse basilicum" en matchen dus niet op naam
    // met "havermout"/"kwark" — de code valt terug op volgorde, zoals de
    // matchSuggestionsToIngredients-tests hierboven vastleggen. Kwark krijgt
    // daardoor de tweede suggestie (611642), niet de eerste.
    expect(await store.getProduct("168813")).not.toBeNull();
    expect(await store.matchMap(["havermout", "kwark"])).toEqual({
      havermout: "168813",
      kwark: "611642",
    });
  });

  it("doet niets aan verrijken als het uitstaat", async () => {
    stubFullRound();
    const env = envFor();

    await ingestComplete(env, ["kip"], 5, { minIntervalMs: 0, backoffMs: 0, maxRequests: 40 });

    const store = new Store(env.DB);
    expect(await store.countRecipes()).toBe(2);
    expect(await store.getProduct("168813")).toBeNull();
  });
});
