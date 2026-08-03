import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/db/queries";
import type { Recipe } from "../src/ah/types";
import { createTestDb, type TestDb } from "./helpers/d1";
import { enrichOneRecipe, findLocalDb, isRecipeDone, markRecipeDone, needsEnrichment, openIngredientNames } from "../scripts/enrich-lib.mjs";

/**
 * De gedeelde script-logica voor lokale productverrijking
 * (scripts/enrich-lib.mjs): de database vinden, bepalen welke recepten nog
 * verrijkt moeten worden en één recept verrijken met een nep-curl-context —
 * er gaat hier nooit echt netwerk overheen. Zowel enrich-local.mjs als
 * enrich-watch.mjs draaien op deze functies.
 */

const RECEPT: Recipe = {
  id: "R-R1202157",
  title: "Bulgursalade met kikkererwten",
  url: "https://www.ah.nl/allerhande/recept/R-R1202157",
  servings: 4,
  imageUrl: null,
  ingredients: [
    { name: "biologische kikkererwten", quantity: 175, unit: "g" },
    { name: "verse basilicum", quantity: 5, unit: "g" },
    { name: "zout", quantity: 1, unit: "snuf" },
    { name: "water", quantity: 1, unit: "l" },
  ],
};

const SUGGESTIES_BODY = {
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
            webPath: "/producten/product/wi168813",
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
          product: { id: 611642, title: "AH Basilicum", brand: "AH", webPath: "/producten/product/wi611642", salesUnitSize: "60 g" },
        },
      },
    ],
  },
};

const VOEDING = (id: number, kcal = 120) => ({
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
              { type: "ENER-", name: "Energie", value: `${(kcal * 4.2).toFixed(1)} kJ (${kcal}.0 kcal)` },
              { type: "PRO-", name: "Eiwitten", value: "6.5 g" },
            ],
          },
        ],
      },
    },
  },
});

/** Antwoordt als /gql, zonder curl: elke query+variables krijgt een vaste body. */
function fakeCurl(handler: (query: string, variables: Record<string, unknown>) => unknown) {
  const calls: { query: string; variables: Record<string, unknown> }[] = [];
  const ctx = {
    ensureSession: vi.fn(async () => {}),
    gqlPost: vi.fn(async (query: string, variables: Record<string, unknown>) => {
      calls.push({ query, variables });
      const antwoord = handler(query, variables);
      if (antwoord instanceof Error) throw antwoord;
      return antwoord;
    }),
  };
  return { ctx, calls };
}

/** Het gebruikelijke antwoordgedrag: suggesties + voeding per product-id. */
function standaardHandler(options: { geenTradeItem?: Set<number>; bundelVariant?: number; voeding?: (id: number) => unknown } = {}) {
  return (query: string, variables: Record<string, unknown>): unknown => {
    if (query.includes("recipeProductSuggestionsV2")) return SUGGESTIES_BODY;
    if (query.includes("virtualBundleProducts")) {
      if (options.bundelVariant === undefined) {
        return { data: { product: { virtualBundleProducts: [] } } };
      }
      return { data: { product: { virtualBundleProducts: [{ product: { id: options.bundelVariant, title: "variant" } }] } } };
    }
    const id = variables["id"] as number;
    if (options.geenTradeItem?.has(id)) {
      return { data: { product: { __typename: "Product", id, title: "x", tradeItem: null } } };
    }
    return (options.voeding ?? VOEDING)(id);
  };
}

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
});

function testDb() {
  db = createTestDb();
  return new Store(db);
}

describe("findLocalDb", () => {
  it("vindt het grootste echte sqlite-bestand en slaat metadata-bestanden over", () => {
    const root = mkdtempSync(join(tmpdir(), "ah-enrich-lib-"));
    try {
      const d1 = join(root, ".wrangler", "state", "v3", "d1", "miniflare-D1DatabaseObject");
      mkdirSync(d1, { recursive: true });
      writeFileSync(join(d1, "metadata.sqlite"), "klein");
      const echt = join(d1, "6668346aa9447aa6a7fe6925714bc25783ca904c8c6382d69065324d237b58f7.sqlite");
      writeFileSync(echt, "x".repeat(4096));
      writeFileSync(join(d1, "nog-kleiner.sqlite"), "kleiner");

      expect(findLocalDb(root)).toBe(echt);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("geeft null als er geen .wrangler-map is", () => {
    const root = mkdtempSync(join(tmpdir(), "ah-enrich-lib-"));
    try {
      expect(findLocalDb(root)).toBeNull();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("openIngredientNames", () => {
  it("laat vrije ingrediënten (water, zout, peper) buiten beschouwing", async () => {
    const store = testDb();
    const open = await openIngredientNames(store, RECEPT);

    expect(open).toEqual(["biologische kikkererwten", "verse basilicum"]);
  });

  it("is leeg zodra alle niet-vrije ingrediënten een koppeling hebben", async () => {
    const store = testDb();
    await store.putMatch("biologische kikkererwten", "168813", 1);
    await store.putMatch("verse basilicum", "611642", 1);

    expect(await openIngredientNames(store, RECEPT)).toEqual([]);
  });

  it("telt een opgeslagen 'geen match' (null) niet als koppeling", async () => {
    const store = testDb();
    await store.putMatch("biologische kikkererwten", null, 1);

    const open = await openIngredientNames(store, RECEPT);
    expect(open).toContain("biologische kikkererwten");
  });
});

describe("de klaar-markering", () => {
  it("needsEnrichment is waar voor een recept met open ingrediënten", async () => {
    const store = testDb();

    expect(await needsEnrichment(store, RECEPT)).toBe(true);
  });

  it("needsEnrichment is onwaar zodra alles gekoppeld is", async () => {
    const store = testDb();
    await store.putMatch("biologische kikkererwten", "168813", 1);
    await store.putMatch("verse basilicum", "611642", 1);

    expect(await needsEnrichment(store, RECEPT)).toBe(false);
  });

  it("needsEnrichment is onwaar na markRecipeDone, ook met nog open ingrediënten", async () => {
    const store = testDb();
    // Bijv. een ingrediënt waarvoor AH geen suggestie heeft: dat wordt nooit
    // gekoppeld, en elke ronde opnieuw proberen zou alleen maar verzoeken
    // verspillen. De markering zegt: deze poging is afgerond.
    await markRecipeDone(store, RECEPT.id);

    expect(await isRecipeDone(store, RECEPT.id)).toBe(true);
    expect(await needsEnrichment(store, RECEPT)).toBe(false);
  });

  it("markRecipeDone is per recept", async () => {
    const store = testDb();
    await markRecipeDone(store, "R-R1202157");

    expect(await isRecipeDone(store, "R-R1202157")).toBe(true);
    expect(await isRecipeDone(store, "R-R9999999")).toBe(false);
  });
});

describe("enrichOneRecipe", () => {
  it("slaat over (null) en vraagt niets op als alles gekoppeld is", async () => {
    const store = testDb();
    await store.putMatch("biologische kikkererwten", "168813", 1);
    await store.putMatch("verse basilicum", "611642", 1);
    const { ctx, calls } = fakeCurl(standaardHandler());

    expect(await enrichOneRecipe(store, ctx, RECEPT)).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it("verrijkt een recept en telt de samenvatting in gekoppeld/nieuw/cached/fouten", async () => {
    const store = testDb();
    const { ctx, calls } = fakeCurl(standaardHandler());

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 2, nieuw: 2, cached: 0, fouten: 0 });

    // Producten met voedingswaarde in de database.
    expect((await store.getProduct("168813"))?.per100g).toEqual({ kcal: 120, protein: 6.5 });
    expect((await store.getProduct("611642"))?.title).toBe("AH Basilicum");

    // Koppelingen voor de boodschappenlijst; vrije ingrediënten blijven vrij.
    expect(await store.matchMap(["biologische kikkererwten", "verse basilicum", "zout", "water"])).toEqual({
      "biologische kikkererwten": "168813",
      "verse basilicum": "611642",
    });

    // Suggesties + twee productverzoeken, geen bundel-omweg nodig.
    expect(calls).toHaveLength(3);
  });

  it("hergebruikt producten die al in de database staan", async () => {
    const store = testDb();
    await store.putProduct({ webshopId: "168813", title: "al bekend", salesUnitSize: null, per100g: {} });
    const { ctx, calls } = fakeCurl(standaardHandler());

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 2, nieuw: 1, cached: 1, fouten: 0 });
    // Alleen het onbekende product kost een verzoek; het bekende niet.
    expect(calls.filter((c) => c.query.includes("nutrients"))).toHaveLength(1);
  });

  it("volgt de bundel-variant als een product geen tradeItem heeft", async () => {
    const store = testDb();
    const { ctx, calls } = fakeCurl(
      standaardHandler({
        geenTradeItem: new Set([168813]),
        bundelVariant: 999,
        voeding: (id) => VOEDING(id, id === 999 ? 200 : 120),
      }),
    );

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 2, nieuw: 2, cached: 0, fouten: 0 });
    // De voeding van de variant (200 kcal) staat op het bundelproduct.
    expect((await store.getProduct("168813"))?.per100g).toEqual({ kcal: 200, protein: 6.5 });
    expect(calls).toHaveLength(5); // suggesties + 168813 + bundel + 999 + 611642
  });

  it("bewaart een product ook als er nergens voedingswaarde is", async () => {
    const store = testDb();
    const { ctx } = fakeCurl(standaardHandler({ geenTradeItem: new Set([168813, 999]), bundelVariant: 999 }));

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 2, nieuw: 2, cached: 0, fouten: 0 });
    expect((await store.getProduct("168813"))?.per100g).toEqual({});
  });

  it("telt een mislukte productopvraging als fout; de koppeling blijft wel staan", async () => {
    const store = testDb();
    const { ctx } = fakeCurl((query, variables) => {
      if (query.includes("recipeProductSuggestionsV2")) return SUGGESTIES_BODY;
      if (Number(variables["id"]) === 168813) throw new Error("tempo-blokkade");
      return VOEDING(Number(variables["id"]));
    });

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 2, nieuw: 1, cached: 0, fouten: 1 });
    // De koppeling op naam staat er al, het product nog niet.
    expect(await store.matchMap(["biologische kikkererwten"])).toEqual({ "biologische kikkererwten": "168813" });
    expect(await store.getProduct("168813")).toBeNull();
  });

  it("telt een mislukte suggestie-aanvraag als fout en schrijft niets weg", async () => {
    const store = testDb();
    const { ctx } = fakeCurl(() => {
      throw new Error("POST https://www.ah.nl/gql -> 403");
    });

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 0, nieuw: 0, cached: 0, fouten: 1 });
    expect(await store.matchMap(["biologische kikkererwten"])).toEqual({});
    expect(await store.getProduct("168813")).toBeNull();
  });

  it("telt een 200 met GraphQL-fouten als fout, zodat het recept niet klaar wordt gevinkt", async () => {
    const store = testDb();
    const { ctx } = fakeCurl((query) => {
      if (query.includes("recipeProductSuggestionsV2")) {
        return { errors: [{ message: "something went wrong" }] };
      }
      return VOEDING(Number(0));
    });

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 0, nieuw: 0, cached: 0, fouten: 1 });
    expect(await store.matchMap(["biologische kikkererwten"])).toEqual({});
  });

  it("een geldige lege suggestielijst is geen fout: AH heeft gewoon geen producten", async () => {
    const store = testDb();
    const { ctx } = fakeCurl((query) => {
      if (query.includes("recipeProductSuggestionsV2")) {
        return { data: { recipeProductSuggestionsV2: [] } };
      }
      return VOEDING(Number(0));
    });

    const line = await enrichOneRecipe(store, ctx, RECEPT);

    expect(line).toEqual({ gekoppeld: 0, nieuw: 0, cached: 0, fouten: 0 });
    expect(await store.matchMap(["biologische kikkererwten"])).toEqual({});
  });
});
