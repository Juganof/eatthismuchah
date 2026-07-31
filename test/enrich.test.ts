import { afterEach, describe, expect, it, vi } from "vitest";
import { Store } from "../src/db/queries";
import { enrichIngredientMatches } from "../src/ingest/pipeline";
import { AhClient, resetEndpointState } from "../src/ah/client";
import { rerollSlot } from "../src/optimize/day";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * `enrichIngredientMatches` is the fix for the bug the user actually hit: a
 * recipe with AH's own nutrition-per-serving skips per-ingredient product
 * matching entirely during ingestion (see `computeNutrition` in
 * src/ingest/pipeline.ts), so every ingredient stays "unmatched" forever and
 * the planner can only scale the whole dish by one factor. This is the
 * background pass that fills those matches in afterwards.
 */

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
  vi.unstubAllGlobals();
  resetEndpointState();
});

function envFor(): { DB: TestDb; AH_USER_AGENT: string } {
  db = createTestDb();
  return { DB: db, AH_USER_AGENT: "test" };
}

const PRODUCTS: Record<string, { webshopId: number; kcal: string; protein: string }> = {
  kwark: { webshopId: 101, kcal: "60 kcal", protein: "10 g" },
  banaan: { webshopId: 102, kcal: "90 kcal", protein: "1 g" },
};

/** Stubt zoeken + detail voor elk van de bovenstaande termen. */
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
        const query = new URL(url).searchParams.get("query") ?? "";
        const hit = Object.entries(PRODUCTS).find(([name]) => query.includes(name));
        const products = hit ? [{ webshopId: hit[1].webshopId, title: "AH " + hit[0], salesUnitSize: "500 g" }] : [];
        return new Response(JSON.stringify({ products }), { status: 200 });
      }
      if (url.includes("/product/detail/")) {
        const id = Number(url.split("/").pop());
        const entry = Object.entries(PRODUCTS).find(([, p]) => p.webshopId === id);
        if (!entry) return new Response("{}", { status: 200 });
        const [name, product] = entry;
        return new Response(
          JSON.stringify({
            webshopId: product.webshopId,
            title: "AH " + name,
            salesUnitSize: "500 g",
            nutritionalInformation: [
              { name: "Energie (kcal)", value: product.kcal },
              { name: "Eiwitten", value: product.protein },
            ],
          }),
          { status: 200 },
        );
      }
      return new Response("{}", { status: 200 });
    }),
  );
}

describe("enrichIngredientMatches", () => {
  it("matches an unlooked-up ingredient and writes it to the cache", async () => {
    stubAhProducts();
    const env = envFor();
    const store = new Store(env.DB);
    await store.putRecipe({
      id: "R-R1",
      title: "Kwark met granola",
      url: "https://www.ah.nl/allerhande/recept/R-R1",
      servings: 1,
      imageUrl: null,
      ingredients: [{ name: "kwark", quantity: 150, unit: "g" }],
    });

    const result = await enrichIngredientMatches(env, 10, { minIntervalMs: 0, backoffMs: 0 });

    expect(result.examined).toBe(1);
    expect(result.matched).toBe(1);
    expect(result.unmatched).toBe(0);
    expect(result.blocked).toBe(0);
    expect(await store.getMatch("kwark")).toEqual({ webshopId: "101", score: expect.any(Number) });
  });

  it("counts a block without throwing, so the round can move on", async () => {
    stubAhProducts({ searchFails: true });
    const env = envFor();
    const store = new Store(env.DB);
    await store.putRecipe({
      id: "R-R1",
      title: "Kwark met granola",
      url: "https://www.ah.nl/allerhande/recept/R-R1",
      servings: 1,
      imageUrl: null,
      ingredients: [{ name: "kwark", quantity: 150, unit: "g" }],
    });

    const result = await enrichIngredientMatches(env, 10, { minIntervalMs: 0, backoffMs: 0, maxRetries: 0 });

    expect(result.examined).toBe(1);
    expect(result.blocked).toBe(1);
    expect(result.matched).toBe(0);
    expect(await store.getMatch("kwark")).toBeUndefined();
  });

  it("maakt een recept pas plambaar zodra de koppelingen er zijn", async () => {
    stubAhProducts();
    const env = envFor();
    const store = new Store(env.DB);

    // Precies het geval uit de bug: AH's eigen voedingswaarde is er, maar geen
    // van de ingredienten is ooit aan een product gekoppeld.
    await store.putRecipe({
      id: "R-R1",
      title: "Kwark met banaan",
      url: "https://www.ah.nl/allerhande/recept/R-R1",
      servings: 1,
      imageUrl: null,
      ingredients: [
        { name: "kwark", quantity: 150, unit: "g" },
        // Eén banaan, geen bare-count van 120 — de piece-tabel in units.ts kent
        // "banaan" al een gewicht van 120 g toe.
        { name: "banaan", quantity: 1, unit: null },
      ],
    });
    await store.putNutrition("R-R1", { kcal: 200, protein: 20, carbs: 30, fat: 1, fiber: 2 }, 1, "ah");

    // Meer eiwit, minder koolhydraten dan het recept van nature heeft: kwark
    // levert het een (weinig koolhydraten), banaan het ander (weinig eiwit), dus
    // dit doel kan alleen gehaald worden door ze verschillend bij te sturen.
    const targets = { kcal: 200, protein: 35, carbs: 15, fat: 1, fiber: 2 };

    // Zonder koppelingen kennen we alleen AH's portietotaal, en daarmee wordt
    // niet gepland: geen voorstel is eerlijker dan een geschat voorstel.
    const before = await rerollSlot(store, new AhClient("test"), { targets, excludeRecipeIds: [] });
    expect(before).toBeNull();

    const enriched = await enrichIngredientMatches(env, 10, { minIntervalMs: 0, backoffMs: 0 });
    expect(enriched.matched).toBe(2);

    const after = await rerollSlot(store, new AhClient("test"), { targets, excludeRecipeIds: [] });

    expect(after).not.toBeNull();
    // Kwark en banaan hebben nu een eigen voedingswaarde, dus de solver kan ze
    // los bijsturen in plaats van allebei met dezelfde factor te schalen.
    const scales = new Set(after!.ingredients.map((i) => i.scale));
    expect(scales.size).toBeGreaterThan(1);
  });
});
