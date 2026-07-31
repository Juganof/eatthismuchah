import { afterEach, describe, expect, it, vi } from "vitest";
import { Store } from "../src/db/queries";
import { DEFAULT_AUTO_CONFIG, autoStatus, configFrom, runAutoIngest } from "../src/ingest/auto";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * De automaat draait op een cron waar niemand naar kijkt, dus het gedrag moet
 * hier vastliggen: wanneer hij wel gaat, wanneer hij zich inhoudt, en dat hij
 * zijn rondes bijhoudt zodat je achteraf kunt zien wat er gebeurde.
 */

const SEARCH_PAGE = (ids: string[]) =>
  `<html>${ids
    .map(
      (id) =>
        `<a data-testid="recipe-card" href="https://www.ah.nl/allerhande/recept/${id}/iets" title="Recept: ${id}"></a>`,
    )
    .join("")}</html>`;

const recipePage = (id: string, keywords: string) =>
  `<html><script type="application/ld+json">${JSON.stringify({
    "@type": "Recipe",
    name: "Testrecept " + id,
    url: `https://www.ah.nl/allerhande/recept/${id}/iets`,
    recipeYield: "2",
    keywords,
    nutrition: { calories: "400 kcal", proteinContent: "30 g", carbohydrateContent: "40 g", fatContent: "10 g" },
    recipeIngredient: ["100 g havermout", "200 g kwark"],
  })}</script></html>`;

/** Doet alsof ah.nl antwoordt: zoekpagina's en receptpagina's, geen netwerk. */
function stubAh(options: { keywords?: string; blockDetails?: boolean; blockProducts?: boolean } = {}) {
  const calls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      calls.push(url);
      if (url.includes("/mobile-auth/")) {
        return new Response(JSON.stringify({ access_token: "test-token" }), { status: 200 });
      }
      if (url.includes("/recepten-zoeken") || url.includes("/service/search/recipes")) {
        return new Response(SEARCH_PAGE(["R-R101", "R-R102"]), { status: 200 });
      }
      if (url.includes("/allerhande/recept/")) {
        if (options.blockDetails) return new Response("Access Denied", { status: 403 });
        const id = url.match(/recept\/(R-R\d+)/)?.[1] ?? "R-R1";
        return new Response(recipePage(id, options.keywords ?? "ontbijt"), { status: 200 });
      }
      if (url.includes("/product/search/")) {
        if (options.blockProducts) return new Response("Access Denied", { status: 403 });
        return new Response(JSON.stringify({ products: [{ webshopId: 1, title: "AH Havermout" }] }), { status: 200 });
      }
      if (url.includes("/product/detail/")) {
        return new Response(
          JSON.stringify({
            webshopId: 1,
            title: "AH Havermout",
            nutritionalInformation: [
              { name: "Energie (kcal)", value: "375 kcal" },
              { name: "Eiwitten", value: "13 g" },
            ],
          }),
          { status: 200 },
        );
      }
      return new Response("{}", { status: 200 });
    }),
  );
  return calls;
}

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
  vi.unstubAllGlobals();
});

function envFor(): { DB: TestDb; AH_USER_AGENT: string } {
  db = createTestDb();
  return { DB: db, AH_USER_AGENT: "test" };
}

/** Zonder pauzes, anders duurt elke test seconden. */
const fastConfig = { ...DEFAULT_AUTO_CONFIG, minIntervalMs: 0, backoffMs: 0, batch: 2 };

describe("runAutoIngest", () => {
  it("fetches new recipes and records the round", async () => {
    stubAh();
    const env = envFor();

    const result = await runAutoIngest(env, fastConfig);

    expect(result.ran).toBe(true);
    expect(result.mode).toBe("moment");
    expect(result.added).toBeGreaterThan(0);

    const runs = await new Store(env.DB).recentRuns(5);
    expect(runs).toHaveLength(1);
    expect(runs[0]!["mode"]).toBe("moment");
    expect(runs[0]!["finished_at"]).not.toBeNull();
  });

  it("takes AH's own nutrition, so no product lookups are needed", async () => {
    const calls = stubAh();
    const env = envFor();

    await runAutoIngest(env, fastConfig);

    // Geen enkele aanroep naar de productcatalogus.
    expect(calls.some((url) => url.includes("product/search"))).toBe(false);
    const row = await env.DB.prepare("SELECT source, kcal FROM recipe_nutrition LIMIT 1").first<{
      source: string;
      kcal: number;
    }>();
    expect(row?.source).toBe("ah");
    // 400 kcal per portie, 2 porties.
    expect(row?.kcal).toBe(800);
  });

  it("finishes empty recipes before fetching new ones", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);
    // Een recept dat alleen als titel bestaat, zoals een geblokkeerde scrape achterlaat.
    await store.putRecipe({
      id: "R-R999",
      title: "Leeg recept",
      url: "https://www.ah.nl/allerhande/recept/R-R999",
      servings: 4,
      imageUrl: null,
      ingredients: [],
    });

    const result = await runAutoIngest(env, fastConfig);

    expect(result.mode).toBe("repair");
    expect(result.repaired).toBe(1);
    expect(await store.countWithoutIngredients()).toBe(0);
  });

  it("rotates over the eating moments instead of hammering one", async () => {
    stubAh({ keywords: "ontbijt, lunch, tussendoortje, hoofdgerecht" });
    const env = envFor();
    const store = new Store(env.DB);
    // Los van enrichment: dat heeft zijn eigen tests hieronder, en zou anders
    // een van deze vier rondes inpikken.
    const config = { ...fastConfig, enrichEvery: 1000 };

    const seen: string[] = [];
    for (let i = 0; i < 4; i++) {
      const result = await runAutoIngest(env, config);
      seen.push((result.detail ?? "").split(":")[0]!);
      // Ruim de lege recepten op, anders kiest de volgende ronde voor repareren.
      await store.putSlots([]);
    }

    expect(new Set(seen).size).toBe(4);
    expect(seen).toEqual(["ontbijt", "lunch", "snack", "diner"]);
  });

  it("cools down after AH blocks it, and skips the next round", async () => {
    stubAh({ blockDetails: true });
    const env = envFor();

    const first = await runAutoIngest(env, fastConfig);
    expect(first.blocked).toBeGreaterThan(0);
    expect(first.cooldownUntil).toBeGreaterThan(Date.now());

    // Doorgaan alsof er niets is maakt het alleen erger, dus de volgende ronde
    // hoort zichzelf over te slaan.
    const second = await runAutoIngest(env, fastConfig);
    expect(second.ran).toBe(false);
    expect(second.reason).toContain("afkoelen");
  });

  it("doubles the cooldown on each consecutive block, capped at the maximum", async () => {
    // A block that outlasts one cooldown period means trying again right after it
    // expires just walks into the same wall; the cooldown should grow instead of
    // resetting to the same fixed length every time.
    const env = envFor();
    const config = { ...fastConfig, cooldownMs: 1000, maxCooldownMs: 3000 };

    stubAh({ blockDetails: true });
    const first = await runAutoIngest(env, config, { force: true });
    expect(first.cooldownUntil! - Date.now()).toBeCloseTo(1000, -2);

    stubAh({ blockDetails: true });
    const second = await runAutoIngest(env, config, { force: true });
    expect(second.cooldownUntil! - Date.now()).toBeCloseTo(2000, -2);

    // Capped rather than continuing to double forever.
    stubAh({ blockDetails: true });
    const third = await runAutoIngest(env, config, { force: true });
    expect(third.cooldownUntil! - Date.now()).toBeCloseTo(3000, -2);

    // A clean round resets the streak, so the next block starts back at the base.
    stubAh();
    await runAutoIngest(env, config, { force: true });

    stubAh({ blockDetails: true });
    const afterReset = await runAutoIngest(env, config, { force: true });
    expect(afterReset.cooldownUntil! - Date.now()).toBeCloseTo(1000, -2);
  });

  it("still runs when forced, so you can test without waiting out the cooldown", async () => {
    stubAh({ blockDetails: true });
    const env = envFor();
    await runAutoIngest(env, fastConfig);

    const forced = await runAutoIngest(env, fastConfig, { force: true });
    expect(forced.ran).toBe(true);
  });

  it("stops for the day once the budget is spent", async () => {
    stubAh();
    const env = envFor();

    await runAutoIngest(env, { ...fastConfig, dailyMax: 1 });
    const second = await runAutoIngest(env, { ...fastConfig, dailyMax: 1 });

    expect(second.ran).toBe(false);
    expect(second.reason).toContain("dagbudget");
  });
});

describe("enrichment tier", () => {
  /** Een recept met ingredienten die nooit aan een product zijn gekoppeld. */
  async function seedUnmatched(store: Store) {
    await store.putRecipe({
      id: "R-R1",
      title: "Havermout met kwark",
      url: "https://www.ah.nl/allerhande/recept/R-R1",
      servings: 1,
      imageUrl: null,
      ingredients: [{ name: "havermout", quantity: 80, unit: "g" }],
    });
    await store.putNutrition("R-R1", { kcal: 300, protein: 13, carbs: 58, fat: 7, fiber: 10 }, 1, "ah");
  }

  it("takes its turn when there are koppelingen open and no empty recipes", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);
    await seedUnmatched(store);

    const result = await runAutoIngest(env, { ...fastConfig, enrichEvery: 1 });

    expect(result.mode).toBe("enrich");
    expect(result.enriched).toBe(1);
    expect(await store.getMatch("havermout")).toBeDefined();
  });

  it("still lets empty recipes win, even on enrichment's turn", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);
    await seedUnmatched(store);
    // Een titel zonder ingredienten: dat moet voorrang houden op koppelingen.
    await store.putRecipe({
      id: "R-R999",
      title: "Leeg recept",
      url: "https://www.ah.nl/allerhande/recept/R-R999",
      servings: 4,
      imageUrl: null,
      ingredients: [],
    });

    const result = await runAutoIngest(env, { ...fastConfig, enrichEvery: 1 });
    expect(result.mode).toBe("repair");
  });

  it("cools down when AH blocks a product search during enrichment", async () => {
    stubAh({ blockProducts: true });
    const env = envFor();
    const store = new Store(env.DB);
    await seedUnmatched(store);

    const result = await runAutoIngest(env, { ...fastConfig, enrichEvery: 1 });

    expect(result.mode).toBe("enrich");
    expect(result.blocked).toBeGreaterThan(0);
    expect(result.cooldownUntil).toBeGreaterThan(Date.now());
  });
});

describe("autoStatus", () => {
  it("reports what came in today and what is still open", async () => {
    stubAh();
    const env = envFor();
    await runAutoIngest(env, fastConfig);

    const status = await autoStatus(env, fastConfig);
    expect(status.vandaag.runs).toBe(1);
    expect(status.vandaag.added).toBeGreaterThan(0);
    expect(status.rondes).toHaveLength(1);
    expect(status.afkoelenTot).toBeNull();
    expect(status.dagbudget).toBe(fastConfig.dailyMax);
  });
});

describe("configFrom", () => {
  it("reads the worker vars", () => {
    const config = configFrom({ AUTO_BATCH: "5", AUTO_DAILY_MAX: "100" });
    expect(config.batch).toBe(5);
    expect(config.dailyMax).toBe(100);
  });

  it("falls back to the defaults for missing or nonsense values", () => {
    const config = configFrom({ AUTO_BATCH: "nul", AUTO_DAILY_MAX: "-3" });
    expect(config.batch).toBe(DEFAULT_AUTO_CONFIG.batch);
    expect(config.dailyMax).toBe(DEFAULT_AUTO_CONFIG.dailyMax);
    expect(configFrom({}).minIntervalMs).toBe(DEFAULT_AUTO_CONFIG.minIntervalMs);
  });
});

describe("opruimen", () => {
  it("gooit elke ronde de recepten weg waar niets bruikbaars in staat", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    // Alles opgezocht, niets gematcht: hier komt nooit meer een echt cijfer uit.
    await store.putRecipe({
      id: "R-DOOD",
      title: "Doodlopend recept",
      url: "https://www.ah.nl/allerhande/recept/R-DOOD",
      servings: 2,
      imageUrl: null,
      ingredients: [{ name: "onvindbaar", quantity: 100, unit: "g" }],
    });
    await store.putNutrition("R-DOOD", { kcal: 400 }, 1, "ah");
    await store.putMatch("onvindbaar", null, 0);

    const result = await runAutoIngest(env, { ...fastConfig, purgeGraceMs: 0 });

    expect(result.purged).toBe(1);
    expect(await store.getRecipe("R-DOOD")).toBeNull();
  });

  it("laat een vers binnengehaald recept met rust tot de respijtperiode om is", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);
    await store.putRecipe({
      id: "R-NIEUW",
      title: "Net binnen",
      url: "https://www.ah.nl/allerhande/recept/R-NIEUW",
      servings: 2,
      imageUrl: null,
      ingredients: [{ name: "onvindbaar", quantity: 100, unit: "g" }],
    });
    await store.putMatch("onvindbaar", null, 0);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.purged).toBe(0);
    expect(await store.getRecipe("R-NIEUW")).not.toBeNull();
  });
});

describe("verzoekbudget", () => {
  it("doet tijdens het ingesten geen enkele productlookup", async () => {
    const calls = stubAh();
    const env = envFor();

    await runAutoIngest(env, fastConfig);

    // Dit was de bug uit de log: elk ongematcht ingredient werd tijdens de
    // ingest opgezocht, waarna Cloudflare de ronde afkapte met "Too many
    // subrequests" en élk resterend recept faalde. Koppelen is werk voor de
    // enrichment-ronde, met zijn eigen budget.
    expect(calls.filter((c) => c.includes("/product/"))).toHaveLength(0);
  });

  it("stopt vóór het budget op is in plaats van erdoorheen te lopen", async () => {
    const calls = stubAh();
    const env = envFor();

    // Vier verzoeken is genoeg voor de zoekpagina en één recept, niet voor twee.
    await runAutoIngest(env, { ...fastConfig, maxRequests: 3, batch: 10 });

    expect(calls.filter((c) => c.includes("ah.nl")).length).toBeLessThanOrEqual(3);
    const logs = await new Store(env.DB).recentLogs(50);
    expect(logs.some((l) => l.message.includes("recepten toegevoegd"))).toBe(true);
    // Geen "Too many subrequests"-achtige foutregels: we stopten zelf op tijd.
    expect(logs.filter((l) => l.level === "error")).toHaveLength(0);
  });

  it("rekent opnieuw door zodra een ingredient alsnog gekoppeld is", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    await store.putRecipe({
      id: "R-R900",
      title: "Havermout",
      url: "https://www.ah.nl/allerhande/recept/R-R900",
      servings: 1,
      imageUrl: null,
      ingredients: [{ name: "havermout", quantity: 100, unit: "g" }],
    });
    await store.putNutrition("R-R900", {}, 0);
    // Alsof de enrichment-ronde hem net gevonden heeft.
    await store.putProduct({
      webshopId: "wi-1",
      title: "AH Havermout",
      salesUnitSize: "500 g",
      per100g: { kcal: 375, protein: 13 },
    });
    await store.putMatch("havermout", "wi-1", 1);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.recomputed).toBe(1);
    const [row] = await store.listRecipes({ query: "Havermout" }).then((r) => r.rows);
    expect(row!.nutrition!.kcal).toBeCloseTo(375);
    expect(row!.coverage).toBe(1);
  });
});
