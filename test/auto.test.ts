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
function stubAh(options: { keywords?: string; blockDetails?: boolean } = {}) {
  const calls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      calls.push(url);
      if (url.includes("/recepten-zoeken") || url.includes("/service/search/recipes")) {
        return new Response(SEARCH_PAGE(["R-R101", "R-R102"]), { status: 200 });
      }
      if (url.includes("/allerhande/recept/")) {
        if (options.blockDetails) return new Response("Access Denied", { status: 403 });
        const id = url.match(/recept\/(R-R\d+)/)?.[1] ?? "R-R1";
        return new Response(recipePage(id, options.keywords ?? "ontbijt"), { status: 200 });
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

    const seen: string[] = [];
    for (let i = 0; i < 4; i++) {
      const result = await runAutoIngest(env, fastConfig);
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
