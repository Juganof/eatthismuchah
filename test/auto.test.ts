import { afterEach, describe, expect, it, vi } from "vitest";
import { resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import {
  DEFAULT_AUTO_CONFIG,
  autoStatus,
  configFrom,
  runAutoIngest,
  setAutoPaused,
} from "../src/ingest/auto";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * De automaat draait op een cron waar niemand naar kijkt, dus het gedrag moet
 * hier vastliggen: wanneer hij wel gaat, wanneer hij zich inhoudt, en vooral dat
 * er nooit een half recept in de database belandt.
 */

const SEARCH_PAGE = (ids: string[]) =>
  `<html>${ids
    .map(
      (id) =>
        `<a data-testid="recipe-card" href="https://www.ah.nl/allerhande/recept/${id}/iets" title="Recept: ${id}"></a>`,
    )
    .join("")}</html>`;

/** Producten die "AH" kent, per zoekterm. */
const PRODUCTS: Record<string, { id: number; title: string; kcal: number; eiwit: number }> = {
  havermout: { id: 11, title: "AH Havermout", kcal: 375, eiwit: 13 },
  kwark: { id: 12, title: "AH Magere kwark", kcal: 57, eiwit: 10 },
};

const nutritionPage = (kcal: number, eiwit: number) =>
  `<html><table data-testid="nutrition-table">
     <tr><td>Energie</td><td>${kcal} kcal</td></tr>
     <tr><td>Eiwitten</td><td>${eiwit} g</td></tr>
   </table></html>`;

const recipePage = (
  id: string,
  keywords: string,
  ingredients: string[],
  nutrition?: Record<string, string>,
) =>
  `<html><script type="application/ld+json">${JSON.stringify({
    "@type": "Recipe",
    name: "Testrecept " + id,
    url: `https://www.ah.nl/allerhande/recept/${id}/iets`,
    recipeYield: "2",
    keywords,
    recipeIngredient: ingredients,
    ...(nutrition ? { nutrition } : {}),
  })}</script></html>`;

interface StubOptions {
  keywords?: string;
  blockDetails?: boolean;
  blockProducts?: boolean;
  /** Ingredientregels van elk recept; standaard twee die een product hebben. */
  ingredients?: string[];
  /** Welke recepten de zoekpagina teruggeeft. */
  ids?: string[];
  /** De voedingswaarde per portie die AH zelf op de receptpagina zet. */
  nutrition?: Record<string, string>;
}

/** Doet alsof ah.nl antwoordt: zoeken, recepten en producten, zonder netwerk. */
function stubAh(options: StubOptions = {}) {
  const calls: string[] = [];
  const ingredients = options.ingredients ?? ["100 g havermout", "200 g kwark"];

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      calls.push(url);

      if (url.includes("/mobile-auth/")) {
        return new Response(JSON.stringify({ access_token: "test-token" }), { status: 200 });
      }
      // Zoals in productie: de JSON-dienst bestaat niet meer en geeft een
      // HTML-foutpagina terug, de gewone zoekpagina werkt wel.
      if (url.includes("/service/search/recipes")) {
        return new Response("<html>Not found</html>", { status: 404 });
      }
      if (url.includes("/recepten-zoeken")) {
        return new Response(SEARCH_PAGE(options.ids ?? ["R-R101", "R-R102"]), { status: 200 });
      }
      if (url.includes("/allerhande/recept/")) {
        if (options.blockDetails) return new Response("Access Denied", { status: 403 });
        const id = url.match(/recept\/(R-R\d+)/)?.[1] ?? "R-R1";
        return new Response(
          recipePage(id, options.keywords ?? "ontbijt", ingredients, options.nutrition),
          { status: 200 },
        );
      }
      if (url.includes("/product/search/")) {
        if (options.blockProducts) return new Response("Access Denied", { status: 403 });
        const query = new URL(url).searchParams.get("query") ?? "";
        const hit = Object.entries(PRODUCTS).find(([name]) => query.includes(name));
        const products = hit ? [{ webshopId: hit[1].id, title: hit[1].title }] : [];
        return new Response(JSON.stringify({ products }), { status: 200 });
      }
      if (url.includes("/producten/product/wi")) {
        const id = Number(url.match(/wi(\d+)/)?.[1]);
        const product = Object.values(PRODUCTS).find((p) => p.id === id);
        if (!product) return new Response("<html></html>", { status: 200 });
        return new Response(nutritionPage(product.kcal, product.eiwit), { status: 200 });
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
  resetEndpointState();
});

function envFor(): { DB: TestDb; AH_USER_AGENT: string } {
  db = createTestDb();
  return { DB: db, AH_USER_AGENT: "test" };
}

/** Zonder pauzes, anders duurt elke test seconden. */
const fastConfig = { ...DEFAULT_AUTO_CONFIG, minIntervalMs: 0, backoffMs: 0, batch: 2 };

describe("een ronde", () => {
  it("slaat alleen complete recepten op, met hun voedingswaarde", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.ran).toBe(true);
    expect(result.added).toBeGreaterThan(0);
    expect(result.rejected).toBe(0);

    // Elk opgeslagen recept heeft een echt totaal: 100 g havermout (375 kcal/100 g)
    // plus 200 g kwark (57 kcal/100 g) = 375 + 114.
    const { rows } = await store.listRecipes();
    expect(rows).toHaveLength(result.added!);
    for (const row of rows) {
      expect(row.nutrition?.kcal, row.title).toBeCloseTo(489, 0);
      expect(row.ingredientCount, row.title).toBe(2);
    }
  });

  it("keurt een recept af als een ingredient nergens cijfers oplevert", async () => {
    stubAh({ ingredients: ["100 g havermout", "2 middelgrote scharreleieren"] });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.added).toBe(0);
    expect(result.rejected).toBeGreaterThan(0);
    // Niets half opgeslagen: geen recept, geen voedingswaarde.
    expect(await store.countRecipes()).toBe(0);
    expect(await store.countSkippedRecipes()).toBe(result.rejected);
  });

  it("houdt een recept dat AH zelf doorrekent, ook zonder product bij elke regel", async () => {
    // Precies het geval waarop bijna alles sneuvelde: courgette bestaat wel bij
    // AH, maar de productpagina heeft geen voedingswaardetabel. AH zet die
    // cijfers wél op de receptpagina, en dat is genoeg.
    stubAh({
      ingredients: ["100 g havermout", "1 courgette"],
      nutrition: { calories: "300 kcal", proteinContent: "20 g" },
    });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.rejected).toBe(0);
    expect(result.added).toBeGreaterThan(0);
    // 2 porties à 300 kcal: het totaal is AH's opgave, niet de 375 van de
    // havermout alleen.
    const { rows } = await store.listRecipes();
    for (const row of rows) expect(row.nutrition?.kcal, row.title).toBeCloseTo(600, 0);
  });

  it("haalt een afgekeurd recept nooit opnieuw op", async () => {
    stubAh({ ingredients: ["1 onvindbaar ingredient"] });
    const env = envFor();

    await runAutoIngest(env, fastConfig);
    const calls = stubAh({ ingredients: ["1 onvindbaar ingredient"] });
    await runAutoIngest(env, fastConfig);

    // Alleen de zoekpagina; de receptpagina's zijn al beoordeeld.
    expect(calls.filter((c) => c.includes("/allerhande/recept/"))).toHaveLength(0);
  });

  it("laat water en zout een recept niet blokkeren", async () => {
    stubAh({ ingredients: ["100 g havermout", "200 ml water", "1 snuf zout"] });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.added).toBeGreaterThan(0);
    // Water en zout leveren nul, dus het totaal is dat van de havermout alleen.
    const { rows } = await store.listRecipes();
    expect(rows[0]!.nutrition?.kcal).toBeCloseTo(375, 0);
  });

  it("haalt een recept dat al compleet is niet opnieuw op", async () => {
    stubAh();
    const env = envFor();

    await runAutoIngest(env, fastConfig);
    const calls = stubAh();
    await runAutoIngest(env, fastConfig);

    expect(calls.filter((c) => c.includes("/allerhande/recept/"))).toHaveLength(0);
  });

  it("legt niets vast als het verzoekbudget midden in een recept opraakt", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    // Genoeg voor de zoekpagina en de receptpagina, niet voor de producten.
    await runAutoIngest(env, { ...fastConfig, maxRequests: 2 });

    expect(await store.countRecipes()).toBe(0);
    // En niet afgekeurd: "past nu niet" is iets anders dan "kan niet".
    expect(await store.countSkippedRecipes()).toBe(0);
  });

  it("bewaart receptpagina's in het archief, maar geen productpagina's", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    await runAutoIngest(env, fastConfig);

    const { rows } = await store.listScrapes({ limit: 100 });
    const kinds = new Set(rows.map((r) => r.kind));
    expect(kinds).toContain("recipe");
    // Productpagina's zijn 500-700 kB per stuk en leveren alleen voedingswaarde.
    expect(kinds).not.toContain("product");
  });

  it("houdt de rondes bij, zodat je achteraf ziet wat er gebeurde", async () => {
    stubAh();
    const env = envFor();

    await runAutoIngest(env, fastConfig);

    const runs = await new Store(env.DB).recentRuns(5);
    expect(runs).toHaveLength(1);
    expect(runs[0]!["mode"]).toBe("scrape");
    expect(runs[0]!["finished_at"]).toBeTruthy();
  });

  it("rouleert over de eetmomenten in plaats van op één term te blijven hangen", async () => {
    stubAh();
    const env = envFor();

    const seen: string[] = [];
    for (let i = 0; i < 4; i++) {
      const result = await runAutoIngest(env, fastConfig);
      seen.push((result.detail ?? "").split(":")[0]!);
    }

    expect(seen).toEqual(["ontbijt", "lunch", "snack", "diner"]);
  });
});

describe("zich inhouden", () => {
  it("koelt af na twee geblokkeerde rondes op rij, en slaat de ronde daarna over", async () => {
    stubAh({ blockDetails: true });
    const env = envFor();

    // Eén geblokkeerde ronde is nog ruis; pas de tweede op rij is een blokkade.
    const first = await runAutoIngest(env, fastConfig);
    expect(first.blocked).toBeGreaterThan(0);
    expect(first.cooldownUntil).toBeUndefined();

    const second = await runAutoIngest(env, fastConfig);
    expect(second.cooldownUntil).toBeGreaterThan(Date.now());

    const third = await runAutoIngest(env, fastConfig);
    expect(third.ran).toBe(false);
    expect(third.reason).toContain("afkoelen");
  });

  it("verdubbelt de afkoelperiode bij elke volgende blokkade, tot het maximum", async () => {
    const env = envFor();
    // cooldownAfterBlocks 1: deze test gaat over hoe lang er afgekoeld wordt,
    // niet over vanaf hoeveel rondes dat begint.
    const config = {
      ...fastConfig,
      cooldownMs: 1000,
      maxCooldownMs: 3000,
      cooldownAfterBlocks: 1,
    };

    stubAh({ blockDetails: true });
    const first = await runAutoIngest(env, config, { force: true });
    expect(first.cooldownUntil! - Date.now()).toBeCloseTo(1000, -2);

    stubAh({ blockDetails: true });
    const second = await runAutoIngest(env, config, { force: true });
    expect(second.cooldownUntil! - Date.now()).toBeCloseTo(2000, -2);

    stubAh({ blockDetails: true });
    const third = await runAutoIngest(env, config, { force: true });
    expect(third.cooldownUntil! - Date.now()).toBeCloseTo(3000, -2);

    // Een schone ronde zet de teller terug op nul.
    stubAh();
    await runAutoIngest(env, config, { force: true });

    // Andere recepten, anders valt er niets meer op te halen en dus ook niets te
    // blokkeren: de vorige ronde heeft R-R101 en R-R102 al compleet opgeslagen.
    stubAh({ blockDetails: true, ids: ["R-R201", "R-R202"] });
    const afterReset = await runAutoIngest(env, config, { force: true });
    expect(afterReset.cooldownUntil! - Date.now()).toBeCloseTo(1000, -2);
  });

  it("draait wel als je hem forceert, zonder de afkoelperiode uit te zitten", async () => {
    stubAh({ blockDetails: true });
    const env = envFor();
    await runAutoIngest(env, fastConfig);
    await runAutoIngest(env, fastConfig);

    const forced = await runAutoIngest(env, fastConfig, { force: true });
    expect(forced.ran).toBe(true);
  });

  it("stopt bij het dagbudget", async () => {
    stubAh();
    const env = envFor();

    await runAutoIngest(env, { ...fastConfig, dailyMax: 1 });
    const second = await runAutoIngest(env, { ...fastConfig, dailyMax: 1 });

    expect(second.ran).toBe(false);
    expect(second.reason).toContain("dagbudget");
  });

  it("doet niets zolang het bijvullen uitstaat", async () => {
    stubAh();
    const env = envFor();
    await setAutoPaused(env, true);

    const result = await runAutoIngest(env, fastConfig);

    expect(result.ran).toBe(false);
    expect(result.reason).toContain("staat uit");
    expect((await autoStatus(env, fastConfig)).gepauzeerd).toBe(true);

    await setAutoPaused(env, false);
    expect((await runAutoIngest(env, fastConfig)).ran).toBe(true);
  });
});

describe("de dode JSON-zoekdienst", () => {
  it("wordt na één 404 niet meer geprobeerd, ook niet in een volgende ronde", async () => {
    const calls = stubAh();
    const env = envFor();

    await runAutoIngest(env, fastConfig);
    expect(calls.filter((c) => c.includes("/service/search/recipes")).length).toBeGreaterThan(0);

    // Nieuwe ronde = nieuw isolate in productie, dus dit moet uit de database
    // komen en niet uit een variabele die de ronde niet overleeft.
    resetEndpointState();
    calls.length = 0;
    await runAutoIngest(env, fastConfig);

    expect(calls.filter((c) => c.includes("/service/search/recipes"))).toHaveLength(0);
  });
});

describe("de eenmalige opruiming", () => {
  it("gooit recepten uit de oude opzet weg, en doet dat maar één keer", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);
    // Zoals de vorige opzet ze achterliet: wel een titel, geen voedingswaarde.
    await store.putRecipe({
      id: "R-OUD",
      title: "Half recept van vroeger",
      url: "https://www.ah.nl/allerhande/recept/R-OUD",
      servings: 2,
      imageUrl: null,
      ingredients: [{ name: "havermout", quantity: 100, unit: "g" }],
    });

    await runAutoIngest(env, fastConfig);
    expect(await store.getRecipe("R-OUD")).toBeNull();

    // Tweede keer: de vlag staat, dus de opruiming raakt niets meer aan — ook
    // niet de complete recepten die deze ronde net binnenkwamen.
    const voor = await store.countRecipes();
    await runAutoIngest(env, fastConfig);
    expect(await store.countRecipes()).toBeGreaterThanOrEqual(voor);
  });
});

describe("autoStatus", () => {
  it("vertelt wat er vandaag binnenkwam en wat er klaarstaat", async () => {
    stubAh();
    const env = envFor();
    await runAutoIngest(env, fastConfig);

    const status = await autoStatus(env, fastConfig);
    expect(status.vandaag.runs).toBe(1);
    expect(status.vandaag.added).toBeGreaterThan(0);
    expect(status.recepten).toBeGreaterThan(0);
    expect(status.volgende).toBe("lunch");
  });
});

describe("configFrom", () => {
  it("leest instellingen uit de omgeving en negeert onzin", async () => {
    const config = configFrom({ AUTO_BATCH: "5", AUTO_DAILY_MAX: "nonsens" });
    expect(config.batch).toBe(5);
    expect(config.dailyMax).toBe(DEFAULT_AUTO_CONFIG.dailyMax);
  });
});

describe("na een budgetstop", () => {
  it("maakt het recept de volgende ronde alsnog af", async () => {
    stubAh();
    const env = envFor();
    const store = new Store(env.DB);

    // Ronde 1 komt niet verder dan de receptpagina.
    await runAutoIngest(env, { ...fastConfig, maxRequests: 2 });
    expect(await store.countRecipes()).toBe(0);

    // Ronde 2 heeft ruimte genoeg en maakt hetzelfde recept alsnog compleet.
    await runAutoIngest(env, fastConfig);
    expect(await store.countRecipes()).toBeGreaterThan(0);
  });
});

describe("een fout is geen oordeel over het recept", () => {
  it("keurt niets af als AH de productzoekopdracht blokkeert", async () => {
    stubAh({ blockProducts: true });
    const env = envFor();
    const store = new Store(env.DB);

    const result = await runAutoIngest(env, fastConfig);

    // Dit was de bug: een 403 kwam als "geen product" terug, waarna een prima
    // recept voorgoed werd afgekeurd. Een blokkade is "later nog eens".
    expect(result.rejected).toBe(0);
    expect(await store.countSkippedRecipes()).toBe(0);
    expect(result.blocked).toBeGreaterThan(0);
  });

  it("probeert een recept opnieuw zodra de blokkade voorbij is", async () => {
    stubAh({ blockProducts: true });
    const env = envFor();
    const store = new Store(env.DB);
    await runAutoIngest(env, fastConfig);

    stubAh();
    await runAutoIngest(env, fastConfig, { force: true });

    expect(await store.countRecipes()).toBeGreaterThan(0);
  });
});

describe("een afkeuring verloopt", () => {
  it("probeert het na twee weken opnieuw", async () => {
    stubAh({ ingredients: ["1 onvindbaar ingredient"] });
    const env = envFor();
    const store = new Store(env.DB);
    await runAutoIngest(env, fastConfig);
    expect(await store.countSkippedRecipes()).toBeGreaterThan(0);

    // Verse afkeuring: niet opnieuw ophalen.
    let calls = stubAh({ ingredients: ["1 onvindbaar ingredient"] });
    await runAutoIngest(env, fastConfig);
    expect(calls.filter((c) => c.includes("/allerhande/recept/"))).toHaveLength(0);

    // Twee weken later wél: misschien vindt de matcher het inmiddels wel.
    await db!
      .prepare("UPDATE skipped_recipes SET at = ?")
      .bind(Date.now() - 15 * 24 * 60 * 60 * 1000)
      .run();
    calls = stubAh();
    await runAutoIngest(env, fastConfig);

    expect(calls.filter((c) => c.includes("/allerhande/recept/")).length).toBeGreaterThan(0);
    expect(await store.countRecipes()).toBeGreaterThan(0);
  });
});
