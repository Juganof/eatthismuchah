import { afterAll, describe, expect, it } from "vitest";
import worker, { type Env } from "../src/index";
import { createTestDb } from "./helpers/d1";

const db = createTestDb();
afterAll(() => db.close());

const env: Env = {
  DB: db,
  AH_USER_AGENT: "test",
  INGEST_QUERIES: "kip",
  INGEST_LIMIT: "1",
};

const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as unknown as ExecutionContext;

const fetchWorker = (path: string, init?: RequestInit) =>
  worker.fetch(new Request(`https://worker.test${path}`, init), env, ctx);

describe("Worker routes", () => {
  it("renders the planner UI", async () => {
    const response = await fetchWorker("/");
    const html = await response.text();
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/html");
    expect(html).toContain("AH Macro Planner");
    // De vier tabbladen en de endpoints waar ze op leunen zitten in één document,
    // dus als er eentje uit de bundel valt, valt deze test om.
    for (const tab of ["panel-profiel", "panel-momenten", "panel-dag", "panel-week"]) {
      expect(html, `tab ${tab} ontbreekt`).toContain(tab);
    }
    for (const route of [
      "/api/profile",
      "/api/slots",
      "/api/day/generate",
      "/api/day/slot",
      "/api/day/reroll",
      "/api/shopping",
      "/api/products/search",
      "/api/logs",
      "/api/wipe",
    ]) {
      expect(html, `${route} wordt niet aangeroepen`).toContain(route);
    }
    // Het receptvenster toont per ingredient het echte AH-product (link, verpakking,
    // voedingswaarde per 100 g) en markeert regels zonder product als schatting.
    expect(html, "label voor geschatte regel ontbreekt").toContain("(geschat)");
    expect(html, "kcal per 100 g uit het productlabel ontbreekt").toContain(" kcal / 100 g)");
  });

  it("reports empty database statistics", async () => {
    const response = await fetchWorker("/api/stats");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      recipes: 0,
      plannable: 0,
      afgekeurd: 0,
      scrapes: 0,
      unparsed: 0,
    });
  });

  it("validates search, plan and match requests", async () => {
    const search = await fetchWorker("/api/search");
    expect(search.status).toBe(400);

    const plan = await fetchWorker("/api/plan", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(plan.status).toBe(400);

    const match = await fetchWorker("/api/match", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(match.status).toBe(400);
  });

  it("answers a database failure with readable JSON, not a bare 500", async () => {
    // Dit is wat er live gebeurde toen de migratie nog niet gedraaid was: D1
    // gooide op een ontbrekende kolom, Hono antwoordde met platte tekst, en de
    // UI kon daar alleen "geen geldige respons" van maken.
    const brokenDb = {
      prepare: () => {
        throw new Error("D1_ERROR: no such column: first_seen_at");
      },
    } as unknown as D1Database;

    const response = await worker.fetch(
      new Request("https://worker.test/api/stats"),
      { ...env, DB: brokenDb },
      ctx,
    );

    expect(response.status).toBe(500);
    expect(response.headers.get("content-type")).toContain("application/json");
    const body = (await response.json()) as { error: string };
    expect(body.error).toContain("no such column");
    // en de melding wijst naar de oplossing
    expect(body.error).toContain("db:migrate");
  });

  it("explains that ingest is required before generation", async () => {
    const response = await fetchWorker("/api/generate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ protein: 60, kcal: 700 }),
    });
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ plans: [] });
  });
});

describe("log en wissen", () => {
  it("geeft de log als platte tekst terug, klaar om te kopiëren", async () => {
    const { Store } = await import("../src/db/queries");
    const store = new Store(db);
    await store.log("error", "ingest", "recept R-R9 mislukt", { fout: "GET -> 403" });

    const response = await fetchWorker("/api/logs?format=text&limit=10");
    const text = await response.text();

    expect(response.status).toBe(200);
    expect(text).toContain("ERROR");
    expect(text).toContain("recept R-R9 mislukt");
    expect(text).toContain("GET -> 403");
  });

  it("filtert op niveau", async () => {
    const response = await fetchWorker("/api/logs?level=info&limit=10");
    const body = (await response.json()) as { rows: { level: string }[] };
    expect(body.rows.every((r) => r.level === "info")).toBe(true);
  });

  it("wist de scrape-data en logt dat als eerste nieuwe regel", async () => {
    const response = await fetchWorker("/api/wipe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const body = (await response.json()) as { scope: string; total: number };

    expect(response.status).toBe(200);
    expect(body.scope).toBe("scrape");

    const logs = (await (await fetchWorker("/api/logs?limit=5")).json()) as {
      rows: { message: string }[];
    };
    expect(logs.rows[0]!.message).toContain("database gewist");
  });
});

describe("/api/recipe/:id", () => {
  it("laat de ingredienten zien met hun aandeel in de voedingswaarde", async () => {
    const { Store } = await import("../src/db/queries");
    const { FOODS, seedRecipes } = await import("./helpers/seed");
    await seedRecipes(new Store(db), [
      {
        id: "R-R900",
        title: "Kwark met havermout",
        servings: 2,
        ingredients: [
          { name: "kwark", grams: 300, per100g: FOODS.kwark! },
          { name: "havermout", grams: 80, per100g: FOODS.havermout! },
        ],
      },
    ]);

    const body = (await (await fetchWorker("/api/recipe/R-R900")).json()) as {
      servings: number;
      perPortion: { kcal: number };
      total: { kcal: number };
      nutritionSource: string;
      ingredients: { name: string; nutrients: { kcal?: number }; nutrientSource: string }[];
    };

    expect(body.servings).toBe(2);
    expect(body.ingredients.map((i) => i.name)).toEqual(["kwark", "havermout"]);
    // Elke regel krijgt zijn aandeel naar gewicht, en dat telt op tot het geheel.
    const som = body.ingredients.reduce((sum, i) => sum + (i.nutrients.kcal ?? 0), 0);
    expect(som).toBeCloseTo(body.total.kcal, 5);
    expect(body.ingredients[0]!.nutrientSource).toBe("geschat");
    expect(body.nutritionSource).toBe("ah");
    // Per portie is de helft van het hele recept.
    expect(body.perPortion.kcal).toBeCloseTo(body.total.kcal / 2, 5);
  });

  it("koppelt een ingredient aan het echte AH-product met voedingswaarden", async () => {
    const { Store } = await import("../src/db/queries");
    const { FOODS, seedRecipes } = await import("./helpers/seed");
    const store = new Store(db);
    await seedRecipes(store, [
      {
        id: "R-R901",
        title: "Kwark met havermout",
        servings: 1,
        ingredients: [
          { name: "kwark", grams: 300, per100g: FOODS.kwark! },
          { name: "havermout", grams: 80, per100g: FOODS.havermout! },
        ],
      },
    ]);
    // Eén ingredient heeft een onthouden productkoppeling, de ander niet.
    await store.putProduct({
      webshopId: "123456",
      title: "AH Magere kwark",
      salesUnitSize: "500 g",
      per100g: FOODS.kwark!,
    });
    await store.putMatch("kwark", "123456", 1);

    const body = (await (await fetchWorker("/api/recipe/R-R901")).json()) as {
      ingredients: {
        name: string;
        product: string | null;
        productUrl: string | null;
        productSize: string | null;
        per100g: { kcal: number; protein: number } | null;
        nutrientSource: string;
      }[];
    };

    const kwark = body.ingredients[0]!;
    expect(kwark.name).toBe("kwark");
    expect(kwark.product).toBe("AH Magere kwark");
    expect(kwark.productUrl).toBe("https://www.ah.nl/producten/product/wi123456");
    expect(kwark.productSize).toBe("500 g");
    // De voedingswaarden van het productlabel zelf, ongeacht de hoeveelheid.
    expect(kwark.per100g).toEqual({ kcal: 57, protein: 10, carbs: 4, fat: 0.2, fiber: 0 });
    expect(kwark.nutrientSource).toBe("product");

    // Zonder match blijft de regel een schatting, zonder productkoppeling.
    const havermout = body.ingredients[1]!;
    expect(havermout.productUrl).toBeNull();
    expect(havermout.product).toBeNull();
    expect(havermout.per100g).toBeNull();
    expect(havermout.nutrientSource).toBe("geschat");
  });
});
