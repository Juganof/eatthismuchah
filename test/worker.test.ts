import { afterAll, describe, expect, it } from "vitest";
import worker, { type Env } from "../src/index";
import type { Plan } from "../src/optimize/plan";
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

describe("porties per maaltijd", () => {
  const post = async (path: string, body: unknown) => {
    const response = await fetchWorker(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return response.json();
  };

  it("schaalt een slot-plan mee met portions en klemt op 1..10", async () => {
    const { Store } = await import("../src/db/queries");
    const { FOODS, seedRecipes } = await import("./helpers/seed");
    await seedRecipes(new Store(db), [
      {
        id: "R-R800",
        title: "Kip met rijst",
        servings: 2,
        ingredients: [
          { name: "kipfilet", grams: 300, per100g: FOODS.kipfilet! },
          { name: "rijst", grams: 200, per100g: FOODS.rijst! },
        ],
      },
    ]);

    const targets = { kcal: 600, protein: 50, carbs: 70, fat: 20, fiber: 10 };
    const slot = (portions: number) =>
      post("/api/day/slot", { targets, portions }) as Promise<{
        plan: {
          portions: number;
          recipeId: string;
          totals: { kcal: number };
          perPortion: { kcal: number };
          ingredients: { name: string; grams: number }[];
        };
      }>;

    const een = await slot(1);
    const twee = await slot(2);
    const nul = await slot(0);
    const elf = await slot(11);

    expect(een.plan.portions).toBe(1);
    expect(twee.plan.portions).toBe(2);
    // Onzin wordt geklemd: 0 porties is er minimaal 1, 11 is maximaal 10.
    expect(nul.plan.portions).toBe(1);
    expect(elf.plan.portions).toBe(10);

    // Zelfde recept (deterministisch): twee porties is twee keer de hoeveelheid.
    expect(twee.plan.recipeId).toBe(een.plan.recipeId);
    expect(twee.plan.totals.kcal).toBeCloseTo(een.plan.totals.kcal * 2, 5);
    // De solver blijft per portie mikken, ongeacht het aantal porties.
    expect(twee.plan.perPortion.kcal).toBeCloseTo(een.plan.perPortion.kcal, 5);
    // En elk ingredient verdubbelt mee (welk recept er ook gekozen is). Grams
    // worden per plan op 0.1 g afgerond, vandaar de ruime marge.
    expect(twee.plan.ingredients.length).toBeGreaterThan(0);
    for (let i = 0; i < twee.plan.ingredients.length; i++) {
      expect(twee.plan.ingredients[i]!.grams).toBeCloseTo(een.plan.ingredients[i]!.grams * 2, 0);
    }
  });

  it("telt de verdubbelde grams van een opgeslagen dag met 2 porties op in de boodschappenlijst", async () => {
    const { Store } = await import("../src/db/queries");
    const { FOODS, seedRecipes } = await import("./helpers/seed");
    await seedRecipes(new Store(db), [
      {
        id: "R-R801",
        title: "Zalm met rijst",
        servings: 1,
        ingredients: [
          { name: "zalm", grams: 180, per100g: FOODS.zalm! },
          { name: "rijst", grams: 90, per100g: FOODS.rijst! },
        ],
      },
    ]);

    const targets = { kcal: 550, protein: 40, carbs: 60, fat: 20, fiber: 6 };
    const een = (await post("/api/day/slot", { targets })) as { plan: Plan };
    const twee = (await post("/api/day/slot", { targets, portions: 2 })) as {
      plan: Plan;
    };

    const dag = {
      date: "2026-08-01",
      targets,
      totals: twee.plan.totals,
      meals: [
        { slotId: "diner", slotName: "Diner", position: 0, targets, slotTags: [], plan: twee.plan },
      ],
    };
    const saved = (await post("/api/day/save", { day: dag })) as {
      ok: boolean;
      id: string;
    };
    expect(saved.ok).toBe(true);

    const shop = (await (
      await fetchWorker("/api/shopping?dayId=" + encodeURIComponent(saved.id))
    ).json()) as { days: number; lines: { name: string; grams: number }[] };
    expect(shop.days).toBe(1);
    const zalm = shop.lines.find((l) => l.name === "zalm")!;
    const zalmEen = een.plan.ingredients.find((i) => i.name === "zalm")!;
    // Het plan draagt de grams al × porties; de lijst telt alleen op.
    expect(zalm.grams).toBeCloseTo(zalmEen.grams * 2, 0);
  });
});

describe("maxKcal per eetmoment", () => {
  // Zoals de UI hem na de fix verstuurt: een hard maximum op het tussendoortje,
  // geen limiet op de rest (leeg veld = null).
  const slots = [
    { id: "tussendoortje", name: "Tussendoortje", position: 0, kcalShare: 0.1, proteinShare: null, enabled: true, tags: ["snack"], maxKcal: 250 },
    { id: "ontbijt", name: "Ontbijt", position: 1, kcalShare: 0.25, proteinShare: null, enabled: true, tags: [], maxKcal: null },
    { id: "lunch", name: "Lunch", position: 2, kcalShare: 0.3, proteinShare: null, enabled: true, tags: [], maxKcal: null },
    { id: "diner", name: "Diner", position: 3, kcalShare: 0.35, proteinShare: null, enabled: true, tags: [], maxKcal: null },
  ];

  const putSlots = () =>
    fetchWorker("/api/slots", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ slots }),
    });

  it("slaat maxKcal op via PUT en geeft het terug via GET", async () => {
    const put = await putSlots();
    expect(put.status).toBe(200);
    const putBody = (await put.json()) as { slots: { id: string; maxKcal: number | null }[] };
    expect(putBody.slots.find((s) => s.id === "tussendoortje")!.maxKcal).toBe(250);
    expect(putBody.slots.filter((s) => s.id !== "tussendoortje").every((s) => s.maxKcal === null)).toBe(true);

    const getBody = (await (await fetchWorker("/api/slots")).json()) as {
      slots: { id: string; maxKcal: number | null }[];
    };
    expect(getBody.slots.find((s) => s.id === "tussendoortje")!.maxKcal).toBe(250);
    expect(getBody.slots.filter((s) => s.id !== "tussendoortje").every((s) => s.maxKcal === null)).toBe(true);
  });

  it("klemt een moment met maxKcal af in een gegenereerde dag (max 250 kcal)", async () => {
    const { Store } = await import("../src/db/queries");
    const { FOODS, seedRecipes } = await import("./helpers/seed");
    await seedRecipes(new Store(db), [
      {
        // Macro's exact evenredig aan het geklemde momentdoel (250 kcal, 20 g
        // eiwit, 30 g kh, 10 g vet, 4 g vezels): dan kan de solver met schaal 1
        // alles raken en is de uitkomst deterministisch exact 250.
        id: "R-R700",
        title: "Snackmix",
        servings: 1,
        ingredients: [
          { name: "snackmix", grams: 50, per100g: { kcal: 500, protein: 40, carbs: 60, fat: 20, fiber: 8 } },
        ],
      },
      {
        id: "R-R701",
        title: "Havermoutpap",
        servings: 1,
        ingredients: [{ name: "havermout", grams: 200, per100g: FOODS.havermout! }],
      },
      {
        id: "R-R702",
        title: "Kip met rijst",
        servings: 1,
        ingredients: [
          { name: "kipfilet", grams: 300, per100g: FOODS.kipfilet! },
          { name: "rijst", grams: 250, per100g: FOODS.rijst! },
        ],
      },
      {
        id: "R-R703",
        title: "Zalm met broccoli",
        servings: 1,
        ingredients: [
          { name: "zalm", grams: 350, per100g: FOODS.zalm! },
          { name: "broccoli", grams: 250, per100g: FOODS.broccoli! },
          { name: "olijfolie", grams: 10, per100g: FOODS.olijfolie! },
        ],
      },
    ]);

    // De indeling staat in de database; de dag wordt daarmee gegenereerd, zoals
    // de UI dat doet: opslaan via PUT, dan pas /api/day/generate.
    await putSlots();

    const targets = { kcal: 3000, protein: 200, carbs: 300, fat: 100, fiber: 40 };
    const day = (await (
      await fetchWorker("/api/day/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ targets, date: "2026-08-03" }),
      })
    ).json()) as {
      meals: {
        slotId: string;
        targets: { kcal: number };
        plan: { totals: { kcal: number } } | null;
      }[];
    };

    const tussendoortje = day.meals.find((m) => m.slotId === "tussendoortje")!;
    expect(tussendoortje.plan).not.toBeNull();
    // Het aandeel zou 300 kcal zijn (10% van 3000); het maximum klemt het af
    // vóórdat er een recept bij gezocht wordt.
    expect(tussendoortje.targets.kcal).toBe(250);
    expect(tussendoortje.plan!.totals.kcal).toBeLessThanOrEqual(250);
  });
});

describe("eetmoment in de dag-responsen", () => {
  const post = async (path: string, body: unknown) => {
    const response = await fetchWorker(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return response.json();
  };

  it("levert het moment mee in de opties en het plan van /api/day/slot", async () => {
    const { Store } = await import("../src/db/queries");
    const { seedRecipes } = await import("./helpers/seed");
    await seedRecipes(new Store(db), [
      {
        id: "R-R850",
        title: "Tofu met paprika",
        keywords: ["lunch"],
        servings: 1,
        ingredients: [
          { name: "tofu", grams: 100, per100g: { kcal: 300, protein: 40, carbs: 20, fat: 10, fiber: 5 } },
        ],
      },
    ]);

    const body = (await post("/api/day/slot", {
      targets: { kcal: 300, protein: 40, carbs: 20, fat: 10, fiber: 5 },
      optionCount: 4,
    })) as {
      plan: { recipeId: string; moment: string | null };
      options: { plan: { recipeId: string; moment: string | null } }[];
    };

    // Het recept past exact bij het doel, dus het is de eerste optie én het plan.
    expect(body.plan.recipeId).toBe("R-R850");
    expect(body.plan.moment).toBe("lunch");
    expect(body.options[0]!.plan.moment).toBe("lunch");
    // Elke optie draagt een geldig moment; een recept zonder label mag null zijn.
    for (const option of body.options) {
      expect(
        option.plan.moment === null ||
          ["ontbijt", "lunch", "snack", "diner"].includes(option.plan.moment),
      ).toBe(true);
    }
  });

  it("levert het moment mee in elk plan van /api/day/generate", async () => {
    const day = (await (
      await fetchWorker("/api/day/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          targets: { kcal: 3000, protein: 200, carbs: 300, fat: 100, fiber: 40 },
          date: "2026-08-04",
        }),
      })
    ).json()) as {
      meals: { plan: { moment: string | null } | null }[];
    };

    for (const meal of day.meals) {
      if (!meal.plan) continue;
      // De recepten in deze database dragen allemaal een momentlabel; een plan
      // zonder label (null) zou hier ook legitiem zijn.
      const moment = meal.plan.moment;
      expect(moment === null || ["ontbijt", "lunch", "snack", "diner"].includes(moment)).toBe(true);
    }
  });
});
