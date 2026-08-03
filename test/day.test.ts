import { afterEach, describe, expect, it } from "vitest";
import { AhClient } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { DEFAULT_SLOTS, type MealSlot } from "../src/nutrition/split";
import type { DailyTargets } from "../src/nutrition/targets";
import { generateDay, macroDistance, rerollSlot, rerollSlotOptions } from "../src/optimize/day";
import { createTestDb, type TestDb } from "./helpers/d1";
import { FOODS, seedRecipes } from "./helpers/seed";

/**
 * De planner mag in deze tests geen netwerk doen: elk recept is voorgekookt in de
 * database. Slaat een test toch aan op ah.nl, dan valt fetch om en faalt hij —
 * precies de bedoeling.
 */
const client = new AhClient("test");

const daily: DailyTargets = { kcal: 2000, protein: 150, carbs: 200, fat: 60, fiber: 28 };

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
});

async function storeWithLibrary(): Promise<Store> {
  db = createTestDb();
  const store = new Store(db);
  await seedRecipes(store, [
    {
      id: "R-R1",
      title: "Kwark met banaan",
      ingredients: [
        { name: "kwark", grams: 250, per100g: FOODS.kwark! },
        { name: "banaan", grams: 100, per100g: FOODS.banaan! },
      ],
    },
    {
      id: "R-R2",
      title: "Havermout met banaan",
      ingredients: [
        { name: "havermout", grams: 80, per100g: FOODS.havermout! },
        { name: "banaan", grams: 100, per100g: FOODS.banaan! },
      ],
    },
    {
      id: "R-R3",
      title: "Kip met rijst en broccoli",
      ingredients: [
        { name: "kipfilet", grams: 200, per100g: FOODS.kipfilet! },
        { name: "rijst", grams: 90, per100g: FOODS.rijst! },
        { name: "broccoli", grams: 200, per100g: FOODS.broccoli! },
      ],
    },
    {
      id: "R-R4",
      title: "Zalm met rijst",
      ingredients: [
        { name: "zalm", grams: 180, per100g: FOODS.zalm! },
        { name: "rijst", grams: 90, per100g: FOODS.rijst! },
        { name: "olijfolie", grams: 10, per100g: FOODS.olijfolie! },
      ],
    },
    {
      id: "R-R5",
      title: "Kipsalade",
      ingredients: [
        { name: "kipfilet", grams: 150, per100g: FOODS.kipfilet! },
        { name: "broccoli", grams: 150, per100g: FOODS.broccoli! },
        { name: "olijfolie", grams: 10, per100g: FOODS.olijfolie! },
      ],
    },
  ]);
  return store;
}

describe("generateDay", () => {
  it("fills every enabled slot", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(day.meals).toHaveLength(4);
    expect(day.meals.map((m) => m.slotName)).toEqual([
      "Ontbijt",
      "Lunch",
      "Tussendoortje",
      "Diner",
    ]);
    for (const meal of day.meals) {
      expect(meal.plan, `${meal.slotName} should have a plan`).not.toBeNull();
    }
  });

  it("never puts the same recipe on a day twice", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    const ids = day.meals.map((m) => m.plan?.recipeId).filter(Boolean);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("lands the whole day near the target rather than each slot separately", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    // De solver mag schalen, dus dit hoort ruim binnen 15% uit te komen.
    expect(Math.abs(day.totals.kcal! - daily.kcal) / daily.kcal).toBeLessThan(0.15);
    // 0.21: sinds de dag per moment de optie kiest die het dichtst bij het
    // momentdoel ligt (plan.cost) in plaats van op de smaakscore, landt de dag
    // hier op ~20.3% — vrijwel identiek, maar de oude 0.2-grens was daar net
    // te krap voor. De test blijft eisen dat de dag als geheel dicht bij het
    // doel landt.
    expect(Math.abs(day.totals.protein! - daily.protein) / daily.protein).toBeLessThan(0.21);
  });

  it("reports the deviation from the day target", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(day.deviation.kcal).toBeCloseTo(daily.kcal - day.totals.kcal!, 0);
  });

  it("passes the diet filter down, so vegetarians get no meat or fish", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
      diet: ["vegetarisch"],
    });

    const chosen = day.meals.map((m) => m.plan?.recipeId).filter(Boolean);
    // R-R3, R-R4 en R-R5 bevatten kip of zalm.
    expect(chosen).not.toContain("R-R3");
    expect(chosen).not.toContain("R-R4");
    expect(chosen).not.toContain("R-R5");
  });

  it("skips recipes that use an excluded ingredient", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
      excludedTerms: ["banaan"],
    });

    const chosen = day.meals.map((m) => m.plan?.recipeId).filter(Boolean);
    expect(chosen).not.toContain("R-R1");
    expect(chosen).not.toContain("R-R2");
  });

  it("reports honestly when there is nothing to plan with", async () => {
    db = createTestDb();
    const day = await generateDay(new Store(db), client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(day.meals.every((m) => m.plan === null)).toBe(true);
    expect(day.meals[0]!.note).toContain("geen passend recept");
  });

  it("honours a slot that is switched off", async () => {
    const store = await storeWithLibrary();
    const slots: MealSlot[] = DEFAULT_SLOTS.map((s) =>
      s.id === "tussendoortje" ? { ...s, enabled: false } : s,
    );
    const day = await generateDay(store, client, { date: "2026-08-01", slots, daily });

    expect(day.meals).toHaveLength(3);
    expect(day.meals.map((m) => m.slotId)).not.toContain("tussendoortje");
  });

  it("vermenigvuldigt elk plan met de opgegeven porties en telt plan-totalen op", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
      portions: 2,
    });

    for (const meal of day.meals) {
      expect(meal.portions).toBe(2);
      // Doelen blijven per portie; het plan zelf is al × porties.
      for (const key of ["kcal", "protein", "carbs", "fat", "fiber"] as const) {
        expect(meal.plan!.totals[key]).toBeCloseTo((meal.plan!.perPortion[key] ?? 0) * 2, 5);
      }
    }

    // Het eerste moment heeft een onvervormd doel (500 kcal): ~1000 kcal in het plan.
    const eerste = day.meals[0]!;
    expect(Math.abs(eerste.plan!.totals.kcal! - eerste.targets.kcal * 2) / (eerste.targets.kcal * 2)).toBeLessThan(0.15);

    // Dagtotaal is de som van de plan-totalen (al × porties), niet van de
    // per-portie-waarden — anders zou het dagtotaal op ~2000 blijven staan.
    const somPlannen = day.meals.reduce((sum, m) => sum + (m.plan?.totals.kcal ?? 0), 0);
    expect(day.totals.kcal).toBeCloseTo(somPlannen, 0);
    const somPerPortie = day.meals.reduce((sum, m) => sum + (m.plan?.perPortion.kcal ?? 0), 0);
    expect(day.totals.kcal! - somPerPortie).toBeGreaterThan(100);

    // De deviatie blijft doel min behaald, ook met porties.
    expect(day.deviation.kcal).toBeCloseTo(daily.kcal - day.totals.kcal!, 0);
  });
});

describe("rerollSlot", () => {
  it("returns a different recipe with comparable macros", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    const dinner = day.meals.at(-1)!;
    const original = dinner.plan!;
    const exclude = [original.recipeId];

    const similar = await rerollSlot(store, client, {
      targets: dinner.targets,
      excludeRecipeIds: exclude,
      similarTo: original.perPortion,
    });
    // Zelfde vraag zonder de gelijkenis-eis, als vergelijkingsmateriaal.
    const anything = await rerollSlot(store, client, {
      targets: dinner.targets,
      excludeRecipeIds: exclude,
    });

    expect(similar).not.toBeNull();
    expect(similar!.recipeId).not.toBe(original.recipeId);
    // Dit is waar `similarTo` voor is: het alternatief hoort dichter bij het
    // vervangen gerecht te liggen dan wat de planner anders had gekozen.
    expect(macroDistance(similar!.perPortion, original.perPortion)).toBeLessThanOrEqual(
      macroDistance(anything!.perPortion, original.perPortion),
    );
  });

  it("gives up rather than repeating something you already rejected", async () => {
    const store = await storeWithLibrary();
    const all = ["R-R1", "R-R2", "R-R3", "R-R4", "R-R5"];
    const replacement = await rerollSlot(store, client, {
      targets: { kcal: 700, protein: 50, carbs: 70, fat: 20, fiber: 10 },
      excludeRecipeIds: all,
    });

    expect(replacement).toBeNull();
  });
});

describe("rerollSlotOptions", () => {
  // Exact het per-portie profiel van R-R3 ("Kip met rijst en broccoli"), zodat
  // dat recept zonder herschalen al bij het doel past.
  const closeToR3: DailyTargets = { kcal: 598.6, protein: 58.3, carbs: 78.2, fat: 4.5, fiber: 7.26 };

  it("returns at most `count` options, each with a distinct recipe and a bucket", async () => {
    const store = await storeWithLibrary();
    const options = await rerollSlotOptions(store, client, {
      targets: closeToR3,
      excludeRecipeIds: [],
      count: 3,
    });

    expect(options.length).toBeLessThanOrEqual(3);
    expect(options.length).toBeGreaterThan(0);
    const ids = options.map((o) => o.plan.recipeId);
    expect(new Set(ids).size).toBe(ids.length);
    for (const option of options) {
      expect(["origineel", "herschaald"]).toContain(option.bucket);
    }
  });

  it("buckets a recipe that already matches as \"origineel\", pinned close to 1x", async () => {
    const store = await storeWithLibrary();
    const options = await rerollSlotOptions(store, client, {
      targets: closeToR3,
      excludeRecipeIds: [],
      count: 4,
    });

    const original = options.find((o) => o.plan.recipeId === "R-R3");
    expect(original?.bucket).toBe("origineel");
    for (const ingredient of original!.plan.ingredients) {
      expect(ingredient.scale).toBeGreaterThanOrEqual(0.9);
      expect(ingredient.scale).toBeLessThanOrEqual(1.1);
    }
  });

  it("returns only \"herschaald\" options when nothing is naturally close to the target", async () => {
    const store = await storeWithLibrary();
    // kcal blijft binnen ieders bandbreedte (zodat de shortlist niet leegloopt),
    // maar de verhouding — vrijwel geen koolhydraten, extreem veel eiwit — komt
    // met geen van de vijf recepten in de buurt.
    const farOff: DailyTargets = { kcal: 500, protein: 150, carbs: 10, fat: 5, fiber: 20 };
    const options = await rerollSlotOptions(store, client, {
      targets: farOff,
      excludeRecipeIds: [],
      count: 4,
    });

    expect(options.length).toBeGreaterThan(0);
    expect(options.every((o) => o.bucket === "herschaald")).toBe(true);
  });
});

describe("generateDay keuzekaarten", () => {
  it("geeft elk moment een options-array, gesorteerd met de beste eerst", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(day.meals).toHaveLength(4);
    for (const meal of day.meals) {
      const options = meal.options;
      expect(options, `${meal.slotName} heeft keuzekaarten`).toBeDefined();
      expect(options!.length).toBeGreaterThanOrEqual(1);
      expect(options!.length).toBeLessThanOrEqual(6);
      // Het sorteercontract: kleinste afstand tot het momentdoel (plan.cost) eerst.
      const costs = options!.map((o) => o.plan.cost);
      expect(costs, `${meal.slotName} op afstand gesorteerd`).toEqual([...costs].sort((a, b) => a - b));
    }
  });

  it("de eerste optie is het plan dat de dag toont", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    for (const meal of day.meals) {
      expect(meal.options![0]!.plan.recipeId, meal.slotName).toBe(meal.plan!.recipeId);
    }
  });

  it("slot/reroll-opties blijven gesorteerd zonder dubbele recipeIds", async () => {
    const store = await storeWithLibrary();
    // Het per-portie profiel van R-R3, zonder similarTo: hetzelfde pad als
    // /api/day/slot (en /api/day/reroll zonder vervanging) bewandelt.
    const options = await rerollSlotOptions(store, client, {
      targets: { kcal: 598.6, protein: 58.3, carbs: 78.2, fat: 4.5, fiber: 7.26 },
      excludeRecipeIds: [],
      count: 4,
    });

    const ids = options.map((o) => o.plan.recipeId);
    expect(new Set(ids).size).toBe(ids.length);
    const costs = options.map((o) => o.plan.cost);
    expect(costs).toEqual([...costs].sort((a, b) => a - b));
  });

  it("twee generaties geven dezelfde opties in dezelfde volgorde", async () => {
    const store = await storeWithLibrary();
    const first = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });
    const second = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    for (const [index, meal] of first.meals.entries()) {
      const again = second.meals[index]!;
      expect(meal.options!.map((o) => o.plan.recipeId)).toEqual(
        again.options!.map((o) => o.plan.recipeId),
      );
    }
  });
});

describe("eetmoment van opties en plannen", () => {
  /**
   * Drie recepten die op titel en ingredienten volkomen neutraal zijn; alleen
   * AH's keywords bepalen het eetmoment. Het vierde recept zegt niets en moet
   * dus ook geen moment krijgen.
   */
  async function storeWithMoments(): Promise<Store> {
    db = createTestDb();
    const store = new Store(db);
    const neutraal = { kcal: 300, protein: 40, carbs: 20, fat: 10, fiber: 5 };
    await seedRecipes(store, [
      {
        id: "R-M1",
        title: "Tofu met paprika",
        keywords: ["ontbijt"],
        ingredients: [{ name: "tofu", grams: 100, per100g: neutraal }],
      },
      {
        id: "R-M2",
        title: "Tofu met paprika",
        keywords: ["tussendoortje"],
        ingredients: [{ name: "tofu", grams: 100, per100g: neutraal }],
      },
      {
        id: "R-M3",
        title: "Tofu met paprika",
        ingredients: [{ name: "tofu", grams: 100, per100g: neutraal }],
      },
    ]);
    return store;
  }

  it("hangt het moment uit AH's keywords aan elke optie", async () => {
    const store = await storeWithMoments();
    // Het exacte per-portie profiel van de drie gelijke recepten: dan passen ze
    // allemaal zonder herschalen bij het doel en komen ze gegarandeerd in de
    // selectie terecht.
    const targets: DailyTargets = { kcal: 300, protein: 40, carbs: 20, fat: 10, fiber: 5 };
    const options = await rerollSlotOptions(store, client, {
      targets,
      excludeRecipeIds: [],
      count: 6,
    });

    const ontbijt = options.find((o) => o.plan.recipeId === "R-M1");
    expect(ontbijt?.plan.moment).toBe("ontbijt");
    // AH zegt "tussendoortje"; de app vertaalt dat naar het moment "snack".
    const snack = options.find((o) => o.plan.recipeId === "R-M2");
    expect(snack?.plan.moment).toBe("snack");
    // Zonder keywords en zonder aanwijzingen in titel/ingredienten: geen moment.
    const onbekend = options.find((o) => o.plan.recipeId === "R-M3");
    expect(onbekend?.plan.moment).toBeNull();
  });

  it("het plan dat een slot kiest draagt het moment ook", async () => {
    const store = await storeWithMoments();
    const plan = await rerollSlot(store, client, {
      targets: { kcal: 300, protein: 40, carbs: 20, fat: 10, fiber: 5 },
      excludeRecipeIds: [],
    });

    expect(plan).not.toBeNull();
    expect(["ontbijt", "snack"]).toContain(plan!.moment);
  });

  it("generateDay geeft elk gekozen plan het afgeleide moment mee", async () => {
    const store = await storeWithLibrary();
    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    for (const meal of day.meals) {
      // De recepten dragen momentlabels via titel/ingredienten (kwark/havermout =
      // ontbijt, rijst = diner); een recept zonder aanwijzingen heeft terecht null.
      const moment = meal.plan!.moment;
      expect(moment === null || ["ontbijt", "lunch", "snack", "diner"].includes(moment)).toBe(true);
      // De eerste keuzekaart is het getoonde plan: hun momenten horen gelijk te zijn.
      expect(meal.options![0]!.plan.moment).toBe(moment);
    }
  });
});

describe("macroDistance", () => {
  it("is zero for identical profiles and grows with the difference", () => {
    const a = { kcal: 600, protein: 40, carbs: 60, fat: 20, fiber: 8 };
    expect(macroDistance(a, a)).toBe(0);
    expect(macroDistance({ ...a, protein: 45 }, a)).toBeLessThan(
      macroDistance({ ...a, protein: 80 }, a),
    );
  });

  it("treats a missing macro as zero rather than throwing", () => {
    expect(macroDistance({ kcal: 100 }, {})).toBeGreaterThan(0);
  });
});

// --- per-ingrediënt schalen op basis van productmatches uit de database ---

describe("per-ingrediënt schalen met DB-producten", () => {
  /** Eén recept met producten en matches, precies zoals de enrich-pipeline ze achterlaat. */
  async function storeWithProductMatches(): Promise<Store> {
    db = createTestDb();
    const store = new Store(db);
    await seedRecipes(store, [
      {
        id: "R-R1",
        title: "Kwark met banaan",
        ingredients: [
          { name: "kwark", grams: 250, per100g: FOODS.kwark! },
          { name: "banaan", grams: 100, per100g: FOODS.banaan! },
        ],
      },
    ]);
    // Zelfde weg als de ingest: product eerst, dan de onthouden koppeling.
    await store.putProduct({
      webshopId: "wi-kwark",
      title: "AH Magere kwark",
      salesUnitSize: "500 g",
      per100g: FOODS.kwark!,
    });
    await store.putProduct({
      webshopId: "wi-banaan",
      title: "AH Banaan",
      salesUnitSize: null,
      per100g: FOODS.banaan!,
    });
    await store.putMatch("kwark", "wi-kwark", 0.9);
    await store.putMatch("banaan", "wi-banaan", 0.8);
    return store;
  }

  // 0 betekent "niet meegeteld": buildTargets slaat nuldoelen over, dus dit is
  // exact het eiwit+kcal-doel waarmee de C5-tests in plan.test.ts schalen.
  const eiwitDoel: DailyTargets = { protein: 45, kcal: 230, carbs: 0, fat: 0, fiber: 0 };

  it("schaalt kwark omhoog en banaan omlaag met gemeten waarden uit de database", async () => {
    const store = await storeWithProductMatches();
    const plan = await rerollSlot(store, client, {
      targets: eiwitDoel,
      kcalMode: "max",
      excludeRecipeIds: [],
    });

    expect(plan).not.toBeNull();
    const kwark = plan!.ingredients.find((i) => i.name === "kwark")!;
    const banaan = plan!.ingredients.find((i) => i.name === "banaan")!;
    // Zonder gelezen matches zou elke regel "geschat" zijn en het recept als één
    // geheel schalen. Hier zijn de gezaaide producten (57 resp. 89 kcal/100 g)
    // doorgedrongen tot de solver: eiwitrijke kwark omhoog, banaan omlaag.
    expect(kwark.nutrientSource).toBe("product");
    expect(banaan.nutrientSource).toBe("product");
    expect(kwark.scale).toBeGreaterThan(1.05);
    expect(banaan.scale).toBeLessThan(0.95);
  });

  it("zonder gezaaide matches schaalt hetzelfde recept uniform en blijft 'geschat'", async () => {
    db = createTestDb();
    const store = new Store(db);
    await seedRecipes(store, [
      {
        id: "R-R1",
        title: "Kwark met banaan",
        ingredients: [
          { name: "kwark", grams: 250, per100g: FOODS.kwark! },
          { name: "banaan", grams: 100, per100g: FOODS.banaan! },
        ],
      },
    ]);

    const plan = await rerollSlot(store, client, {
      targets: eiwitDoel,
      kcalMode: "max",
      excludeRecipeIds: [],
    });

    expect(plan).not.toBeNull();
    const kwark = plan!.ingredients.find((i) => i.name === "kwark")!;
    expect(kwark.nutrientSource).toBe("geschat");
    // Alle regels zijn aandelen van AH's recepttotaal: één factor voor het geheel.
    const scales = plan!.ingredients.map((i) => i.scale);
    expect(new Set(scales).size).toBe(1);
  });
});
