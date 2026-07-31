import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { DEFAULT_SLOTS } from "../src/nutrition/split";
import type { DailyTargets } from "../src/nutrition/targets";
import { blankDay, generateDay, rerollSlot } from "../src/optimize/day";
import { createTestDb, type TestDb } from "./helpers/d1";

/**
 * Plannen mag ah.nl nooit aanraken.
 *
 * Dat ging eerder mis: een recept waarvan de ingredienten niet aan een product
 * gekoppeld waren liet `resolveRecipe` alsnog zoeken, en met tientallen
 * kandidaten maal tien ingredienten maal de verplichte pauze ertussen liep het
 * genereren van een dag in de minuten. Elke test hier zet een fetch neer die
 * ontploft, zodat één enkel netwerkverzoek de test omgooit.
 */

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
  vi.unstubAllGlobals();
});

const daily: DailyTargets = { kcal: 2000, protein: 150, carbs: 200, fat: 60, fiber: 28 };
const client = new AhClient("test");

function explodingFetch() {
  const fetchMock = vi.fn(() => {
    throw new Error("plannen mag geen netwerk doen");
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

/**
 * Recepten zoals ze binnenkomen van AH: mét voedingswaarde van de receptpagina,
 * maar zónder dat de ingredienten aan producten gekoppeld zijn. Precies het
 * geval dat het netwerk op ging.
 */
async function seedUnmatched(store: Store): Promise<void> {
  const recipes = [
    { id: "R-R1", title: "Kwark met granola", kcal: 420, protein: 32 },
    { id: "R-R2", title: "Kip met rijst", kcal: 640, protein: 52 },
    { id: "R-R3", title: "Pasta pesto", kcal: 700, protein: 24 },
    { id: "R-R4", title: "Soep met brood", kcal: 380, protein: 18 },
  ];
  for (const r of recipes) {
    await store.putRecipe({
      id: r.id,
      title: r.title,
      url: `https://www.ah.nl/allerhande/recept/${r.id}`,
      servings: 2,
      imageUrl: null,
      ingredients: [
        { name: "onbekend hoofdingredient", quantity: 300, unit: "g" },
        { name: "nog iets onbekends", quantity: 100, unit: "g" },
      ],
    });
    // Twee porties, dus het totaal is het dubbele van wat per portie geldt.
    await store.putNutrition(
      r.id,
      { kcal: r.kcal * 2, protein: r.protein * 2, carbs: 100, fat: 30, fiber: 10 },
      1,
      "ah",
    );
  }
}

describe("planning without the network", () => {
  it("generates a whole day from the database alone", async () => {
    const fetchMock = explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedUnmatched(store);

    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(day.meals.filter((m) => m.plan)).not.toHaveLength(0);
  });

  it("uses AH's own totals when the ingredients have no products", async () => {
    explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedUnmatched(store);

    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    // Zonder de terugval op het totaal zou elk plan op nul calorieën uitkomen.
    for (const meal of day.meals) {
      if (!meal.plan) continue;
      expect(meal.plan.perPortion.kcal, meal.slotName).toBeGreaterThan(0);
      expect(meal.plan.coverage, meal.slotName).toBe(1);
    }
    expect(day.totals.kcal).toBeGreaterThan(0);
  });

  it("scales the dish as a whole towards the target", async () => {
    explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedUnmatched(store);

    const plan = await rerollSlot(store, client, {
      targets: { kcal: 300, protein: 25, carbs: 30, fat: 8, fiber: 4 },
      excludeRecipeIds: [],
    });

    expect(plan).not.toBeNull();
    // Elk ingredient krijgt dezelfde factor: je maakt een kleinere pan.
    const factors = new Set(plan!.ingredients.map((i) => i.scale));
    expect(factors.size).toBe(1);
    // En het resultaat ligt dichter bij 300 kcal dan het ongeschaalde recept.
    expect(plan!.perPortion.kcal!).toBeLessThan(500);
  });

  it("rerolls without touching the network either", async () => {
    const fetchMock = explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedUnmatched(store);

    const plan = await rerollSlot(store, client, {
      targets: { kcal: 600, protein: 45, carbs: 60, fat: 20, fiber: 8 },
      excludeRecipeIds: ["R-R1"],
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(plan?.recipeId).not.toBe("R-R1");
  });
});

describe("blankDay", () => {
  it("lists every enabled slot with its target and no recipe yet", () => {
    const day = blankDay("2026-08-01", DEFAULT_SLOTS, daily);

    expect(day.meals).toHaveLength(4);
    expect(day.meals.map((m) => m.slotName)).toEqual(["Ontbijt", "Lunch", "Tussendoortje", "Diner"]);
    expect(day.meals.every((m) => m.plan === null)).toBe(true);
    expect(day.meals[0]!.targets.kcal).toBe(500); // 25% van 2000
    // De hints gaan mee, zodat het invullen van één moment ze kan meesturen.
    expect(day.meals[0]!.slotTags).toContain("kwark");
  });

  it("reports the full day as the shortfall, since nothing is planned yet", () => {
    const day = blankDay("2026-08-01", DEFAULT_SLOTS, daily);
    expect(day.totals.kcal).toBe(0);
    expect(day.deviation.kcal).toBe(daily.kcal);
  });

  it("leaves out slots that are switched off", () => {
    const slots = DEFAULT_SLOTS.map((s) => (s.id === "lunch" ? { ...s, enabled: false } : s));
    expect(blankDay("2026-08-01", slots, daily).meals.map((m) => m.slotId)).not.toContain("lunch");
  });
});
