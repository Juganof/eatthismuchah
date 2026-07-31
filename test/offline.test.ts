import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient, resetEndpointState } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { DEFAULT_SLOTS } from "../src/nutrition/split";
import type { DailyTargets } from "../src/nutrition/targets";
import { blankDay, generateDay, rerollSlot } from "../src/optimize/day";
import { createTestDb, type TestDb } from "./helpers/d1";
import { FOODS, seedRecipes } from "./helpers/seed";

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
  resetEndpointState();
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

/** Complete recepten: product, koppeling en voedingswaarde staan er allemaal in. */
async function seedPlannable(store: Store): Promise<void> {
  await seedRecipes(store, [
    { id: "R-R1", title: "Kwark met granola", servings: 2, ingredients: [
      { name: "kwark", grams: 300, per100g: FOODS.kwark! },
      { name: "havermout", grams: 80, per100g: FOODS.havermout! },
    ] },
    { id: "R-R2", title: "Kip met rijst", servings: 2, ingredients: [
      { name: "kipfilet", grams: 300, per100g: FOODS.kipfilet! },
      { name: "rijst", grams: 150, per100g: FOODS.rijst! },
    ] },
    { id: "R-R3", title: "Pasta pesto", servings: 2, ingredients: [
      { name: "rijst", grams: 250, per100g: FOODS.rijst! },
      { name: "olijfolie", grams: 30, per100g: FOODS.olijfolie! },
    ] },
    { id: "R-R4", title: "Soep met brood", servings: 2, ingredients: [
      { name: "broccoli", grams: 200, per100g: FOODS.broccoli! },
      { name: "kwark", grams: 100, per100g: FOODS.kwark! },
    ] },
  ]);
}

describe("planning without the network", () => {
  it("generates a whole day from the database alone", async () => {
    const fetchMock = explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedPlannable(store);

    const day = await generateDay(store, client, {
      date: "2026-08-01",
      slots: DEFAULT_SLOTS,
      daily,
    });

    expect(fetchMock).not.toHaveBeenCalled();
    expect(day.meals.filter((m) => m.plan)).not.toHaveLength(0);
  });

  it("rerolls without touching the network either", async () => {
    const fetchMock = explodingFetch();
    db = createTestDb();
    const store = new Store(db);
    await seedPlannable(store);

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
