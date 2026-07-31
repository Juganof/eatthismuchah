import { afterEach, describe, expect, it } from "vitest";
import { AhClient } from "../src/ah/client";
import { Store } from "../src/db/queries";
import { DEFAULT_SLOTS, type MealSlot } from "../src/nutrition/split";
import type { DailyTargets } from "../src/nutrition/targets";
import { generateDay, macroDistance, rerollSlot } from "../src/optimize/day";
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
    expect(Math.abs(day.totals.protein! - daily.protein) / daily.protein).toBeLessThan(0.2);
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
