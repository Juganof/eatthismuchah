import { describe, expect, it } from "vitest";
import { DEFAULT_SLOTS, remainingTargets, splitTargets, type MealSlot } from "../src/nutrition/split";
import type { DailyTargets } from "../src/nutrition/targets";

const daily: DailyTargets = { kcal: 2500, protein: 160, carbs: 250, fat: 80, fiber: 35 };

const slot = (over: Partial<MealSlot> & { id: string }): MealSlot => ({
  name: over.id,
  position: 0,
  kcalShare: 0.25,
  proteinShare: null,
  enabled: true,
  tags: [],
  maxKcal: null,
  ...over,
});

describe("splitTargets", () => {
  it("splits the day across the default slots", () => {
    const split = splitTargets(daily, DEFAULT_SLOTS);
    expect(split.map((s) => s.slot.id)).toEqual(["ontbijt", "lunch", "tussendoortje", "diner"]);
    expect(split[0]!.targets.kcal).toBe(625); // 25% of 2500
    expect(split[3]!.targets.kcal).toBe(875); // 35% of 2500
  });

  it("adds up to exactly the day target, rounding included", () => {
    // 3 slots on 2500 kcal is where naive rounding loses a calorie.
    const slots = [
      slot({ id: "a", position: 0, kcalShare: 1 }),
      slot({ id: "b", position: 1, kcalShare: 1 }),
      slot({ id: "c", position: 2, kcalShare: 1 }),
    ];
    const split = splitTargets(daily, slots);

    for (const key of ["kcal", "protein", "carbs", "fat", "fiber"] as const) {
      const sum = split.reduce((total, s) => total + s.targets[key], 0);
      expect(sum, `${key} must sum to the day target`).toBe(daily[key]);
    }
  });

  it("normalises shares that do not add up to one", () => {
    // Two slots of "3" and "1" mean 75/25, whatever the absolute numbers are.
    const split = splitTargets(daily, [
      slot({ id: "a", position: 0, kcalShare: 3 }),
      slot({ id: "b", position: 1, kcalShare: 1 }),
    ]);
    expect(split[0]!.targets.kcal).toBe(1875);
    expect(split[1]!.targets.kcal).toBe(625);
  });

  it("splits protein on its own share when a slot sets one", () => {
    const split = splitTargets(daily, [
      slot({ id: "ontbijt", position: 0, kcalShare: 0.5, proteinShare: 0.75 }),
      slot({ id: "diner", position: 1, kcalShare: 0.5, proteinShare: 0.25 }),
    ]);
    expect(split[0]!.targets.kcal).toBe(1250);
    expect(split[0]!.targets.protein).toBe(120);
    expect(split[1]!.targets.protein).toBe(40);
  });

  it("leaves disabled slots out entirely", () => {
    const split = splitTargets(daily, [
      slot({ id: "a", position: 0, kcalShare: 0.5 }),
      slot({ id: "uit", position: 1, kcalShare: 0.5, enabled: false }),
    ]);
    expect(split).toHaveLength(1);
    expect(split[0]!.targets.kcal).toBe(2500);
  });

  it("caps a slot at its own maximum", () => {
    const split = splitTargets(daily, [
      slot({ id: "snack", position: 0, kcalShare: 0.5, maxKcal: 200 }),
      slot({ id: "diner", position: 1, kcalShare: 0.5 }),
    ]);
    expect(split[0]!.targets.kcal).toBe(200);
  });

  it("divides evenly when every share is zero", () => {
    const split = splitTargets(daily, [
      slot({ id: "a", position: 0, kcalShare: 0 }),
      slot({ id: "b", position: 1, kcalShare: 0 }),
    ]);
    expect(split[0]!.targets.kcal).toBe(1250);
    expect(split[1]!.targets.kcal).toBe(1250);
  });

  it("returns nothing when there is no enabled slot", () => {
    expect(splitTargets(daily, [])).toEqual([]);
    expect(splitTargets(daily, [slot({ id: "uit", enabled: false })])).toEqual([]);
  });
});

describe("remainingTargets", () => {
  it("subtracts what has already been planned", () => {
    const left = remainingTargets(daily, [{ kcal: 600, protein: 40, carbs: 60, fat: 20, fiber: 8 }]);
    expect(left).toEqual({ kcal: 1900, protein: 120, carbs: 190, fat: 60, fiber: 27 });
  });

  it("never goes negative", () => {
    const left = remainingTargets(daily, [
      { kcal: 4000, protein: 300, carbs: 400, fat: 200, fiber: 60 },
    ]);
    expect(left).toEqual({ kcal: 0, protein: 0, carbs: 0, fat: 0, fiber: 0 });
  });
});
