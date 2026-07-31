import { describe, expect, it } from "vitest";
import {
  ACTIVITY_FACTORS,
  DEFAULT_PROFILE,
  bmr,
  dailyTargets,
  kcalTarget,
  sanitiseProfile,
  tdee,
  type Profile,
} from "../src/nutrition/targets";

const base: Profile = {
  ...DEFAULT_PROFILE,
  age: 30,
  sex: "man",
  heightCm: 180,
  weightKg: 80,
  activityLevel: "moderate",
};

describe("bmr", () => {
  it("follows Mifflin-St Jeor for men", () => {
    // 10*80 + 6.25*180 - 5*30 + 5 = 1780
    expect(bmr(base)).toBe(1780);
  });

  it("follows Mifflin-St Jeor for women", () => {
    // same body, -161 instead of +5
    expect(bmr({ ...base, sex: "vrouw" })).toBe(1614);
  });

  it("refuses to guess when the profile is incomplete", () => {
    expect(bmr({ ...base, weightKg: null })).toBeNull();
    expect(bmr({ ...base, sex: null })).toBeNull();
    expect(tdee({ ...base, age: null })).toBeNull();
    expect(dailyTargets({ ...base, heightCm: null })).toBeNull();
  });
});

describe("tdee", () => {
  it("multiplies by the activity factor", () => {
    expect(tdee(base)).toBeCloseTo(1780 * ACTIVITY_FACTORS.moderate, 5);
    expect(tdee({ ...base, activityLevel: "sedentary" })).toBeCloseTo(1780 * 1.2, 5);
  });
});

describe("kcalTarget", () => {
  it("subtracts a deficit when cutting and adds a surplus when bulking", () => {
    const maintenance = tdee(base)!;
    const cut = kcalTarget({ ...base, goal: "cut", rateKgPerWeek: 0.5 })!;
    const bulk = kcalTarget({ ...base, goal: "bulk", rateKgPerWeek: 0.5 })!;

    // 0.5 kg/week is 7700/2 kcal per week, so 550 per day.
    expect(cut).toBe(Math.round(maintenance - 550));
    expect(bulk).toBe(Math.round(maintenance + 550));
  });

  it("keeps maintenance flat regardless of the rate", () => {
    expect(kcalTarget({ ...base, goal: "maintain", rateKgPerWeek: 1 })).toBe(
      Math.round(tdee(base)!),
    );
  });

  it("never drops below the floor, however aggressive the goal", () => {
    const extreme = kcalTarget({ ...base, goal: "cut", rateKgPerWeek: 1.5 })!;
    expect(extreme).toBeGreaterThanOrEqual(1200);
  });

  it("lets a manual override win", () => {
    expect(kcalTarget({ ...base, kcalOverride: 2400 })).toBe(2400);
  });
});

describe("dailyTargets", () => {
  it("sets protein and fat per kilo and fills the rest with carbs", () => {
    const targets = dailyTargets({ ...base, proteinPerKg: 2, fatPerKg: 1 })!;
    expect(targets.protein).toBe(160);
    expect(targets.fat).toBe(80);

    const fromMacros = targets.protein * 4 + targets.fat * 9 + targets.carbs * 4;
    // Rounding to whole grams may shift a few kcal, but no more than that.
    expect(Math.abs(fromMacros - targets.kcal)).toBeLessThanOrEqual(4);
  });

  it("scales protein and fat back rather than going negative on carbs", () => {
    // 4 g/kg protein and 2 g/kg fat on 80 kg is 2720 kcal before any carbs.
    const targets = dailyTargets({
      ...base,
      proteinPerKg: 4,
      fatPerKg: 2,
      kcalOverride: 1800,
    })!;

    expect(targets.carbs).toBe(0);
    expect(targets.protein).toBeLessThan(320);
    expect(targets.protein * 4 + targets.fat * 9).toBeLessThanOrEqual(1810);
  });

  it("scales fibre with the calorie target", () => {
    const targets = dailyTargets({ ...base, kcalOverride: 2000 })!;
    expect(targets.fiber).toBe(28);
  });
});

describe("sanitiseProfile", () => {
  it("clamps impossible values instead of storing them", () => {
    const profile = sanitiseProfile({ age: 900, weightKg: 5, heightCm: 400, proteinPerKg: 99 });
    expect(profile.age).toBe(120);
    expect(profile.weightKg).toBe(30);
    expect(profile.heightCm).toBe(250);
    expect(profile.proteinPerKg).toBe(4);
  });

  it("falls back to defaults for unknown enums and junk input", () => {
    const profile = sanitiseProfile({
      sex: "onbekend" as never,
      activityLevel: "turbo" as never,
      goal: "shred" as never,
      age: "dertig" as never,
      diet: ["vegetarisch", 5 as never],
    });
    expect(profile.sex).toBeNull();
    expect(profile.activityLevel).toBe("moderate");
    expect(profile.goal).toBe("maintain");
    expect(profile.age).toBeNull();
    expect(profile.diet).toEqual(["vegetarisch"]);
  });
});
