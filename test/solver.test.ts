import { describe, expect, it } from "vitest";
import { solve, type MacroTarget, type SolverIngredient } from "../src/optimize/solver";

/** Chicken, rice and oil: enough spread to make the solver actually choose. */
const kitchen: SolverIngredient[] = [
  { nutrients: { kcal: 165, protein: 31, carbs: 0, fat: 3.6 }, min: 0.25, max: 3 }, // 100g kipfilet
  { nutrients: { kcal: 350, protein: 7, carbs: 78, fat: 0.6 }, min: 0.25, max: 3 }, // 100g rijst
  { nutrients: { kcal: 884, protein: 0, carbs: 0, fat: 100 }, min: 0.25, max: 3 }, // 100g olie
];

describe("solve", () => {
  it("hits a protein target it can reach", () => {
    const targets: MacroTarget[] = [{ key: "protein", value: 60, mode: "target", weight: 2 }];
    const { totals } = solve(kitchen, targets, { shapePenalty: 0 });
    expect(totals.protein).toBeCloseTo(60, 0);
  });

  it("balances protein and calories together", () => {
    const targets: MacroTarget[] = [
      { key: "protein", value: 60, mode: "target", weight: 2 },
      { key: "kcal", value: 700, mode: "target", weight: 1.5 },
    ];
    const { totals } = solve(kitchen, targets, { shapePenalty: 0 });
    expect(totals.protein).toBeCloseTo(60, 0);
    expect(totals.kcal).toBeCloseTo(700, 0);
  });

  it("respects upper bounds even when that means missing the target", () => {
    // 3x everything is only 93g protein short of a 500g demand.
    const targets: MacroTarget[] = [{ key: "protein", value: 500, mode: "target" }];
    const { scales, totals } = solve(kitchen, targets, { shapePenalty: 0 });
    for (const s of scales) expect(s).toBeLessThanOrEqual(3 + 1e-9);
    expect(totals.protein!).toBeLessThan(500);
  });

  it("respects lower bounds", () => {
    const targets: MacroTarget[] = [{ key: "kcal", value: 0, mode: "target" }];
    const { scales } = solve(kitchen, targets, { shapePenalty: 0 });
    for (const s of scales) expect(s).toBeGreaterThanOrEqual(0.25 - 1e-9);
  });

  it("leaves a satisfied 'max' target alone", () => {
    // The recipe as written is 1399 kcal; a 2000 kcal ceiling should not push it up.
    const targets: MacroTarget[] = [{ key: "kcal", value: 2000, mode: "max" }];
    const { scales } = solve(kitchen, targets, { shapePenalty: 0.15 });
    for (const s of scales) expect(s).toBeCloseTo(1, 2);
  });

  it("pushes a violated 'max' target down", () => {
    const targets: MacroTarget[] = [{ key: "kcal", value: 600, mode: "max" }];
    const { totals } = solve(kitchen, targets, { shapePenalty: 0 });
    expect(totals.kcal!).toBeLessThanOrEqual(601);
  });

  it("keeps proportions closer to the original as the shape penalty rises", () => {
    const targets: MacroTarget[] = [{ key: "protein", value: 90, mode: "target" }];
    const loose = solve(kitchen, targets, { shapePenalty: 0 });
    const tight = solve(kitchen, targets, { shapePenalty: 5 });
    const drift = (s: number[]) => s.reduce((a, x) => a + Math.abs(x - 1), 0);
    expect(drift(tight.scales)).toBeLessThan(drift(loose.scales));
  });

  it("never increases the objective", () => {
    const targets: MacroTarget[] = [
      { key: "protein", value: 60, mode: "target" },
      { key: "kcal", value: 700, mode: "target" },
    ];
    const start = solve(kitchen, targets, { shapePenalty: 0.15, maxIterations: 0 });
    const done = solve(kitchen, targets, { shapePenalty: 0.15 });
    expect(done.cost).toBeLessThanOrEqual(start.cost);
  });

  it("handles an empty recipe without crashing", () => {
    expect(solve([], [{ key: "protein", value: 60, mode: "target" }]).scales).toEqual([]);
  });

  it("is deterministic", () => {
    const targets: MacroTarget[] = [{ key: "protein", value: 60, mode: "target" }];
    expect(solve(kitchen, targets).scales).toEqual(solve(kitchen, targets).scales);
  });
});
