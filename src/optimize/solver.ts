import { NUTRIENT_KEYS, type NutrientKey, type Nutrients } from "../ah/types";

/**
 * Rescales a recipe's ingredients to hit macro targets.
 *
 * The problem is: choose a scale factor per ingredient, within sane bounds, so the
 * totals land on the targets while the dish still resembles the original recipe.
 * That is a convex quadratic in the scale factors, so projected gradient descent
 * finds the global optimum — no heuristics and no randomness.
 */

export type TargetMode = "target" | "max" | "min";

export interface MacroTarget {
  key: NutrientKey;
  value: number;
  mode: TargetMode;
  /** Relative importance. Defaults to 1. */
  weight?: number;
}

export interface SolverIngredient {
  /** Absolute nutrients at the recipe's original quantity. */
  nutrients: Nutrients;
  /** Smallest allowed multiple of the original amount. */
  min: number;
  /** Largest allowed multiple of the original amount. */
  max: number;
}

export interface SolverOptions {
  /**
   * Pull toward the original recipe, as a tie-breaker between solutions that fit
   * the targets equally well. It is averaged over the ingredients, so a 12-line
   * recipe is not held more rigidly than a 4-line one, and it is measured on the
   * same relative scale as the target residuals: 0.01 is worth about a 4% miss.
   * 0 lets the solver do anything within bounds.
   */
  shapePenalty?: number;
  maxIterations?: number;
  /** Stop once the objective improves by less than this fraction. */
  tolerance?: number;
}

export interface SolverResult {
  /** One scale factor per input ingredient, in the same order. */
  scales: number[];
  /** Achieved totals after scaling. */
  totals: Nutrients;
  /** Objective value; lower is better. Useful for ranking recipes. */
  cost: number;
  iterations: number;
}

const DEFAULTS = {
  shapePenalty: 0.01,
  maxIterations: 500,
  tolerance: 1e-9,
};

/** Totals for a given set of scale factors. */
function totalsFor(ingredients: SolverIngredient[], scales: number[]): Nutrients {
  const out: Nutrients = {};
  for (const key of NUTRIENT_KEYS) {
    let sum = 0;
    for (let i = 0; i < ingredients.length; i++) {
      sum += (ingredients[i]!.nutrients[key] ?? 0) * (scales[i] ?? 0);
    }
    out[key] = sum;
  }
  return out;
}

/**
 * Residual for one target, already normalised by the target's own magnitude so
 * that 60 g of protein and 700 kcal carry comparable weight. Returns 0 for the
 * satisfied side of a one-sided target.
 */
function residual(target: MacroTarget, achieved: number): number {
  const scale = Math.max(Math.abs(target.value), 1e-6);
  const diff = (achieved - target.value) / scale;
  if (target.mode === "max") return Math.max(0, diff);
  if (target.mode === "min") return Math.min(0, diff);
  return diff;
}

function objective(
  ingredients: SolverIngredient[],
  targets: MacroTarget[],
  scales: number[],
  shapePenalty: number,
): number {
  const totals = totalsFor(ingredients, scales);
  let cost = 0;
  for (const t of targets) {
    const r = residual(t, totals[t.key] ?? 0);
    cost += (t.weight ?? 1) * r * r;
  }
  let drift = 0;
  for (const s of scales) drift += (s - 1) * (s - 1);
  cost += scales.length > 0 ? (shapePenalty * drift) / scales.length : 0;
  return cost;
}

function gradient(
  ingredients: SolverIngredient[],
  targets: MacroTarget[],
  scales: number[],
  shapePenalty: number,
): number[] {
  const totals = totalsFor(ingredients, scales);
  const grad = new Array<number>(ingredients.length).fill(0);

  for (const t of targets) {
    const scale = Math.max(Math.abs(t.value), 1e-6);
    const r = residual(t, totals[t.key] ?? 0);
    if (r === 0) continue; // one-sided target on its satisfied side
    const factor = (2 * (t.weight ?? 1) * r) / scale;
    for (let i = 0; i < ingredients.length; i++) {
      grad[i] = (grad[i] ?? 0) + factor * (ingredients[i]!.nutrients[t.key] ?? 0);
    }
  }

  const shapeCoefficient = ingredients.length > 0 ? (2 * shapePenalty) / ingredients.length : 0;
  for (let i = 0; i < ingredients.length; i++) {
    grad[i] = (grad[i] ?? 0) + shapeCoefficient * ((scales[i] ?? 0) - 1);
  }
  return grad;
}

/** Clamps each scale back into its ingredient's allowed range. */
function project(ingredients: SolverIngredient[], scales: number[]): number[] {
  return scales.map((s, i) => {
    const ing = ingredients[i]!;
    return Math.min(ing.max, Math.max(ing.min, s));
  });
}

export function solve(
  ingredients: SolverIngredient[],
  targets: MacroTarget[],
  options: SolverOptions = {},
): SolverResult {
  const shapePenalty = options.shapePenalty ?? DEFAULTS.shapePenalty;
  const maxIterations = options.maxIterations ?? DEFAULTS.maxIterations;
  const tolerance = options.tolerance ?? DEFAULTS.tolerance;

  if (ingredients.length === 0) {
    return { scales: [], totals: {}, cost: objective([], targets, [], shapePenalty), iterations: 0 };
  }

  let scales = project(ingredients, ingredients.map(() => 1));
  let cost = objective(ingredients, targets, scales, shapePenalty);
  let step = 1;
  let iterations = 0;

  for (let iter = 0; iter < maxIterations; iter++) {
    iterations = iter + 1;
    const grad = gradient(ingredients, targets, scales, shapePenalty);

    // Backtracking line search: the curvature varies hugely between recipes
    // (a 900 kcal oil vs a 20 kcal herb), so a fixed step size is not usable.
    let accepted = false;
    for (let probe = 0; probe < 40; probe++) {
      const candidate = project(
        ingredients,
        scales.map((s, i) => s - step * (grad[i] ?? 0)),
      );
      const candidateCost = objective(ingredients, targets, candidate, shapePenalty);
      if (candidateCost < cost) {
        const improvement = cost - candidateCost;
        scales = candidate;
        cost = candidateCost;
        accepted = true;
        // Creep the step back up so we don't stay stuck at a tiny stride.
        step *= 1.2;
        if (improvement < tolerance * Math.max(1, Math.abs(cost))) return finish();
        break;
      }
      step *= 0.5;
    }
    if (!accepted) break; // step size collapsed: we're at the constrained optimum
  }

  return finish();

  function finish(): SolverResult {
    return { scales, totals: totalsFor(ingredients, scales), cost, iterations };
  }
}
