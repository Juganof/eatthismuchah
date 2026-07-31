import type { Nutrients, ResolvedRecipe } from "../ah/types";
import { sumNutrients } from "../nutrition/resolve";
import { solve, type MacroTarget, type SolverIngredient } from "./solver";

/**
 * Applies the solver to a resolved recipe and presents the answer in the terms a
 * cook needs: how many grams of each thing, for one portion.
 */

export interface PlanOptions {
  /** Portions the plan is for. Targets are per portion. */
  portions?: number;
  /** Ingredient names that must keep their original proportion. */
  locked?: string[];
  /** Lower/upper bound on how far any one ingredient may be scaled. */
  minScale?: number;
  maxScale?: number;
  shapePenalty?: number;
}

export interface PlannedIngredient {
  name: string;
  /** Grams for the whole plan (all portions). */
  grams: number;
  originalGrams: number;
  scale: number;
  nutrients: Nutrients;
  productTitle: string | null;
  matchScore: number;
  /** True when nutrition is unknown, so the solver had nothing to go on. */
  unmatched: boolean;
  /**
   * Quantity in the recipe's own unit (e.g. "3" bananen, "1.5" el), for the whole
   * plan. Null when AH gave no quantity. This is what should be shown to a cook —
   * grams are an internal bridge for nutrition, not what the recipe was written in.
   */
  originalQuantity: number | null;
  /** The recipe's own unit, e.g. "el", "stuk"; null for a bare count or unknown quantity. */
  unit: string | null;
  /** How grams were derived; also picks which unit is shown for this line. */
  gramsSource: "explicit" | "volume" | "piece" | "spoon" | "fallback";
}

export interface Plan {
  recipeId: string;
  title: string;
  url: string;
  imageUrl: string | null;
  portions: number;
  ingredients: PlannedIngredient[];
  /** Totals per portion, which is what the targets were expressed in. */
  perPortion: Nutrients;
  totals: Nutrients;
  /** Solver cost; lower means the targets were met more closely. */
  cost: number;
  /** Share of recipe weight with known nutrition. */
  coverage: number;
  /**
   * Whether this plan came from the per-ingredient solver (real per-ingredient
   * nutrition was known) or from scaling the whole recipe by one factor (only the
   * recipe-level total was known, e.g. AH's own nutrition-per-serving figure).
   */
  scalingMode: "solver" | "uniform";
}

/**
 * Ingredients that should not be freely rescaled: seasonings dominate nothing
 * nutritionally but doubling the salt ruins the dish, and the solver has no
 * concept of taste.
 */
const SEASONING = /peper|zout|kruid|specerij|kaneel|nootmuskaat|paprikapoeder|komijn|kerrie|bouillon|gist|bakpoeder|vanille/i;

function boundsFor(name: string, opts: PlanOptions): { min: number; max: number } {
  if (opts.locked?.some((l) => name.includes(l.toLowerCase()))) return { min: 1, max: 1 };
  if (SEASONING.test(name)) {
    // Intersect rather than override: a caller pinning everything close to 1 (an
    // "as written" option) must still pin the seasoning, not fall back to its own
    // wider default range.
    const min = Math.max(0.75, opts.minScale ?? 0.25);
    const max = Math.min(1.5, opts.maxScale ?? 3);
    return { min: Math.min(min, max), max };
  }
  return { min: opts.minScale ?? 0.25, max: opts.maxScale ?? 3 };
}

export function planRecipe(
  resolved: ResolvedRecipe,
  targets: MacroTarget[],
  opts: PlanOptions = {},
): Plan {
  const portions = opts.portions ?? 1;
  const servings = resolved.recipe.servings > 0 ? resolved.recipe.servings : 1;

  // The targets are per portion, so the solver must see one portion's worth of
  // nutrition — not the whole recipe and not the whole order. Portion count is
  // applied afterwards, to the amounts only.
  const solverIngredients: SolverIngredient[] = resolved.ingredients.map((ing) => {
    const perPortionNutrients: Nutrients = {};
    for (const [k, v] of Object.entries(ing.nutrients)) {
      perPortionNutrients[k as keyof Nutrients] = v / servings;
    }
    return { nutrients: perPortionNutrients, ...boundsFor(ing.raw.name, opts) };
  });

  const result = solve(solverIngredients, targets, { shapePenalty: opts.shapePenalty });

  const ingredients: PlannedIngredient[] = resolved.ingredients.map((ing, i) => {
    const scale = result.scales[i] ?? 1;
    // Amounts are what you buy and cook, so these are for all portions.
    const originalGrams = (ing.grams / servings) * portions;
    const nutrients: Nutrients = {};
    for (const [k, v] of Object.entries(solverIngredients[i]!.nutrients)) {
      nutrients[k as keyof Nutrients] = v * scale * portions;
    }
    return {
      name: ing.raw.name,
      originalGrams: round(originalGrams),
      grams: round(originalGrams * scale),
      scale: Math.round(scale * 100) / 100,
      nutrients,
      productTitle: ing.product?.title ?? null,
      matchScore: ing.matchScore,
      unmatched: !ing.product || Object.keys(ing.product.per100g).length === 0,
      originalQuantity: ing.raw.quantity !== null ? round((ing.raw.quantity / servings) * portions) : null,
      unit: ing.raw.unit,
      gramsSource: ing.gramsSource,
    };
  });

  const totals = sumNutrients(ingredients.map((i) => i.nutrients));
  const perPortion = result.totals;

  return {
    recipeId: resolved.recipe.id,
    title: resolved.recipe.title,
    url: resolved.recipe.url,
    imageUrl: resolved.recipe.imageUrl,
    portions,
    ingredients,
    totals,
    perPortion,
    cost: result.cost,
    coverage: coverageOfPlan(ingredients),
    scalingMode: "solver",
  };
}

/**
 * Herschaalt een recept als geheel, met één factor voor alles.
 *
 * Nodig omdat AH de voedingswaarde per portie meelevert maar niet per
 * ingredient. De solver kan dan niets: die verdeelt juist over ingredienten.
 * Alles met dezelfde factor schalen is wél eerlijk — je maakt gewoon een grotere
 * of kleinere pan — en gebruikt het cijfer waarvan we zeker weten dat het klopt,
 * in plaats van een optelsom van half gematchte producten.
 */
export function planUniform(
  resolved: ResolvedRecipe,
  totalForRecipe: Nutrients,
  targets: MacroTarget[],
  opts: PlanOptions = {},
): Plan {
  const portions = opts.portions ?? 1;
  const servings = resolved.recipe.servings > 0 ? resolved.recipe.servings : 1;

  const perPortionBase: Nutrients = {};
  for (const [key, value] of Object.entries(totalForRecipe)) {
    perPortionBase[key as keyof Nutrients] = value / servings;
  }

  const scale = bestUniformScale(perPortionBase, targets, {
    min: opts.minScale ?? 0.4,
    max: opts.maxScale ?? 2.5,
  });

  const perPortion: Nutrients = {};
  for (const [key, value] of Object.entries(perPortionBase)) {
    perPortion[key as keyof Nutrients] = value * scale;
  }

  const ingredients: PlannedIngredient[] = resolved.ingredients.map((ing) => {
    const originalGrams = (ing.grams / servings) * portions;
    return {
      name: ing.raw.name,
      originalGrams: round(originalGrams),
      grams: round(originalGrams * scale),
      scale: Math.round(scale * 100) / 100,
      // De verdeling over de ingredienten kennen we niet; alleen het totaal.
      nutrients: {},
      productTitle: ing.product?.title ?? null,
      matchScore: ing.matchScore,
      unmatched: !ing.product || Object.keys(ing.product.per100g).length === 0,
      originalQuantity: ing.raw.quantity !== null ? round((ing.raw.quantity / servings) * portions) : null,
      unit: ing.raw.unit,
      gramsSource: ing.gramsSource,
    };
  });

  const totals: Nutrients = {};
  for (const [key, value] of Object.entries(perPortion)) {
    totals[key as keyof Nutrients] = value * portions;
  }

  return {
    recipeId: resolved.recipe.id,
    title: resolved.recipe.title,
    url: resolved.recipe.url,
    imageUrl: resolved.recipe.imageUrl,
    portions,
    ingredients,
    totals,
    perPortion,
    cost: targetCost(perPortion, targets),
    // De totalen komen van AH zelf, dus hier valt niets te schatten.
    coverage: 1,
    scalingMode: "uniform",
  };
}

/**
 * De factor die de doelen het dichtst benadert. Voor de gewone doelen is dat een
 * kleinsteklassenoplossing in één onbekende; daarna worden de maxima en minima
 * erop losgelaten, want die zijn harde grenzen en geen wensen.
 */
function bestUniformScale(
  perPortion: Nutrients,
  targets: MacroTarget[],
  bounds: { min: number; max: number },
): number {
  let numerator = 0;
  let denominator = 0;
  for (const target of targets) {
    if (target.mode !== "target") continue;
    const value = perPortion[target.key] ?? 0;
    if (value <= 0) continue;
    const weight = target.weight ?? 1;
    numerator += weight * value * target.value;
    denominator += weight * value * value;
  }

  let scale = denominator > 0 ? numerator / denominator : 1;

  for (const target of targets) {
    const value = perPortion[target.key] ?? 0;
    if (value <= 0) continue;
    if (target.mode === "max") scale = Math.min(scale, target.value / value);
    if (target.mode === "min") scale = Math.max(scale, target.value / value);
  }

  return Math.min(bounds.max, Math.max(bounds.min, scale));
}

/**
 * Dezelfde kostenmaat als de solver, zodat plannen onderling vergelijkbaar zijn.
 * Ook bruikbaar vóór het herschalen: hoe goed past dit recept al zoals het
 * geschreven staat, zonder dat er iets aan gerekend is.
 */
export function targetCost(perPortion: Nutrients, targets: MacroTarget[]): number {
  let cost = 0;
  for (const target of targets) {
    const value = perPortion[target.key] ?? 0;
    const diff =
      target.mode === "max"
        ? Math.max(0, value - target.value)
        : target.mode === "min"
          ? Math.max(0, target.value - value)
          : value - target.value;
    // Delen door het doel maakt de macro's onderling vergelijkbaar.
    const scaled = diff / Math.max(1, target.value);
    cost += (target.weight ?? 1) * scaled * scaled;
  }
  return cost;
}

function coverageOfPlan(ingredients: PlannedIngredient[]): number {
  let total = 0;
  let matched = 0;
  for (const ing of ingredients) {
    total += ing.originalGrams;
    if (!ing.unmatched) matched += ing.originalGrams;
  }
  return total > 0 ? matched / total : 0;
}

function round(n: number): number {
  return Math.round(n * 10) / 10;
}

/**
 * Ranks candidate recipes by how well they can be bent to the targets. A recipe
 * with poor ingredient coverage is penalised: its low cost is not trustworthy.
 */
export function rankPlans(plans: Plan[]): Plan[] {
  return [...plans].sort((a, b) => {
    const penalty = (p: Plan) => p.cost + (1 - p.coverage) * 2;
    return penalty(a) - penalty(b);
  });
}

/** Builds the solver's target list from the simple numbers the UI collects. */
export function buildTargets(input: {
  protein?: number;
  kcal?: number;
  carbs?: number;
  fat?: number;
  fiber?: number;
  kcalMode?: "target" | "max";
}): MacroTarget[] {
  const targets: MacroTarget[] = [];
  // Protein is usually the binding goal, so it outweighs the rest by default.
  if (input.protein) targets.push({ key: "protein", value: input.protein, mode: "target", weight: 2 });
  if (input.kcal) {
    targets.push({ key: "kcal", value: input.kcal, mode: input.kcalMode ?? "target", weight: 1.5 });
  }
  if (input.carbs) targets.push({ key: "carbs", value: input.carbs, mode: "target", weight: 1 });
  if (input.fat) targets.push({ key: "fat", value: input.fat, mode: "target", weight: 1 });
  if (input.fiber) targets.push({ key: "fiber", value: input.fiber, mode: "min", weight: 0.5 });
  return targets;
}
