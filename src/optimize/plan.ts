import { NUTRIENT_KEYS, type Nutrients, type ResolvedIngredient, type ResolvedRecipe } from "../ah/types";
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
  /** Webshop-id van het gekoppelde product, zodat de UI ernaartoe kan linken. */
  productId: string | null;
  /** Verpakking zoals AH die noemt, bv. "500 g" — dat is wat je in de winkel pakt. */
  productSize: string | null;
  matchScore: number;
  /** True when nutrition is unknown, so the solver had nothing to go on. */
  unmatched: boolean;
  /** Waar de cijfers van deze regel vandaan komen; zie ResolvedIngredient. */
  nutrientSource: "product" | "nul" | "geschat" | "onbekend";
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
}

/**
 * Ingredients that should not be freely rescaled: seasonings dominate nothing
 * nutritionally but doubling the salt ruins the dish, and the solver has no
 * concept of taste.
 */
const SEASONING = /peper|zout|kruid|specerij|kaneel|nootmuskaat|paprikapoeder|komijn|kerrie|bouillon|gist|bakpoeder|vanille/i;

function isLocked(name: string, opts: PlanOptions): boolean {
  return opts.locked?.some((l) => name.includes(l.toLowerCase())) ?? false;
}

/**
 * Of deze regel mee mag schalen. Alleen regels met echte cijfers achter zich —
 * een AH-productlabel ("product") of een aandeel van AH's recepttotaal
 * ("geschat") — mogen per regel worden bijgesteld. "nul" draagt niets bij,
 * "onbekend" heeft geen cijfers. Een locked regel is een eigenschap van de
 * planner-aanroep, geen van de regel: die wordt in `boundsForLine` op 1 gezet.
 */
export function isScalable(line: ResolvedIngredient): boolean {
  return line.nutrientSource === "product" || line.nutrientSource === "geschat";
}

/** Voedingswaarde per gram, als vector over de macro's. */
function densityOf(line: ResolvedIngredient): number[] {
  return NUTRIENT_KEYS.map((key) => (line.nutrients[key] ?? 0) / Math.max(1e-9, line.grams));
}

/**
 * Data-poort voor de takkeuze: hebben alle schaalbare regels dezelfde
 * dichtheidsvector (binnen epsilon, genormaliseerd per macro), dan is per-regel
 * schalen zinloos — elke regel is in verhouding identiek aan de andere, dus de
 * solver zou ze allemaal op dezelfde factor zetten. Zo'n recept (het normale
 * geval: alles "geschat" uit AH's recepttotaal) schaalt als één geheel, exact
 * zoals voorheen. Echte productlabels hebben vrijwel altijd verschillende
 * dichtheden en krijgen wél per-regel grenzen.
 */
export function sameDensity(lines: ResolvedIngredient[]): boolean {
  let reference: number[] | null = null;
  for (const line of lines) {
    if (!isScalable(line)) continue;
    const density = densityOf(line);
    if (reference === null) {
      reference = density;
      continue;
    }
    for (let k = 0; k < density.length; k++) {
      const a = density[k]!;
      const b = reference[k]!;
      if (Math.abs(a - b) > 1e-9 * Math.max(1, Math.abs(a), Math.abs(b))) return false;
    }
  }
  return true;
}

/**
 * Per-regel grenzen voor de per-ingrediënt-tak. Locked en niet-schaalbare
 * regels blijven op 1; kruiden mogen een klein beetje mee (ze verpesten het
 * gerecht snel), "naar smaak"-regels iets meer, en de rest krijgt de vaste
 * default [0.5, 2.0]. De oude ruime [0.25, 3.0] bestaat alleen nog in de
 * uniforme tak, waar één factor het hele gerecht is.
 */
function boundsForLine(line: ResolvedIngredient, opts: PlanOptions): { min: number; max: number } {
  if (isLocked(line.raw.name, opts) || !isScalable(line)) return { min: 1, max: 1 };
  if (SEASONING.test(line.raw.name)) return { min: 0.75, max: 1.5 };
  // "naar smaak": AH gaf geen hoeveelheid, dus elke gram is een schatting —
  // daar mag ruim geschaald worden, maar niet met de default mee.
  if (line.raw.quantity === null || line.gramsSource === "fallback") return { min: 0.5, max: 1.5 };
  return { min: 0.5, max: 2.0 };
}

/**
 * Schaalt het hele gerecht met één factor. Dat is dezelfde solver, maar dan met
 * het recept als één post: de uitkomst is de portiegrootte waarmee dit gerecht
 * het dichtst bij het doel komt, en die geldt voor elke regel gelijk.
 */
function solveUniform(
  ingredients: SolverIngredient[],
  targets: MacroTarget[],
  opts: PlanOptions,
): { scales: number[]; totals: Nutrients; cost: number } {
  const whole: SolverIngredient = {
    nutrients: sumNutrients(ingredients.map((i) => i.nutrients)),
    min: opts.minScale ?? 0.25,
    max: opts.maxScale ?? 3,
  };
  const result = solve([whole], targets, { shapePenalty: opts.shapePenalty });
  const scale = result.scales[0] ?? 1;
  return { scales: ingredients.map(() => scale), totals: result.totals, cost: result.cost };
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
    return { nutrients: perPortionNutrients, ...boundsForLine(ing, opts) };
  });

  // Zonder productgegevens per ingredient is de voedingswaarde per regel een
  // aandeel van AH's recepttotaal naar gewicht, geen meting. Regels dan los van
  // elkaar schalen zou verzonnen precisie zijn: "minder olie" scheelt in die
  // rekensom evenveel als "minder courgette", en dat is niet zo. Het hele
  // gerecht schaalt daarom als één geheel — dat is precies wél waar AH's cijfer
  // over gaat, want een halve portie is de helft van alles. Echte productlabels
  // ("product") hebben vrijwel altijd verschillende dichtheden; die mogen per
  // regel geschaald worden. Eén locked regel dwingt ook de per-ingrediënt-tak
  // af: de uniforme tak schaalt alles met één factor en zou de gelockte regel
  // daarin mee laten schalen.
  const hasLocked = resolved.ingredients.some((ing) => isLocked(ing.raw.name, opts));
  const uniform = !hasLocked && sameDensity(resolved.ingredients);
  const result = uniform
    ? solveUniform(solverIngredients, targets, opts)
    : solve(solverIngredients, targets, { shapePenalty: opts.shapePenalty });

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
      productId: ing.product?.webshopId ?? null,
      productSize: ing.product?.salesUnitSize ?? null,
      matchScore: ing.matchScore,
      nutrientSource: ing.nutrientSource,
      // "Geen voedingswaarde", niet "geen product": een regel die uit AH's eigen
      // recepttotaal is bijgevuld heeft cijfers, alleen geen productlabel.
      unmatched: ing.nutrientSource === "onbekend",
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
  };
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
  let known = 0;
  for (const ing of ingredients) {
    total += ing.originalGrams;
    if (!ing.unmatched) known += ing.originalGrams;
  }
  return total > 0 ? known / total : 0;
}

/** Het aandeel van het gewicht waar een écht AH-product achter zit. */
export function productCoverageOf(plan: Plan): number {
  let total = 0;
  let backed = 0;
  for (const ing of plan.ingredients) {
    total += ing.originalGrams;
    if (ing.nutrientSource === "product") backed += ing.originalGrams;
  }
  return total > 0 ? backed / total : 0;
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
