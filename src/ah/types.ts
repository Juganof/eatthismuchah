/** The five macros the planner reasons about. All in grams except kcal. */
export type NutrientKey = "kcal" | "protein" | "carbs" | "fat" | "fiber";

export const NUTRIENT_KEYS: NutrientKey[] = ["kcal", "protein", "carbs", "fat", "fiber"];

/** A nutrient vector. Missing keys are treated as 0 by the solver. */
export type Nutrients = Partial<Record<NutrientKey, number>>;

/** One line from an Allerhande ingredient list, as scraped. */
export interface RawIngredient {
  /** Free-text name, e.g. "kipfilet" or "olijfolie". */
  name: string;
  /** Numeric quantity for `unit`, per the recipe's own serving count. Null when AH gives none. */
  quantity: number | null;
  /** AH's unit string, e.g. "g", "ml", "el", "stuk". Null when AH gives none. */
  unit: string | null;
}

export interface Recipe {
  /** AH recipe id, e.g. "R-R1193067". */
  id: string;
  title: string;
  url: string;
  /** Servings the ingredient quantities refer to. */
  servings: number;
  imageUrl: string | null;
  ingredients: RawIngredient[];
}

/** An AH webshop product, with nutrition normalised to per-100g. */
export interface Product {
  webshopId: string;
  title: string;
  /** e.g. "500 g", "1 l" — used to sanity-check unit handling. */
  salesUnitSize: string | null;
  /** Nutrition per 100 g (or per 100 ml for liquids). */
  per100g: Nutrients;
}

/** A recipe ingredient after unit conversion and product matching. */
export interface ResolvedIngredient {
  raw: RawIngredient;
  /** Grams of this ingredient for the whole recipe as written. */
  grams: number;
  /** Null when no AH product could be matched — nutrition is then all zero. */
  product: Product | null;
  /** Absolute nutrients contributed at the recipe's original quantity. */
  nutrients: Nutrients;
  /** How the grams figure was derived, for display and debugging. */
  gramsSource: "explicit" | "volume" | "piece" | "spoon" | "fallback";
  /** 0..1 confidence of the ingredient→product match. */
  matchScore: number;
}

export interface ResolvedRecipe {
  recipe: Recipe;
  ingredients: ResolvedIngredient[];
  /** Sum over all ingredients, for the whole recipe. */
  total: Nutrients;
}
