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
  /**
   * Het webshop-id van het product dat AH zelf aan deze regel hangt — de
   * "bestel de ingrediënten"-link op de receptpagina. Als die er is hoeven we
   * niet te zoeken en te raden welk product bedoeld wordt: dit is AH's eigen
   * koppeling en dus per definitie de juiste.
   */
  productId?: string | null;
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
  /**
   * AH's own labels uit het `keywords`-veld van de recept-pagina, bijvoorbeeld
   * "ontbijt", "tussendoortje", "vegetarisch". Dit is AH's eigen indeling en
   * daarmee betrouwbaarder dan wat wij uit de titel kunnen raden.
   */
  keywords?: string[];
  /**
   * Voedingswaarde per portie zoals AH die zelf op de pagina zet. Als die er is,
   * hoeven we hem niet uit losse producten op te bouwen — dat scheelt tientallen
   * productzoekopdrachten en is bovendien nauwkeuriger.
   */
  nutritionPerServing?: Nutrients | null;
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
  /**
   * Waar de cijfers van deze regel vandaan komen. "product" is een echte
   * AH-productlabel, "nul" een ingredient dat niets bijdraagt (water, zout), en
   * "geschat" het deel van AH's eigen recepttotaal dat aan deze regel is
   * toegerekend omdat er geen product bij te vinden was. Zie `fillFromRecipeTotal`.
   */
  nutrientSource: "product" | "nul" | "geschat" | "onbekend";
}

export interface ResolvedRecipe {
  recipe: Recipe;
  ingredients: ResolvedIngredient[];
  /** Sum over all ingredients, for the whole recipe. */
  total: Nutrients;
  /**
   * "products" = helemaal opgebouwd uit gematchte AH-producten. "ah" = het gat
   * dat de producten lieten vallen is gevuld met AH's eigen voedingswaarde van
   * de receptpagina.
   */
  source: "ah" | "products";
}
