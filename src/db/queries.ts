import type { Nutrients, Product, RawIngredient, Recipe } from "../ah/types";

/** How long cached upstream data stays usable before we refetch. */
const PRODUCT_TTL_MS = 14 * 24 * 60 * 60 * 1000;
const RECIPE_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MATCH_TTL_MS = 30 * 24 * 60 * 60 * 1000;

export interface RecipeSummary {
  id: string;
  title: string;
  url: string;
  servings: number;
  imageUrl: string | null;
  nutrition: Nutrients;
  coverage: number;
}

export class Store {
  constructor(private readonly db: D1Database) {}

  // ---------------------------------------------------------------- recipes

  async getRecipe(id: string): Promise<Recipe | null> {
    const row = await this.db
      .prepare("SELECT * FROM recipes WHERE id = ? AND fetched_at > ?")
      .bind(id, Date.now() - RECIPE_TTL_MS)
      .first<{
        id: string;
        title: string;
        url: string;
        servings: number;
        image_url: string | null;
        ingredients: string;
      }>();
    if (!row) return null;
    return {
      id: row.id,
      title: row.title,
      url: row.url,
      servings: row.servings,
      imageUrl: row.image_url,
      ingredients: JSON.parse(row.ingredients) as RawIngredient[],
    };
  }

  async putRecipe(recipe: Recipe): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO recipes (id, title, url, servings, image_url, ingredients, fetched_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           title = excluded.title,
           url = excluded.url,
           servings = excluded.servings,
           image_url = excluded.image_url,
           ingredients = excluded.ingredients,
           fetched_at = excluded.fetched_at`,
      )
      .bind(
        recipe.id,
        recipe.title,
        recipe.url,
        recipe.servings,
        recipe.imageUrl,
        JSON.stringify(recipe.ingredients),
        Date.now(),
      )
      .run();
  }

  // --------------------------------------------------------------- products

  async getProduct(webshopId: string): Promise<Product | null> {
    const row = await this.db
      .prepare("SELECT * FROM products WHERE webshop_id = ? AND fetched_at > ?")
      .bind(webshopId, Date.now() - PRODUCT_TTL_MS)
      .first<{
        webshop_id: string;
        title: string;
        sales_unit_size: string | null;
        per_100g: string;
      }>();
    if (!row) return null;
    return {
      webshopId: row.webshop_id,
      title: row.title,
      salesUnitSize: row.sales_unit_size,
      per100g: JSON.parse(row.per_100g) as Nutrients,
    };
  }

  async putProduct(product: Product): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO products (webshop_id, title, sales_unit_size, per_100g, fetched_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(webshop_id) DO UPDATE SET
           title = excluded.title,
           sales_unit_size = excluded.sales_unit_size,
           per_100g = excluded.per_100g,
           fetched_at = excluded.fetched_at`,
      )
      .bind(
        product.webshopId,
        product.title,
        product.salesUnitSize,
        JSON.stringify(product.per100g),
        Date.now(),
      )
      .run();
  }

  // ---------------------------------------------------------------- matches

  /**
   * Returns the remembered decision for an ingredient name. A stored null
   * webshop_id is a real answer — "we looked, nothing matched" — so the outer
   * `undefined` means "never looked" and the inner `null` means "no match".
   */
  async getMatch(
    ingredientName: string,
  ): Promise<{ webshopId: string | null; score: number } | undefined> {
    const row = await this.db
      .prepare(
        "SELECT webshop_id, score FROM ingredient_matches WHERE ingredient_name = ? AND matched_at > ?",
      )
      .bind(ingredientName, Date.now() - MATCH_TTL_MS)
      .first<{ webshop_id: string | null; score: number }>();
    if (!row) return undefined;
    return { webshopId: row.webshop_id, score: row.score };
  }

  async putMatch(ingredientName: string, webshopId: string | null, score: number): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO ingredient_matches (ingredient_name, webshop_id, score, matched_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(ingredient_name) DO UPDATE SET
           webshop_id = excluded.webshop_id,
           score = excluded.score,
           matched_at = excluded.matched_at`,
      )
      .bind(ingredientName, webshopId, score, Date.now())
      .run();
  }

  /** Lets the UI correct a bad automatic match; corrections outlive the TTL sweep. */
  async overrideMatch(ingredientName: string, webshopId: string): Promise<void> {
    await this.putMatch(ingredientName, webshopId, 1);
  }

  // -------------------------------------------------------------- nutrition

  async putNutrition(recipeId: string, n: Nutrients, coverage: number): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO recipe_nutrition (recipe_id, kcal, protein, carbs, fat, fiber, coverage, computed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(recipe_id) DO UPDATE SET
           kcal = excluded.kcal, protein = excluded.protein, carbs = excluded.carbs,
           fat = excluded.fat, fiber = excluded.fiber, coverage = excluded.coverage,
           computed_at = excluded.computed_at`,
      )
      .bind(
        recipeId,
        n.kcal ?? 0,
        n.protein ?? 0,
        n.carbs ?? 0,
        n.fat ?? 0,
        n.fiber ?? 0,
        coverage,
        Date.now(),
      )
      .run();
  }

  /**
   * Shortlists cached recipes for a target. Ordering by protein density rather
   * than raw protein keeps 3000 kcal party dishes out of a 700 kcal dinner slot.
   */
  async shortlist(limit: number, minCoverage = 0.5): Promise<RecipeSummary[]> {
    const { results } = await this.db
      .prepare(
        `SELECT r.id, r.title, r.url, r.servings, r.image_url,
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage
         FROM recipe_nutrition n
         JOIN recipes r ON r.id = n.recipe_id
         WHERE n.coverage >= ? AND n.kcal > 0
         ORDER BY n.protein / n.kcal DESC
         LIMIT ?`,
      )
      .bind(minCoverage, limit)
      .all<{
        id: string;
        title: string;
        url: string;
        servings: number;
        image_url: string | null;
        kcal: number;
        protein: number;
        carbs: number;
        fat: number;
        fiber: number;
        coverage: number;
      }>();

    return (results ?? []).map((r) => ({
      id: r.id,
      title: r.title,
      url: r.url,
      servings: r.servings,
      imageUrl: r.image_url,
      coverage: r.coverage,
      nutrition: {
        kcal: r.kcal,
        protein: r.protein,
        carbs: r.carbs,
        fat: r.fat,
        fiber: r.fiber,
      },
    }));
  }

  async countRecipes(): Promise<number> {
    const row = await this.db.prepare("SELECT COUNT(*) AS n FROM recipes").first<{ n: number }>();
    return row?.n ?? 0;
  }
}
