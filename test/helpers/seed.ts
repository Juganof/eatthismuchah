import { Store } from "../../src/db/queries";
import type { Nutrients, Recipe } from "../../src/ah/types";

/**
 * Vult een testdatabase met recepten zoals ze na een scrape in productie staan:
 * ingredienten plus AH's eigen voedingswaarde per portie. De `per100g`-waarden
 * hieronder zijn alleen het rekengereedschap waarmee die voedingswaarde wordt
 * opgebouwd — de app kent ze niet meer, want er wordt geen product meer bij een
 * ingredient gezocht.
 */

export interface SeedIngredient {
  name: string;
  /** Gram voor het hele recept. */
  grams: number;
  per100g: Nutrients;
}

export interface SeedRecipe {
  id: string;
  title: string;
  servings?: number;
  ingredients: SeedIngredient[];
}

export async function seedRecipes(store: Store, recipes: SeedRecipe[]): Promise<void> {
  for (const seed of recipes) {
    const servings = seed.servings ?? 1;
    const totals: Nutrients = { kcal: 0, protein: 0, carbs: 0, fat: 0, fiber: 0 };
    for (const ingredient of seed.ingredients) {
      const factor = ingredient.grams / 100;
      for (const key of ["kcal", "protein", "carbs", "fat", "fiber"] as const) {
        totals[key] = (totals[key] ?? 0) + (ingredient.per100g[key] ?? 0) * factor;
      }
    }

    const perServing: Nutrients = {};
    for (const key of ["kcal", "protein", "carbs", "fat", "fiber"] as const) {
      perServing[key] = (totals[key] ?? 0) / servings;
    }

    const recipe: Recipe = {
      id: seed.id,
      title: seed.title,
      url: `https://www.ah.nl/allerhande/recept/${seed.id}`,
      servings,
      imageUrl: null,
      ingredients: seed.ingredients.map((i) => ({ name: i.name, quantity: i.grams, unit: "g" })),
      nutritionPerServing: perServing,
    };
    await store.putRecipe(recipe);
    await store.putNutrition(seed.id, totals, 1, "ah");
  }
}

/** Een paar realistische producten om recepten mee samen te stellen. */
export const FOODS: Record<string, Nutrients> = {
  kwark: { kcal: 57, protein: 10, carbs: 4, fat: 0.2, fiber: 0 },
  havermout: { kcal: 375, protein: 13, carbs: 58, fat: 7, fiber: 10 },
  kipfilet: { kcal: 106, protein: 23, carbs: 0, fat: 1.4, fiber: 0 },
  rijst: { kcal: 354, protein: 7, carbs: 78, fat: 1, fiber: 1.4 },
  zalm: { kcal: 202, protein: 20, carbs: 0, fat: 13, fiber: 0 },
  broccoli: { kcal: 34, protein: 3, carbs: 4, fat: 0.4, fiber: 3 },
  olijfolie: { kcal: 900, protein: 0, carbs: 0, fat: 100, fiber: 0 },
  banaan: { kcal: 89, protein: 1.1, carbs: 23, fat: 0.3, fiber: 2.6 },
};
