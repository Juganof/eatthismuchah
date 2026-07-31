import type { AhClient } from "../ah/client";
import type { Store } from "../db/queries";
import {
  NUTRIENT_KEYS,
  type Nutrients,
  type Product,
  type RawIngredient,
  type Recipe,
  type ResolvedIngredient,
  type ResolvedRecipe,
} from "../ah/types";
import { bestMatch, searchTermFor } from "./match";
import { toGrams } from "./units";

/**
 * Turns a scraped recipe into per-ingredient nutrition: each line is converted to
 * grams, matched to an AH product, and multiplied by that product's per-100g values.
 */

export function scaleNutrients(per100g: Nutrients, grams: number): Nutrients {
  const factor = grams / 100;
  const out: Nutrients = {};
  for (const key of NUTRIENT_KEYS) {
    const v = per100g[key];
    if (v !== undefined) out[key] = v * factor;
  }
  return out;
}

export function sumNutrients(items: Nutrients[]): Nutrients {
  const out: Nutrients = {};
  for (const key of NUTRIENT_KEYS) {
    let sum = 0;
    let seen = false;
    for (const item of items) {
      const v = item[key];
      if (v !== undefined) {
        sum += v;
        seen = true;
      }
    }
    if (seen) out[key] = sum;
  }
  return out;
}

/**
 * Finds the AH product for one ingredient line, consulting the cache first. Failed
 * lookups are cached too — an ingredient that has no sensible product ("water",
 * "peper naar smaak") should not trigger a search on every single request.
 */
async function resolveProduct(
  ingredient: RawIngredient,
  client: AhClient,
  store: Store,
  cacheOnly = false,
): Promise<{ product: Product | null; score: number }> {
  const cached = await store.getMatch(ingredient.name);
  if (cached !== undefined) {
    if (cached.webshopId === null) return { product: null, score: 0 };
    const product = await store.getProduct(cached.webshopId);
    if (product) return { product, score: cached.score };
  }

  // Bij plannen mag er niets naar ah.nl: dat zijn tientallen zoekopdrachten per
  // recept maal het aantal kandidaten, en met de verplichte pauzes ertussen loopt
  // dat in de minuten. Onbekend is dan gewoon onbekend.
  if (cacheOnly) return { product: null, score: 0 };

  let candidates: Product[];
  try {
    candidates = await client.searchProducts(searchTermFor(ingredient.name), 8);
  } catch {
    // A failed search must not poison the cache: leave it unrecorded so the next
    // request tries again rather than remembering a network blip as "no match".
    return { product: null, score: 0 };
  }

  const match = bestMatch(ingredient.name, candidates);
  if (!match) {
    await store.putMatch(ingredient.name, null, 0);
    return { product: null, score: 0 };
  }

  // Search results carry no nutrition, so fetch the detail record.
  let full = await store.getProduct(match.product.webshopId);
  if (!full) {
    try {
      full = await client.getProduct(match.product.webshopId);
    } catch {
      return { product: null, score: 0 };
    }
    if (full) await store.putProduct(full);
  }
  if (!full) return { product: null, score: 0 };

  await store.putMatch(ingredient.name, full.webshopId, match.score);
  return { product: full, score: match.score };
}

/**
 * `cacheOnly` maakt dit een pure databaseoperatie: geen enkele aanroep naar
 * ah.nl. Dat is wat het plannen gebruikt — daar worden tientallen recepten
 * doorgerekend en is wachten op het netwerk geen optie.
 */
export async function resolveRecipe(
  recipe: Recipe,
  client: AhClient,
  store: Store,
  options: { cacheOnly?: boolean } = {},
): Promise<ResolvedRecipe> {
  const ingredients: ResolvedIngredient[] = [];

  for (const raw of recipe.ingredients) {
    const { grams, source } = toGrams(raw);
    const { product, score } = await resolveProduct(raw, client, store, options.cacheOnly);
    ingredients.push({
      raw,
      grams,
      product,
      gramsSource: source,
      matchScore: score,
      nutrients: product ? scaleNutrients(product.per100g, grams) : {},
    });
  }

  return {
    recipe,
    ingredients,
    total: sumNutrients(ingredients.map((i) => i.nutrients)),
  };
}

/**
 * Share of the recipe's weight that we found nutrition for. This is the honest
 * confidence signal: 12 matched herbs and one unmatched 400 g chicken is bad
 * coverage even though 12 of 13 lines matched.
 */
export function coverageOf(resolved: ResolvedRecipe): number {
  let total = 0;
  let matched = 0;
  for (const ing of resolved.ingredients) {
    total += ing.grams;
    if (ing.product && Object.keys(ing.product.per100g).length > 0) matched += ing.grams;
  }
  return total > 0 ? matched / total : 0;
}
