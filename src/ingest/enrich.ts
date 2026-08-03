import { GqlClient, type ProductSuggestion } from "../ah/gql";
import type { Recipe } from "../ah/types";
import { isBudgetError } from "../ah/client";
import type { ScrapeEnv } from "./pipeline";
import { Store } from "../db/queries";
import { tokenize, isNutritionFree } from "../nutrition/resolve";

/**
 * De tweede helft van een recept binnenhalen: de echte producten.
 *
 * `completeRecipe` slaat een recept op met AH's eigen voedingswaarde per
 * portie — dat is het anker van de planner. Deze stap voegt daar per regel het
 * webshopproduct aan toe dat AH zelf voorstelt (de "bestel de ingrediënten"-
 * koppeling via /gql), mét de voedingswaarde per 100 g van dat product.
 *
 * Dat levert drie dingen op: de boodschappenlijst krijgt automatisch
 * productlinks, en de planner kan per regel gemeten voedingswaarden gebruiken
 * in plaats van alleen een gewichtsverdeling.
 *
 * Een verrijkt recept kost één verzoek voor de suggesties plus één per uniek
 * product (een handvol). Producten worden in de database bewaard, dus na een
 * tijdje kost een recept alleen nog de suggesties. Raakt het budget op of
 * blokkeert AH ons, dan is dat geen ramp: het recept staat er al, compleet met
 * AH's eigen voedingswaarde.
 */

export interface EnrichResult {
  /** Recept-regels waarvoor een product-suggestie was en die een koppeling kregen. */
  matched: number;
  /** Producten waarvan de voedingswaarde nu is opgeslagen. */
  products: number;
  /** Producten die al in de database stonden en dus niets kostten. */
  cached: number;
  errors: string[];
}

export interface EnrichOptions {
  /** Rust tussen twee verzoeken aan ah.nl; zie sharedPace. */
  minIntervalMs?: number;
  /** Wachttijd voor de eerste herkansing binnen een ronde. */
  backoffMs?: number;
  /** Bovengrens op het aantal verzoeken aan /gql voor deze aanroep. */
  maxRequests?: number;
}

/**
 * Koppelt suggesties aan recept-regels. Eerst op genormaliseerde naam (zodat
 * "verse basilicum" en "basilicum" elkaar vinden). Pas als dat nergens raakt,
 * valt het terug op de volgorde — en dan alleen als de suggestie minstens één
 * woord gemeen heeft met de regel. AH slaat regels namelijk wel eens over
 * (bijv. water), waardoor de suggestielijst verschoven is; blind op volgorde
 * koppelen zet dan elk product één plek te laat neer ("water" kreeg zo de
 * chili-olie van de regel ernaast).
 */
export function matchSuggestionsToIngredients(
  ingredients: Recipe["ingredients"],
  suggestions: ProductSuggestion[],
): (ProductSuggestion | null)[] {
  const out: (ProductSuggestion | null)[] = new Array(ingredients.length).fill(null);

  const byKey = new Map<string, number[]>();
  suggestions.forEach((s, i) => {
    const key = tokenize(s.ingredientName).join(" ");
    if (!key) return;
    const bucket = byKey.get(key);
    if (bucket) bucket.push(i);
    else byKey.set(key, [i]);
  });

  const used = new Set<number>();
  ingredients.forEach((ing, index) => {
    const key = tokenize(ing.name).join(" ");
    const pool = key ? byKey.get(key) ?? [] : [];
    const pick = pool.find((i) => !used.has(i));
    if (pick !== undefined) {
      used.add(pick);
      out[index] = suggestions[pick]!;
      return;
    }
    if (index < suggestions.length && !used.has(index)) {
      const candidate = suggestions[index]!;
      if (sharesToken(key, tokenize(candidate.ingredientName).join(" "))) {
        used.add(index);
        out[index] = candidate;
      }
    }
  });
  return out;
}

/** Of twee genormaliseerde namen minstens één woord gemeen hebben. */
function sharesToken(a: string, b: string): boolean {
  if (!a || !b) return false;
  const words = new Set(a.split(" "));
  return b.split(" ").some((w) => words.has(w));
}

/**
 * Haalt de producten bij één recept en bewaart ze. Gooit nooit: alles wat
 * misgaat wordt gelogd en in `errors` gerapporteerd — het recept zelf is al
 * veilig opgeslagen voordat deze stap begint.
 */
export async function enrichRecipeWithProducts(
  env: ScrapeEnv,
  store: Store,
  recipe: Recipe,
  options: EnrichOptions = {},
): Promise<EnrichResult> {
  const result: EnrichResult = { matched: 0, products: 0, cached: 0, errors: [] };
  const client = new GqlClient(env.AH_USER_AGENT, {
    minIntervalMs: options.minIntervalMs ?? 700,
    backoffMs: options.backoffMs ?? 8000,
    maxRequests: options.maxRequests ?? 20,
  });

  const fail = (message: string, detail?: Record<string, unknown>): void => {
    result.errors.push(message);
    void store.log("warn", "ingest", `producten voor ${recipe.title} niet compleet`, {
      recept: recipe.id,
      ...detail,
    });
  };

  let suggestions: ProductSuggestion[];
  try {
    suggestions = await client.suggestionsForRecipe(recipe.id, recipe.servings);
  } catch (err) {
    if (!isBudgetError(err)) fail("suggesties: " + (err instanceof Error ? err.message : String(err)));
    return result;
  }

  const perIngredient = matchSuggestionsToIngredients(recipe.ingredients, suggestions);
  const seen = new Set<string>();

  for (const [index, ingredient] of recipe.ingredients.entries()) {
    // Vrije ingrediënten (water, zout, peper) krijgen nooit een koppeling:
    // ze leveren geen voedingswaarde op en een suggestie ernaast is per
    // definitie een vergissing.
    if (isNutritionFree(ingredient.name)) continue;
    const suggestion = perIngredient[index];
    if (!suggestion?.productId) continue;

    result.matched++;
    // De koppeling op naam is lokaal en kost niets; de boodschappenlijst hangt
    // hieraan. AH's eigen suggestie is per definitie de juiste: score 1.
    await store.putMatch(ingredient.name, suggestion.productId, 1);

    if (seen.has(suggestion.productId)) continue;
    seen.add(suggestion.productId);

    if (await store.getProduct(suggestion.productId)) {
      result.cached++;
      continue;
    }

    let nutrition: Awaited<ReturnType<GqlClient["productNutrition"]>>;
    try {
      nutrition = await client.productNutrition(suggestion.productId);
    } catch (err) {
      // Budget op: de koppelingen op naam zijn al opgeslagen en kostten niets;
      // alleen het product zelf blijft liggen. De resterende regels krijgen hun
      // lokale koppeling nog wel.
      if (isBudgetError(err)) continue;
      fail("product " + suggestion.productId + ": " + (err instanceof Error ? err.message : String(err)));
      continue;
    }

    // Ook zonder voedingswaarde bewaren we het product: dan weten we dat we het
    // al opgehaald hebben en kost het volgende recept er geen verzoek aan.
    await store.putProduct({
      webshopId: suggestion.productId,
      title: suggestion.productTitle ?? suggestion.productId,
      salesUnitSize: suggestion.salesUnitSize,
      per100g: nutrition?.per100g ?? {},
    });
    result.products++;
  }

  return result;
}
