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
import { bestMatch, headTermFor, scoreMatch, searchTermFor, tokenize } from "./match";
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

export type MatchOutcome = "matched" | "no-match";

/**
 * Zoekt het product voor één ingredientnaam en onthoudt de uitkomst — gedeeld
 * door plannen (via `resolveProduct`, hieronder) en de achtergrond-enrichment
 * (`enrichIngredientMatches` in `src/ingest/pipeline.ts`). Gooit door bij een
 * netwerk- of blokkadefout, zodat de aanroeper die apart kan tellen; alleen een
 * bevestigd "geen product gevonden" wordt als negatieve match onthouden.
 */
export async function lookupIngredientMatch(
  name: string,
  client: AhClient,
  store: Store,
): Promise<{ outcome: MatchOutcome; product: Product | null; score: number }> {
  let candidates = await client.searchProducts(searchTermFor(name), 8);
  let match = bestMatch(name, candidates);

  // Tweede poging op de kernnaam. "middelgroot scharrelei" tegen "AH
  // Scharreleieren" scoort 0,38 — net onder de drempel, puur omdat "middelgroot"
  // nergens op slaat. Op "scharrelei" alleen is het 0,60. Er wordt dan ook op de
  // kernnaam gescoord: we hebben er bewust op gezocht, dus de rest van de regel
  // afstraffen zou het antwoord opnieuw wegdrukken. Alleen als de eerste poging
  // faalde, dus hooguit één extra verzoek per ingredientnaam.
  const head = match ? null : headTermFor(name);
  if (head) {
    const extra = await client.searchProducts(head, 8);
    const better = bestMatch(head, extra);
    if (better) {
      candidates = extra;
      match = better;
    }
  }

  if (!match) {
    // Waaróm er niets matchte is het enige spoor dat we hebben als een recept
    // hierop sneuvelt: aan de drempel, aan de zoekterm, of aan AH zelf.
    await store.log("info", "match", `"${name}" geen match`, {
      kandidaten: candidates.length,
      titels: candidates.slice(0, 3).map((c) => c.title),
      beste: Math.round(bestScoreAmong(head ?? name, candidates) * 100) / 100,
      ...(head ? { ookGezochtOp: head } : {}),
    });
    await store.putMatch(name, null, 0);
    return { outcome: "no-match", product: null, score: 0 };
  }

  // Het zoekresultaat draagt titel en verpakking al; alleen de voedingswaarde
  // ontbreekt, en die komt van de productpagina. Eén verzoek dus, geen twee.
  let full = await store.getProduct(match.product.webshopId);
  if (!full) {
    const per100g = await client.getProductNutrition(match.product.webshopId);
    full = per100g.kcal !== undefined
      ? { ...match.product, per100g }
      // Staat er niets op de pagina, dan alsnog de detail-API proberen: die
      // leverde de voedingswaarde vroeger wel, en als AH dat herstelt willen we
      // niet vastzitten aan de dure route.
      : await client.getProduct(match.product.webshopId);
    if (full) await store.putProduct(full);
  }
  // Geen bruikbaar productdetail: niet als negatieve match onthouden, want dit is
  // net zo goed "probeer het later nog eens" als een netwerkfout.
  if (!full) return { outcome: "no-match", product: null, score: 0 };

  await store.putMatch(name, full.webshopId, match.score);
  return { outcome: "matched", product: full, score: match.score };
}

/** De hoogste score onder de kandidaten, ook als die onder de drempel bleef. */
function bestScoreAmong(name: string, candidates: Product[]): number {
  let best = 0;
  for (const candidate of candidates) best = Math.max(best, scoreMatch(name, candidate.title));
  return best;
}

/**
 * Haalt het product op waar AH de receptregel zelf aan koppelt. Geen zoekopdracht,
 * geen scorefunctie: één detailaanroep (of nul, als we het product al kennen).
 * Score 1, want er valt hier niets te raden.
 */
export async function lookupLinkedProduct(
  name: string,
  productId: string,
  client: AhClient,
  store: Store,
): Promise<Product | null> {
  let product = await store.getProduct(productId);
  if (!product) {
    product = await client.getProduct(productId);
    if (product) await store.putProduct(product);
  }
  if (!product) return null;
  await store.putMatch(name, product.webshopId, 1);
  return product;
}

/**
 * Zoekt het product voor één receptregel, cache eerst.
 *
 * Fouten gaan hier bewust omhoog in plaats van als "geen product" te eindigen.
 * Dat verschil is duur gebleken: toen een blokkade of een opgeraakt verzoekbudget
 * hier stilletjes `null` werd, keurde `firstIncomplete` een prima recept af — en
 * afgekeurd is voorgoed. Een fout betekent "later nog eens proberen"; alleen een
 * geslaagde zoekopdracht die niets bruikbaars oplevert is een echt oordeel.
 */
async function resolveProduct(
  ingredient: RawIngredient,
  client: AhClient,
  store: Store,
  cacheOnly = false,
): Promise<{ product: Product | null; score: number }> {
  // AH's eigen koppeling wint van alles wat in de cache staat: die kan uit een
  // eerdere zoekronde komen en dus een gok zijn.
  if (ingredient.productId) {
    const linked = await store.getProduct(ingredient.productId);
    if (linked) return { product: linked, score: 1 };
    if (!cacheOnly) {
      const product = await lookupLinkedProduct(
        ingredient.name,
        ingredient.productId,
        client,
        store,
      );
      if (product) return { product, score: 1 };
    }
  }

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

  const { product, score } = await lookupIngredientMatch(ingredient.name, client, store);
  return { product, score };
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

/**
 * Ingredienten waarvoor geen product hoeft te bestaan.
 *
 * Water, zout en peper staan in bijna elk recept en leveren geen calorieen. Zonder
 * deze uitzondering zou de eis "elk ingredient een echt product" vrijwel elk recept
 * afkeuren, en dan houd je een lege database over in plaats van een strenge.
 * Nul is hier geen schatting maar de waarheid.
 */
const NUTRITION_FREE = new Set([
  "water", "kraanwater", "ijswater", "ijsblokjes", "ijsklontjes", "ijs",
  "zout", "zeezout", "keukenzout", "peper", "peperkorrels",
  "azijn", "natuurazijn", "wijnazijn",
]);

/**
 * Woorden die alleen iets zeggen over de hoeveelheid, de maalgraad of de kleur.
 * "Een snuf zout" is zout en "versgemalen zwarte peper" is peper; zonder dit zou
 * de vrijstelling daar al op stuklopen. Ze staan los van de vrijstellingslijst
 * zelf, zodat "zwarte bonen" niet ineens nul calorieen krijgt.
 */
const AMOUNT_WORDS = new Set([
  "snuf", "snufje", "mespunt", "scheut", "scheutje", "beetje", "flinke", "lauw",
  "koud", "warm", "heet", "gefilterd", "bruisend", "plat", "gemalen", "grove",
  "grof", "versgemalen", "zwarte", "witte", "roze", "groene", "gebroken",
]);

/**
 * Of dit ingredient zonder product als compleet mag gelden. Vergelijkt op hele
 * woorden via dezelfde tokenizer als de matcher, zodat "peperoni" geen peper is
 * en "water" in "kokoswater" niet meetelt.
 */
export function isNutritionFree(name: string): boolean {
  const words = tokenize(name).filter((word) => !AMOUNT_WORDS.has(word));
  if (words.length === 0) return false;
  // Elk overgebleven woord moet vrijgesteld zijn: "water met citroen" is dat niet.
  return words.every((word) => NUTRITION_FREE.has(word));
}

/** Een ingredient telt als opgelost als het echte cijfers heeft, of nul hoort te zijn. */
export function isComplete(ingredient: ResolvedIngredient): boolean {
  if (isNutritionFree(ingredient.raw.name)) return true;
  return ingredient.product !== null && ingredient.product.per100g.kcal !== undefined;
}

/**
 * Het eerste ingredient dat dit recept onbruikbaar maakt, of null als alles klopt.
 * De naam gaat mee de log en `skipped_recipes` in: zo is achteraf te zien waaróp
 * een recept sneuvelde, en of de vrijstellingslijst een woord mist.
 */
export function firstIncomplete(resolved: ResolvedRecipe): string | null {
  for (const ingredient of resolved.ingredients) {
    if (!isComplete(ingredient)) return ingredient.raw.name;
  }
  return null;
}
