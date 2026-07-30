import type { Product } from "../ah/types";

/**
 * Recipe lines are written for humans ("2 rijpe trostomaten, in blokjes") while the
 * product catalogue is written for a webshop ("AH Trostomaten 500 g"). This module
 * bridges the two well enough to pick the right nutrition, and reports how sure it is.
 */

/** Preparation words and qualifiers that carry no product meaning. */
const NOISE = new Set([
  "verse", "vers", "rijpe", "rijp", "grote", "groot", "kleine", "klein", "fijne", "fijn",
  "gesneden", "gesnipperde", "gesnipperd", "geraspte", "geraspt", "gehakte", "gehakt",
  "gepelde", "gepeld", "geschilde", "geschild", "blokjes", "reepjes", "plakjes", "ringen",
  "in", "van", "de", "het", "een", "of", "en", "met", "naar", "smaak", "extra",
  "biologische", "bio", "ongezouten", "gezouten", "magere", "volle", "halfvolle",
  "ah", "huismerk", "voor", "erbij", "eventueel", "optioneel", "stuks", "stuk",
]);

/** Words whose presence in a product title but not the query is a strong mismatch. */
const DISQUALIFIERS = ["saus", "soep", "snack", "chips", "koek", "reep", "maaltijd", "pizza"];

export function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ") // drop parenthetical asides
    .split(/[^a-zàâäçéèêëîïôöûüù]+/i)
    .map((t) => t.trim())
    .filter((t) => t.length > 2 && !NOISE.has(t));
}

/**
 * Reduces a Dutch word to a comparable stem. Beyond dropping the plural suffix this
 * collapses doubled vowels, because Dutch writes the singular with a long vowel and
 * the plural with a short one — "tomaat"/"tomaten" are the same word, and without
 * this they share no characters the matcher can use.
 */
function stem(word: string): string {
  let w = word;
  if (w.endsWith("en") && w.length > 4) w = w.slice(0, -2);
  else if (w.endsWith("s") && w.length > 3) w = w.slice(0, -1);
  return w.replace(/([aeiou])\1/g, "$1");
}

function stems(text: string): Set<string> {
  return new Set(tokenize(text).map(stem));
}

/** Longest shared prefix, the useful similarity signal for Dutch compounds. */
function sharedPrefix(a: string, b: string): number {
  let i = 0;
  while (i < a.length && i < b.length && a[i] === b[i]) i++;
  return i;
}

/**
 * Scores a product against an ingredient name on 0..1. Recall matters more than
 * precision here: covering the ingredient's words is what makes it the right product.
 */
export function scoreMatch(ingredientName: string, productTitle: string): number {
  const want = stems(ingredientName);
  const have = stems(productTitle);
  if (want.size === 0 || have.size === 0) return 0;

  let hits = 0;
  for (const w of want) {
    if (have.has(w)) {
      hits += 1;
      continue;
    }
    // Partial credit for near-misses: "tomaat" vs "tomatensoep". Four characters
    // is enough to be meaningful without letting "kip" claim "kipdijfilet".
    for (const h of have) {
      if (sharedPrefix(w, h) >= 4) {
        hits += 0.6;
        break;
      }
    }
  }

  const recall = hits / want.size;
  const precision = hits / have.size;
  let score = 0.75 * recall + 0.25 * precision;

  // A ready-made product is rarely what a raw ingredient line means.
  const title = productTitle.toLowerCase();
  const query = ingredientName.toLowerCase();
  for (const bad of DISQUALIFIERS) {
    if (title.includes(bad) && !query.includes(bad)) {
      score *= 0.5;
      break;
    }
  }
  return Math.max(0, Math.min(1, score));
}

/** Confidence below this means we'd rather report "unmatched" than guess. */
export const MIN_MATCH_SCORE = 0.4;

export interface Match {
  product: Product;
  score: number;
}

/** Picks the best-scoring candidate, or null when nothing clears the threshold. */
export function bestMatch(ingredientName: string, candidates: Product[]): Match | null {
  let best: Match | null = null;
  for (const product of candidates) {
    const score = scoreMatch(ingredientName, product.title);
    if (!best || score > best.score) best = { product, score };
  }
  return best && best.score >= MIN_MATCH_SCORE ? best : null;
}

/**
 * Turns a recipe line into a product search term: the meaningful words only, so
 * "2 rijpe trostomaten, in blokjes" searches for "trostomaten".
 */
export function searchTermFor(ingredientName: string): string {
  const tokens = tokenize(ingredientName);
  return tokens.length > 0 ? tokens.slice(0, 3).join(" ") : ingredientName.trim();
}
