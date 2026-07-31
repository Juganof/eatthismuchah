import type { RawIngredient } from "../ah/types";

/**
 * Everything downstream works in grams, because AH nutrition is per 100 g. Recipes
 * are written in spoons, pieces and millilitres, so each of those needs a bridge.
 */

/** Volume units, in millilitres. Converted to grams via ingredient density. */
const VOLUME_ML: Record<string, number> = {
  ml: 1,
  cl: 10,
  dl: 100,
  l: 1000,
  liter: 1000,
  tl: 5, // theelepel
  theelepel: 5,
  el: 15, // eetlepel
  eetlepel: 15,
  kop: 240,
  mok: 250,
  snufje: 0.5,
  scheutje: 10,
};

const MASS_G: Record<string, number> = {
  g: 1,
  gr: 1,
  gram: 1,
  kg: 1000,
  kilo: 1000,
  pond: 500,
};

/** Units that mean "one item"; grams then come from PIECE_WEIGHTS. */
const PIECE_UNITS = new Set([
  "stuk",
  "stuks",
  "st",
  "x",
  "teen",
  "tenen",
  "blik",
  "blikje",
  "pak",
  "pakje",
  "bosje",
  "bos",
  "krop",
  "plak",
  "plakken",
  "takje",
  "takjes",
]);

/**
 * Typical edible weight per piece, in grams. Only common Dutch recipe items — the
 * list is deliberately short, and anything missing falls back to PIECE_DEFAULT_G.
 */
const PIECE_WEIGHTS: Record<string, number> = {
  ui: 110,
  sjalot: 30,
  knoflook: 5,
  knoflookteen: 5,
  teen: 5,
  ei: 55,
  eieren: 55,
  citroen: 100,
  limoen: 60,
  sinaasappel: 150,
  appel: 150,
  banaan: 120,
  tomaat: 100,
  trostomaat: 90,
  paprika: 150,
  courgette: 250,
  aubergine: 300,
  wortel: 80,
  winterpeen: 150,
  aardappel: 120,
  zoeteaardappel: 200,
  komkommer: 350,
  avocado: 150,
  prei: 200,
  bleekselderij: 60,
  venkel: 250,
  broccoli: 400,
  bloemkool: 600,
  kipfilet: 150,
  kippendij: 100,
  zalmfilet: 125,
  boterham: 35,
  wrap: 60,
  tortilla: 60,
  blik: 400,
  blikje: 400,
  pak: 500,
  pakje: 250,
  bosje: 25,
  bos: 25,
  krop: 300,
  plak: 20,
  takje: 3,
};

const PIECE_DEFAULT_G = 100;

/**
 * Density in g/ml for ingredients where it matters. Water is 1.0 and that is a fine
 * approximation for most watery things, so only genuine outliers are listed.
 */
const DENSITY_G_PER_ML: Record<string, number> = {
  olie: 0.92,
  olijfolie: 0.92,
  zonnebloemolie: 0.92,
  sesamolie: 0.92,
  boter: 0.91,
  honing: 1.42,
  stroop: 1.4,
  siroop: 1.33,
  suiker: 0.85,
  bloem: 0.53,
  meel: 0.53,
  rijst: 0.85,
  havermout: 0.4,
  room: 1.01,
  melk: 1.03,
  yoghurt: 1.03,
  kwark: 1.05,
  sojasaus: 1.2,
  azijn: 1.01,
  zout: 1.2,
};

export interface GramsResult {
  grams: number;
  source: "explicit" | "volume" | "piece" | "spoon" | "fallback";
}

function normaliseUnit(unit: string | null): string {
  return (unit ?? "").toLowerCase().replace(/\.$/, "").trim();
}

/**
 * Whether a word is a real unit (g, el, stuk, ...) rather than a stray adjective a
 * naive "quantity, word, rest" split would otherwise mistake for one — e.g. "rijpe"
 * in "3 rijpe bananen".
 */
export function isKnownUnit(word: string): boolean {
  const unit = normaliseUnit(word);
  return unit in MASS_G || unit in VOLUME_ML || unit === "x" || PIECE_UNITS.has(unit);
}

/** Picks the best density match by looking for any known word inside the name. */
export function densityFor(name: string): number {
  const n = name.toLowerCase();
  let best = 1.0;
  let bestLen = 0;
  for (const [word, density] of Object.entries(DENSITY_G_PER_ML)) {
    if (n.includes(word) && word.length > bestLen) {
      best = density;
      bestLen = word.length;
    }
  }
  return best;
}

/** Same longest-word-wins lookup, for per-piece weights. */
export function pieceWeightFor(name: string): number {
  const n = name.toLowerCase().replace(/\s+/g, "");
  let best = PIECE_DEFAULT_G;
  let bestLen = 0;
  for (const [word, grams] of Object.entries(PIECE_WEIGHTS)) {
    if (n.includes(word) && word.length > bestLen) {
      best = grams;
      bestLen = word.length;
    }
  }
  return best;
}

/**
 * Converts one recipe line to grams. Unknown units and missing quantities resolve to
 * a conservative default rather than failing, so one odd line can't sink a recipe.
 */
export function toGrams(ing: RawIngredient): GramsResult {
  const unit = normaliseUnit(ing.unit);
  const qty = ing.quantity;

  if (qty === null || !Number.isFinite(qty) || qty <= 0) {
    // "peper en zout naar smaak" and friends: negligible, but not zero-weight for
    // the solver, which needs a positive base to scale.
    return { grams: PIECE_DEFAULT_G * 0.05, source: "fallback" };
  }

  const mass = MASS_G[unit];
  if (mass !== undefined) return { grams: qty * mass, source: "explicit" };

  const ml = VOLUME_ML[unit];
  if (ml !== undefined) {
    const grams = qty * ml * densityFor(ing.name);
    const isSpoon = unit === "el" || unit === "tl" || unit === "eetlepel" || unit === "theelepel";
    return { grams, source: isSpoon ? "spoon" : "volume" };
  }

  if (unit === "" || PIECE_UNITS.has(unit)) {
    // A bare number ("2 uien") means pieces; so does an explicit piece unit.
    const per = unit === "" || unit === "x" ? pieceWeightFor(ing.name) : pieceWeightFor(unit === "teen" || unit === "tenen" ? "knoflook" : ing.name);
    return { grams: qty * per, source: "piece" };
  }

  return { grams: qty * PIECE_DEFAULT_G, source: "fallback" };
}
