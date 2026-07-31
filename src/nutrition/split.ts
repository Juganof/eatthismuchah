import type { DailyTargets } from "./targets";

/**
 * Verdeelt de dagdoelen over de eetmomenten. De aandelen die de gebruiker instelt
 * hoeven niet op 100% uit te komen — ze worden genormaliseerd — zodat je aan één
 * schuifje kunt draaien zonder de rest te hoeven narekenen.
 */

export interface MealSlot {
  id: string;
  name: string;
  position: number;
  /** Aandeel van de dag. Relatief: 2 en 2 is hetzelfde als 0.5 en 0.5. */
  kcalShare: number;
  /** Null = volg het kcal-aandeel. Apart instelbaar voor bv. een eiwitrijk ontbijt. */
  proteinShare: number | null;
  enabled: boolean;
  /** Zoekhints, bv. ["kwark","havermout"]. */
  tags: string[];
  maxKcal: number | null;
}

export interface SlotTargets {
  slot: MealSlot;
  targets: DailyTargets;
}

/** De verdeling waarmee een lege database begint. */
export const DEFAULT_SLOTS: MealSlot[] = [
  { id: "ontbijt", name: "Ontbijt", position: 0, kcalShare: 0.25, proteinShare: 0.3, enabled: true, tags: ["ontbijt", "kwark", "havermout"], maxKcal: null },
  { id: "lunch", name: "Lunch", position: 1, kcalShare: 0.3, proteinShare: null, enabled: true, tags: ["lunch", "salade", "brood"], maxKcal: null },
  { id: "tussendoortje", name: "Tussendoortje", position: 2, kcalShare: 0.1, proteinShare: null, enabled: true, tags: ["snack"], maxKcal: null },
  { id: "diner", name: "Diner", position: 3, kcalShare: 0.35, proteinShare: null, enabled: true, tags: ["diner"], maxKcal: null },
];

/** Aandelen naar een som van 1. Zijn ze allemaal 0, dan wordt het gelijk verdeeld. */
function normalise(shares: number[]): number[] {
  const total = shares.reduce((a, b) => a + b, 0);
  if (total <= 0) return shares.map(() => 1 / Math.max(1, shares.length));
  return shares.map((s) => s / total);
}

/**
 * Rondt af zonder dat de som verschuift: elk deel wordt afgerond en het
 * afrondingsverschil gaat naar het grootste deel. Zonder dit mis je bij vier
 * eetmomenten zomaar 3 gram eiwit op de dag.
 */
function splitRounded(total: number, shares: number[]): number[] {
  const parts = shares.map((s) => Math.round(total * s));
  const drift = Math.round(total) - parts.reduce((a, b) => a + b, 0);
  if (drift !== 0 && parts.length > 0) {
    let biggest = 0;
    for (let i = 1; i < parts.length; i++) if (parts[i]! > parts[biggest]!) biggest = i;
    parts[biggest] = parts[biggest]! + drift;
  }
  return parts;
}

/**
 * Geeft per ingeschakeld eetmoment de doelen voor dat moment. De som over alle
 * momenten is exact gelijk aan het dagdoel.
 */
export function splitTargets(daily: DailyTargets, slots: MealSlot[]): SlotTargets[] {
  const active = slots.filter((s) => s.enabled).sort((a, b) => a.position - b.position);
  if (active.length === 0) return [];

  const kcalShares = normalise(active.map((s) => Math.max(0, s.kcalShare)));
  // Een moment zonder eigen eiwitaandeel volgt zijn kcal-aandeel.
  const proteinShares = normalise(
    active.map((s, i) => (s.proteinShare === null ? kcalShares[i]! : Math.max(0, s.proteinShare))),
  );

  const kcal = splitRounded(daily.kcal, kcalShares);
  const protein = splitRounded(daily.protein, proteinShares);
  const carbs = splitRounded(daily.carbs, kcalShares);
  const fat = splitRounded(daily.fat, kcalShares);
  const fiber = splitRounded(daily.fiber, kcalShares);

  return active.map((slot, i) => ({
    slot,
    targets: {
      // Een hard maximum op een moment wint van het berekende aandeel.
      kcal: slot.maxKcal !== null ? Math.min(kcal[i]!, Math.round(slot.maxKcal)) : kcal[i]!,
      protein: protein[i]!,
      carbs: carbs[i]!,
      fat: fat[i]!,
      fiber: fiber[i]!,
    },
  }));
}

/** Wat er van het dagdoel overblijft na de al ingevulde momenten. */
export function remainingTargets(daily: DailyTargets, eaten: DailyTargets[]): DailyTargets {
  const sum = (key: keyof DailyTargets) => eaten.reduce((a, e) => a + (e[key] ?? 0), 0);
  return {
    kcal: Math.max(0, Math.round(daily.kcal - sum("kcal"))),
    protein: Math.max(0, Math.round(daily.protein - sum("protein"))),
    carbs: Math.max(0, Math.round(daily.carbs - sum("carbs"))),
    fat: Math.max(0, Math.round(daily.fat - sum("fat"))),
    fiber: Math.max(0, Math.round(daily.fiber - sum("fiber"))),
  };
}
