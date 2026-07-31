import type { AhClient } from "../ah/client";
import type { Nutrients } from "../ah/types";
import type { SlotCandidate, Store } from "../db/queries";
import { resolveRecipe } from "../nutrition/resolve";
import { splitTargets, type MealSlot } from "../nutrition/split";
import type { DailyTargets } from "../nutrition/targets";
import { buildTargets, planRecipe, type Plan } from "./plan";

/**
 * Stelt een hele dag samen: per eetmoment het recept dat, na herschalen, het
 * dichtst bij het doel van dat moment komt.
 *
 * Twee dingen maken dit meer dan vier losse zoekopdrachten:
 *
 *   * Wat het ene moment te veel of te weinig oplevert, wordt verrekend met de
 *     momenten die nog komen. Zonder die doorschuif klopt elk moment ongeveer
 *     en de dag als geheel nergens.
 *   * Een recept dat al op de dag staat, valt af voor de volgende momenten —
 *     twee keer dezelfde pasta is geen dagmenu.
 */

export interface DayMeal {
  slotId: string;
  slotName: string;
  position: number;
  /** Het doel dat dit moment kreeg, inclusief doorgeschoven rest. */
  targets: DailyTargets;
  /** De zoekhints van dit moment, zodat herrollen ze mee kan sturen. */
  slotTags: string[];
  plan: Plan | null;
  /** Ingevuld wanneer er geen recept gevonden werd. */
  note?: string;
}

export interface DayPlan {
  date: string;
  targets: DailyTargets;
  meals: DayMeal[];
  totals: Nutrients;
  /** Doel min behaald, per macro. Positief = tekort. */
  deviation: Nutrients;
}

export interface DayOptions {
  date: string;
  slots: MealSlot[];
  daily: DailyTargets;
  diet?: string[];
  excludedTerms?: string[];
  /** Recepten die sowieso niet mogen, bv. omdat ze al op een andere dag staan. */
  excludeRecipeIds?: string[];
  kcalMode?: "target" | "max";
  candidatesPerSlot?: number;
}

const MACRO_KEYS = ["kcal", "protein", "carbs", "fat", "fiber"] as const;

/**
 * Hoeveel van de zoekhints van een eetmoment dit recept raakt. Kwark in de
 * ochtend hoort daaraan te voldoen, een ovenschotel niet.
 */
export function tagAffinity(candidate: SlotCandidate, slotTags: string[]): number {
  if (slotTags.length === 0) return 0;
  const haystack = [candidate.title.toLowerCase(), ...candidate.tags].join(" ");
  let hits = 0;
  for (const tag of slotTags) if (tag && haystack.includes(tag)) hits++;
  return hits;
}

/**
 * Rangschikt kandidaten voor een moment. De solverkosten zijn de basis; daar
 * bovenop tellen dingen die de solver niet kan weten: of het gerecht bij dit
 * eetmoment past, hoe betrouwbaar de voedingswaarde is, of je het lekker vindt,
 * en of je het pas nog at.
 *
 * De hints zijn een voorkeur en geen filter: liever een dagmenu dat qua macro's
 * klopt met een ongebruikelijk ontbijt, dan een gat omdat er niets met kwark in
 * de database staat.
 */
function scoreOf(plan: Plan, candidate: SlotCandidate, slotTags: string[]): number {
  let score = plan.cost + (1 - plan.coverage) * 2;
  score -= Math.min(tagAffinity(candidate, slotTags), 2) * 0.8;
  if (candidate.favourite) score -= 0.75;
  score += Math.min(candidate.recentUses, 4) * 0.6;
  return score;
}

/** Hoe ver twee macroprofielen uit elkaar liggen, relatief per macro. */
export function macroDistance(a: Nutrients, b: Nutrients): number {
  let sum = 0;
  for (const key of MACRO_KEYS) {
    const left = a[key] ?? 0;
    const right = b[key] ?? 0;
    const scale = Math.max(Math.abs(right), 1);
    sum += ((left - right) / scale) ** 2;
  }
  return Math.sqrt(sum);
}

/** Plant één moment: haalt kandidaten op, herschaalt ze en kiest de beste. */
async function planSlot(
  store: Store,
  client: AhClient,
  targets: DailyTargets,
  options: {
    diet?: string[];
    excludedTerms?: string[];
    excludeRecipeIds: string[];
    kcalMode?: "target" | "max";
    limit: number;
    /** Zoekhints van het eetmoment, bv. ["kwark","havermout"]. */
    slotTags?: string[];
    /** Gezet bij herrollen: kies iets met vergelijkbare macro's als dit profiel. */
    similarTo?: Nutrients;
  },
): Promise<Plan | null> {
  const candidates = await store.shortlistForSlot({
    kcalPerPortion: targets.kcal,
    diet: options.diet,
    excludedTerms: options.excludedTerms,
    excludeRecipeIds: options.excludeRecipeIds,
    limit: options.limit,
  });
  if (candidates.length === 0) return null;

  const macroTargets = buildTargets({ ...targets, kcalMode: options.kcalMode });
  let best: { plan: Plan; score: number } | null = null;

  for (const candidate of candidates) {
    const recipe = await store.getRecipe(candidate.id);
    if (!recipe || recipe.ingredients.length === 0) continue;
    // Alle producten staan al in de cache, dus dit doet geen netwerk.
    const resolved = await resolveRecipe(recipe, client, store);
    const plan = planRecipe(resolved, macroTargets, { portions: 1 });

    let score = scoreOf(plan, candidate, options.slotTags ?? []);
    // Bij herrollen weegt gelijkenis met het vervangen gerecht net zo zwaar als
    // het doel zelf: je vroeg om iets anders, niet om iets heel anders.
    if (options.similarTo) score += macroDistance(plan.perPortion, options.similarTo) * 1.5;

    if (!best || score < best.score) best = { plan, score };
  }

  return best?.plan ?? null;
}

/** Doel min behaald, per macro. */
function deviationOf(targets: DailyTargets, totals: Nutrients): Nutrients {
  const out: Nutrients = {};
  for (const key of MACRO_KEYS) {
    out[key] = Math.round(((targets[key] ?? 0) - (totals[key] ?? 0)) * 10) / 10;
  }
  return out;
}

function sumPerPortion(meals: DayMeal[]): Nutrients {
  const out: Nutrients = {};
  for (const key of MACRO_KEYS) {
    let sum = 0;
    for (const meal of meals) sum += meal.plan?.perPortion[key] ?? 0;
    out[key] = Math.round(sum * 10) / 10;
  }
  return out;
}

export async function generateDay(
  store: Store,
  client: AhClient,
  options: DayOptions,
): Promise<DayPlan> {
  const base = splitTargets(options.daily, options.slots);
  const chosen = [...(options.excludeRecipeIds ?? [])];
  const meals: DayMeal[] = [];

  // Wat de al geplande momenten te weinig (positief) of te veel opleverden.
  const carry: DailyTargets = { kcal: 0, protein: 0, carbs: 0, fat: 0, fiber: 0 };

  for (const [index, entry] of base.entries()) {
    const slotsLeft = base.length - index;
    // Verdeel de opgelopen rest over de momenten die nog komen, dit moment
    // meegerekend — in één keer inhalen zou het eerstvolgende moment vervormen.
    const targets: DailyTargets = {
      kcal: Math.max(0, Math.round(entry.targets.kcal + carry.kcal / slotsLeft)),
      protein: Math.max(0, Math.round(entry.targets.protein + carry.protein / slotsLeft)),
      carbs: Math.max(0, Math.round(entry.targets.carbs + carry.carbs / slotsLeft)),
      fat: Math.max(0, Math.round(entry.targets.fat + carry.fat / slotsLeft)),
      fiber: Math.max(0, Math.round(entry.targets.fiber + carry.fiber / slotsLeft)),
    };

    const plan = await planSlot(store, client, targets, {
      diet: options.diet,
      excludedTerms: options.excludedTerms,
      excludeRecipeIds: chosen,
      kcalMode: options.kcalMode,
      limit: options.candidatesPerSlot ?? 25,
      slotTags: entry.slot.tags,
    });

    if (plan) {
      chosen.push(plan.recipeId);
      for (const key of MACRO_KEYS) {
        carry[key] += (targets[key] ?? 0) - (plan.perPortion[key] ?? 0);
      }
    }

    meals.push({
      slotId: entry.slot.id,
      slotName: entry.slot.name,
      position: entry.slot.position,
      targets,
      slotTags: entry.slot.tags,
      plan,
      ...(plan ? {} : { note: "geen passend recept in de database — scrape er meer met /api/ingest" }),
    });
  }

  const totals = sumPerPortion(meals);
  return {
    date: options.date,
    targets: options.daily,
    meals,
    totals,
    deviation: deviationOf(options.daily, totals),
  };
}

export interface RerollOptions {
  targets: DailyTargets;
  /** Het huidige gerecht plus alles wat je al weggeklikt hebt. */
  excludeRecipeIds: string[];
  /** Macro's van het vervangen gerecht; het alternatief blijft hier dichtbij. */
  similarTo?: Nutrients;
  /** Zoekhints van het eetmoment waar dit voor is. */
  slotTags?: string[];
  diet?: string[];
  excludedTerms?: string[];
  kcalMode?: "target" | "max";
  candidates?: number;
}

/** Zoekt een ander recept voor één moment, met vergelijkbare macro's. */
export async function rerollSlot(
  store: Store,
  client: AhClient,
  options: RerollOptions,
): Promise<Plan | null> {
  return await planSlot(store, client, options.targets, {
    diet: options.diet,
    excludedTerms: options.excludedTerms,
    excludeRecipeIds: options.excludeRecipeIds,
    kcalMode: options.kcalMode,
    limit: options.candidates ?? 30,
    slotTags: options.slotTags,
    similarTo: options.similarTo,
  });
}
