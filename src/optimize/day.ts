import type { AhClient } from "../ah/client";
import type { Nutrients, Recipe, ResolvedRecipe } from "../ah/types";
import type { SlotCandidate, Store } from "../db/queries";
import { resolveRecipe, type ProductMatch } from "../nutrition/resolve";
import { splitTargets, type MealSlot } from "../nutrition/split";
import type { DailyTargets } from "../nutrition/targets";
import { buildTargets, planRecipe, targetCost, type Plan, type PlanOptions } from "./plan";

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
  /**
   * De keuzekaarten voor dit moment: maximaal zes opties, gesorteerd met de
   * beste eerst (kleinste afstand tot het momentdoel). De eerste optie is
   * hetzelfde als `plan`, zodat de UI met één tik het getoonde gerecht kiest.
   */
  options?: PlanOption[];
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
  /** Hoeveel keuzekaarten elk moment meekrijgt; 1..6, standaard 4. */
  optionsPerMeal?: number;
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

/**
 * Maakt het plan voor dit recept.
 *
 * Er valt hier niets meer te wegen: alles in de database is compleet, want
 * anders was het er niet in gekomen (zie `completeRecipe` in
 * `src/ingest/pipeline.ts`). Elk ingredient heeft echte voedingswaarde, dus de
 * solver kan gewoon zijn werk doen.
 *
 * `extraBounds` klemt elk ingredient dicht bij zijn oorspronkelijke hoeveelheid —
 * gebruikt voor een "zoals het recept"-optie die niet toevallig dicht bij 1 mag
 * uitkomen, maar dat ook echt gegarandeerd is.
 */
function planFor(
  resolved: ResolvedRecipe,
  macroTargets: ReturnType<typeof buildTargets>,
  extraBounds?: Pick<PlanOptions, "minScale" | "maxScale">,
): Plan {
  return planRecipe(resolved, macroTargets, { portions: 1, ...extraBounds });
}

export interface PlanOption {
  plan: Plan;
  /** "origineel": het recept past al zo'n beetje; "herschaald": bijgesteld naar het doel. */
  bucket: "origineel" | "herschaald";
  /** Dezelfde score als scoreOf, zodat de volgorde te verantwoorden is. */
  score: number;
}

/**
 * Onder deze kost past een recept al ongeveer bij het doel zoals het geschreven
 * staat. `buildTargets` geeft protein/kcal/carbs/fat samen een gewicht van zo'n
 * 6, dus een gelijkmatige afwijking van 10% op elke macro kost ongeveer 0.06 —
 * dat is de duimregel achter dit getal.
 */
const NEAR_FIT_COST = 0.06;

interface CandidateLoopOptions {
  diet?: string[];
  excludedTerms?: string[];
  excludeRecipeIds: string[];
  kcalMode?: "target" | "max";
  limit: number;
  /** Zoekhints van het eetmoment, bv. ["kwark","havermout"]. */
  slotTags?: string[];
  /** Gezet bij herrollen: kies iets met vergelijkbare macro's als dit profiel. */
  similarTo?: Nutrients;
  /**
   * Recepten die al dicht bij het doel zitten krijgen hun ingredienten vastgezet
   * op 0.9-1.1x, zodat een "origineel"-optie ook echt nauwelijks herschaald is.
   */
  pinNearFit: boolean;
}

/**
 * De ene kandidatenloop achter zowel het gewone plannen (`planSlot`, kiest de
 * beste) als de keuzekaarten (`planSlotOptions`/`rerollSlotOptions`, houdt ze
 * allemaal met hun score en emmer).
 */
async function planCandidates(
  store: Store,
  client: AhClient,
  targets: DailyTargets,
  options: CandidateLoopOptions,
): Promise<PlanOption[]> {
  const candidates = await store.shortlistForSlot({
    kcalPerPortion: targets.kcal,
    diet: options.diet,
    excludedTerms: options.excludedTerms,
    excludeRecipeIds: options.excludeRecipeIds,
    limit: options.limit,
  });
  if (candidates.length === 0) {
    await store.log("warn", "plan", `geen kandidaten voor ${Math.round(targets.kcal)} kcal`, {
      dieet: options.diet ?? [],
      uitgesloten: options.excludeRecipeIds.length,
    });
    return [];
  }

  const macroTargets = buildTargets({ ...targets, kcalMode: options.kcalMode });
  const results: PlanOption[] = [];

  // Eerst alle recepten van deze ronde ophalen en hun ingredientnamen
  // verzamelen, zodat één query alle onthouden productmatches levert — niet één
  // lookup per kandidaat per ingredient. De matchMap-achtige lookup is zuiver
  // databasewerk, dus plannen raakt ah.nl er niet door aan.
  const recipes = new Map<string, Recipe>();
  const names = new Set<string>();
  for (const candidate of candidates) {
    const recipe = await store.getRecipe(candidate.id);
    if (!recipe || recipe.ingredients.length === 0) continue;
    recipes.set(candidate.id, recipe);
    for (const ingredient of recipe.ingredients) names.add(ingredient.name.toLowerCase());
  }
  const allMatches = await store.productMatchesFor([...names]);

  for (const candidate of candidates) {
    const recipe = recipes.get(candidate.id);
    if (!recipe) continue;

    // De onthouden matches van deze ronde, gegroepeerd per recept: alleen de
    // namen die dit recept gebruikt. Met die gemeten productwaarden rekent
    // `resolveRecipe` per regel (nutrientSource "product") in plaats van het
    // recepttotaal naar gewicht te verdelen.
    const matches = new Map<string, ProductMatch>();
    for (const ingredient of recipe.ingredients) {
      const name = ingredient.name.toLowerCase();
      const found = allMatches.get(name);
      if (found !== undefined) matches.set(name, found);
    }

    // Puur rekenwerk: de voedingswaarde staat bij het recept, dus plannen raakt
    // ah.nl niet aan.
    const resolved = resolveRecipe(recipe, matches);

    // Kost van het recept zoals het geschreven staat, vóór herschalen — puur uit
    // het gecachte recepttotaal, dus geen extra netwerk of solve nodig.
    const servings = Math.max(candidate.servings, 1);
    const perPortionRaw: Nutrients = {};
    for (const [key, value] of Object.entries(candidate.nutrition)) {
      perPortionRaw[key as keyof Nutrients] = value / servings;
    }
    const near = targetCost(perPortionRaw, macroTargets) <= NEAR_FIT_COST;

    const plan = planFor(
      resolved,
      macroTargets,
      options.pinNearFit && near ? { minScale: 0.9, maxScale: 1.1 } : undefined,
    );

    let score = scoreOf(plan, candidate, options.slotTags ?? []);
    // Bij herrollen weegt gelijkenis met het vervangen gerecht net zo zwaar als
    // het doel zelf: je vroeg om iets anders, niet om iets heel anders.
    if (options.similarTo) score += macroDistance(plan.perPortion, options.similarTo) * 1.5;

    results.push({ plan, bucket: near ? "origineel" : "herschaald", score });
  }

  await store.log(
    "info",
    "plan",
    `${results.length} kandidaten doorgerekend (doel ${Math.round(targets.kcal)} kcal)`,
  );
  return results;
}

/** Plant één moment: haalt kandidaten op, herschaalt ze en kiest de beste. */
async function planSlot(
  store: Store,
  client: AhClient,
  targets: DailyTargets,
  options: Omit<CandidateLoopOptions, "pinNearFit">,
): Promise<Plan | null> {
  const results = await planCandidates(store, client, targets, { ...options, pinNearFit: false });
  let best: PlanOption | null = null;
  for (const result of results) if (!best || result.score < best.score) best = result;
  return best?.plan ?? null;
}

/**
 * Verdeelt gescoorde kandidaten over de kaarten die de gebruiker te zien krijgt:
 * tot 2 die al dicht bij het doel zitten, de rest bijgesteld — en is een van
 * beide emmers te klein, dan vult de andere aan in plaats van minder kaarten
 * te tonen dan gevraagd.
 *
 * De smaakscore (favoriet, recent, zoekhints) beslist wélke opties de selectie
 * halen. De volgorde die de gebruiker te zien krijgt is daar los van: die is
 * altijd op afstand tot het momentdoel, de beste eerst. Die afstand is
 * `plan.cost` — de maat die de solver zelf minimaliseert, mét de weging die
 * `buildTargets` aan de macro's geeft en mét het kcalMode-gedrag. `targetCost`
 * meet het recept zoals het geschreven staat (vóór herschalen) en
 * `macroDistance` negeert die weging; `plan.cost` zegt dus het beste wat de
 * gebruiker daadwerkelijk eet.
 */
function pickOptions(all: PlanOption[], count: number): PlanOption[] {
  const seen = new Set<string>();
  const unique = all.filter((option) => {
    if (seen.has(option.plan.recipeId)) return false;
    seen.add(option.plan.recipeId);
    return true;
  });

  const origineel = unique.filter((o) => o.bucket === "origineel").sort((a, b) => a.score - b.score);
  const herschaald = unique.filter((o) => o.bucket === "herschaald").sort((a, b) => a.score - b.score);

  const picked = origineel.slice(0, Math.min(2, count));
  const rest = [...origineel.slice(picked.length), ...herschaald].sort((a, b) => a.score - b.score);
  for (const option of rest) {
    if (picked.length >= count) break;
    picked.push(option);
  }
  // Bij gelijke kosten beslist de recipeId, zodat de volgorde deterministisch
  // blijft, los van de volgorde waarin de database de kandidaten teruggeeft.
  return picked.sort(
    (a, b) => a.plan.cost - b.plan.cost || a.plan.recipeId.localeCompare(b.plan.recipeId),
  );
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

/**
 * Het lege dagoverzicht: alle eetmomenten met hun doel, nog zonder recept. De
 * UI toont dit meteen, zodat je ziet hoe je dag eruitziet voordat er iets
 * gegenereerd is — en per moment kunt kiezen wat je invult.
 */
export function blankDay(date: string, slots: MealSlot[], daily: DailyTargets): DayPlan {
  const meals: DayMeal[] = splitTargets(daily, slots).map((entry) => ({
    slotId: entry.slot.id,
    slotName: entry.slot.name,
    position: entry.slot.position,
    targets: entry.targets,
    slotTags: entry.slot.tags,
    plan: null,
  }));

  return {
    date,
    targets: daily,
    meals,
    totals: sumPerPortion(meals),
    deviation: deviationOf(daily, sumPerPortion(meals)),
  };
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

    // Eén candidate-ronde levert én het gekozen plan én de keuzekaarten: de
    // beste optie op afstand tot het momentdoel is wat de dag krijgt, de rest
    // blijft liggen om op te tikken. Zo toont "Genereer hele dag" hetzelfde
    // soort keuze als "Genereer dit eetmoment", zonder het moment dubbel te
    // berekenen. De pincode (0.9-1.1) blijft een herrollen-ding; net als bij
    // planSlot mogen hier recepten vrij geschaald worden.
    const candidates = await planCandidates(store, client, targets, {
      diet: options.diet,
      excludedTerms: options.excludedTerms,
      excludeRecipeIds: chosen,
      kcalMode: options.kcalMode,
      limit: options.candidatesPerSlot ?? 25,
      slotTags: entry.slot.tags,
      pinNearFit: false,
    });

    const count = Math.min(Math.max(options.optionsPerMeal ?? 4, 1), 6);
    const choices = pickOptions(candidates, count);
    const plan = choices[0]?.plan ?? null;

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
      ...(plan
        ? { options: choices }
        : { note: "geen passend recept in de database — scrape er meer met /api/ingest" }),
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

/**
 * Zelfde kandidaten als `rerollSlot`, maar in plaats van er meteen de beste uit
 * te kiezen krijg je er een handvol terug om uit te kiezen: zonder `similarTo`
 * is dit "vul dit moment", mét is het "ander recept" — beide vullen straks een
 * leeg of een bestaand moment pas als de gebruiker er een aantikt.
 */
export async function rerollSlotOptions(
  store: Store,
  client: AhClient,
  options: RerollOptions & { count?: number },
): Promise<PlanOption[]> {
  const all = await planCandidates(store, client, options.targets, {
    diet: options.diet,
    excludedTerms: options.excludedTerms,
    excludeRecipeIds: options.excludeRecipeIds,
    kcalMode: options.kcalMode,
    limit: options.candidates ?? 30,
    slotTags: options.slotTags,
    similarTo: options.similarTo,
    pinNearFit: true,
  });
  return pickOptions(all, options.count ?? 4);
}
