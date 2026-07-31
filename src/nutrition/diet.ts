import type { Recipe } from "../ah/types";

/**
 * Recepten van AH dragen geen bruikbare labels, dus leiden we ze zelf af uit de
 * titel en de ingredientenlijst. Twee soorten labels, met verschillend gebruik:
 *
 *   * inhoudslabels (vlees, vis, ...) — waar de dieetfilters op werken;
 *   * momentlabels (ontbijt, lunch, ...) — waar de eetmomenten op zoeken.
 *
 * Het is heuristiek, geen waarheid. Daarom filtert alles wat een dieet uitsluit
 * hard weg (liever een recept te weinig dan vlees in een vegetarische dag), maar
 * werken de momentlabels alleen als voorkeur bij het rangschikken.
 */

/** Inhoudslabels: waar dieetfilters op aangrijpen. */
const CONTENT_RULES: [string, RegExp][] = [
  ["varken", /\b(varken|spek|bacon|ham\b|prosciutto|chorizo|salami|schnitzel|worst)/i],
  ["vlees", /\b(kip|kalkoen|rund|biefstuk|gehakt|lam\b|varken|spek|bacon|ham\b|worst|salami|chorizo|eend|shoarma|steak|rosbief|schnitzel|kalfs)/i],
  ["vis", /\b(zalm|tonijn|kabeljauw|garnaal|garnalen|vis\b|haring|makreel|forel|ansjovis|mossel|inktvis|schol|tilapia|krab|sardine)/i],
  ["zuivel", /\b(melk|kaas|kwark|yoghurt|room|boter|mozzarella|parmezaan|feta|mascarpone|creme fraiche|skyr)/i],
  ["ei", /\b(ei\b|eieren|eigeel|eiwit\b|omelet)/i],
  ["noten", /\b(noot|noten|amandel|walnoot|hazelnoot|cashew|pinda|pistache|pecan)/i],
  ["gluten", /\b(brood|pasta|spaghetti|penne|tarwe|bloem|couscous|bulgur|panko|paneermeel|cracker|wrap|tortilla|noedel|macaroni)/i],
];

/** Momentlabels: zoekhints voor de eetmomenten. */
const MOMENT_RULES: [string, RegExp][] = [
  ["ontbijt", /\b(ontbijt|havermout|overnight oats|kwark|yoghurt|granola|muesli|pannenkoek|smoothie|wentelteefjes|croissant|beschuit)/i],
  ["lunch", /\b(lunch|brood|sandwich|wrap|tosti|salade|soep|broodje|bagel)/i],
  ["snack", /\b(snack|tussendoor|reep|bar\b|hapje|borrel|koek|muffin|energieball|dip\b)/i],
  ["diner", /\b(diner|avondeten|ovenschotel|curry|stoof|pasta|rijst|aardappel|risotto|wok\b|lasagne|stamppot|traybake)/i],
];

/**
 * Dieetvoorkeuren en de inhoudslabels die ze verbieden. De sleutels zijn wat er
 * in `profile.diet` staat.
 */
export const DIETS: Record<string, string[]> = {
  vegetarisch: ["vlees", "vis", "varken"],
  veganistisch: ["vlees", "vis", "varken", "zuivel", "ei"],
  geen_vis: ["vis"],
  geen_varken: ["varken"],
  geen_noten: ["noten"],
  glutenvrij: ["gluten"],
  lactosevrij: ["zuivel"],
};

/** Alles waar we labels uit lezen: titel plus elke ingredientnaam. */
function searchableText(recipe: Recipe): string {
  return [recipe.title, ...recipe.ingredients.map((i) => i.name)].join(" ").toLowerCase();
}

/** Leidt de labels van een recept af. Draait bij elke opslag, dus houd het goedkoop. */
export function deriveTags(recipe: Recipe): string[] {
  const text = searchableText(recipe);
  const tags = new Set<string>();
  for (const [tag, re] of CONTENT_RULES) if (re.test(text)) tags.add(tag);
  for (const [tag, re] of MOMENT_RULES) if (re.test(text)) tags.add(tag);
  return [...tags];
}

/** De inhoudslabels die deze dieetkeuzes samen verbieden. */
export function forbiddenTags(diet: string[]): string[] {
  const out = new Set<string>();
  for (const choice of diet) for (const tag of DIETS[choice] ?? []) out.add(tag);
  return [...out];
}

/** False zodra het recept iets bevat dat het dieet uitsluit. */
export function matchesDiet(tags: string[], diet: string[]): boolean {
  const forbidden = forbiddenTags(diet);
  return !tags.some((tag) => forbidden.includes(tag));
}
