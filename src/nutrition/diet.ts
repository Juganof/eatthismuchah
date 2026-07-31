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

/**
 * Inhoudslabels: waar dieetfilters op aangrijpen.
 *
 * Let op de woordgrenzen. Het Nederlands plakt woorden aan elkaar
 * ("volkorenbrood", "afbakciabatta", "kipgehakt"), dus een `\b` vóór een term
 * laat precies die samenstellingen door — en dan krijgt iemand die glutenvrij
 * eet een broodrecept voorgeschoteld. Lange, kenmerkende termen zoeken we
 * daarom als losse deelstring; alleen bij korte woorden die toevallig in iets
 * anders kunnen zitten (ei, ham, lam, vis, room) blijft de grens staan.
 */
const CONTENT_RULES: [string, RegExp][] = [
  ["varken", /(varken|spek|\bbacon|\bham\b|prosciutto|chorizo|salami|schnitzel|worst)/i],
  ["vlees", /(\bkip|kalkoen|\brund|biefstuk|gehakt|\blam\b|varken|spek|\bbacon|\bham\b|worst|salami|chorizo|\beend\b|shoarma|\bsteak|rosbief|schnitzel|kalfs)/i],
  ["vis", /(zalm|tonijn|kabeljauw|garnaal|garnalen|\bvis\b|haring|makreel|forel|ansjovis|mossel|inktvis|\bschol\b|tilapia|\bkrab|sardine)/i],
  ["zuivel", /(melk|\bkaas|kaas\b|kwark|yoghurt|\broom|boter\b|boterm|mozzarella|parmezaan|\bfeta\b|mascarpone|creme fraiche|\bskyr\b|zuivel)/i],
  ["ei", /(\bei\b|eieren|eigeel|\beiwit\b|omelet)/i],
  ["noten", /(\bnoot|noten|amandel|walnoot|hazelnoot|cashew|pinda|pistache|pecan)/i],
  ["gluten", /(brood|pasta|spaghetti|penne|tarwe|bloem|couscous|bulgur|panko|paneermeel|cracker|\bwrap|tortilla|noedel|macaroni|ciabatta|pizza|\bdeeg|beschuit|bagel|lasagne|croissant)/i],
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

/**
 * Woorden die een inhoudslabel zouden opleveren terwijl ze iets anders betekenen.
 * "kip buiteneieren" is de AH-schrijfwijze voor scharreleieren en is dus géén
 * vlees; zonder deze stap krijgt een vegetarier dat recept ten onrechte niet.
 */
const FALSE_FRIENDS: [RegExp, string][] = [
  [/\bkip(?:pen)?\s*(?:buiten)?eieren\b/g, "eieren"],
  [/\bkippenei(?:eren)?\b/g, "eieren"],
  // Vleesvervangers dragen de naam van het vlees dat ze vervangen.
  [/\b(?:vega|vegan|vegetarische?)\s+\w+/g, " "],
];

/** Alles waar we labels uit lezen: titel plus elke ingredientnaam. */
function searchableText(recipe: Recipe): string {
  let text = [recipe.title, ...recipe.ingredients.map((i) => i.name)].join(" ").toLowerCase();
  for (const [re, replacement] of FALSE_FRIENDS) text = text.replace(re, replacement);
  return text;
}

/** Leidt de labels van een recept af. Draait bij elke opslag, dus houd het goedkoop. */
export function deriveTags(recipe: Recipe): string[] {
  const text = searchableText(recipe);
  const tags = new Set<string>();
  for (const [tag, re] of CONTENT_RULES) if (re.test(text)) tags.add(tag);
  for (const [tag, re] of MOMENT_RULES) if (re.test(text)) tags.add(tag);
  // Varkensvlees is vlees. Dat afleiden in plaats van elke varkensterm ook in de
  // vleeslijst te herhalen, want die twee lijsten lopen gegarandeerd uit elkaar
  // — "prosciutto" stond al alleen in de eerste.
  if (tags.has("varken")) tags.add("vlees");
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
