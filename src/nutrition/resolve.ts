import {
  NUTRIENT_KEYS,
  type Nutrients,
  type Recipe,
  type ResolvedIngredient,
  type ResolvedRecipe,
} from "../ah/types";
import { toGrams } from "./units";

/**
 * Zet een gescrapet recept om in cijfers waar de planner mee kan rekenen.
 *
 * De bron is AH's eigen voedingswaarde per portie, die op elke receptpagina
 * staat. Dat is hun berekening over het hele gerecht en dus het meest
 * betrouwbare wat er te krijgen is.
 *
 * Er wordt niet meer per ingredient een AH-product bij gezocht. Dat is er
 * bewust uit: het matchen op naam raadde te vaak mis ("middelgroot scharrelei"
 * tegen "AH Scharreleieren", "snoepkomkommer" tegen niets), veel verse producten
 * hebben bij AH helemaal geen voedingswaardetabel, en één misser keurde een
 * verder prima recept voorgoed af. Bovendien kostte het per recept vijftien tot
 * dertig verzoeken aan ah.nl, terwijl de receptpagina zelf er maar één kost — en
 * met dat verschil passen er per ronde tientallen recepten in plaats van een
 * handjevol.
 *
 * Deze module doet daarom geen enkele aanroep meer: hij is een pure berekening.
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

/** De voedingswaarde van het hele recept, uit AH's opgave per portie. */
export function recipeTotal(recipe: Recipe): Nutrients | null {
  const perServing = recipe.nutritionPerServing;
  if (!perServing || perServing.kcal === undefined) return null;
  const servings = recipe.servings > 0 ? recipe.servings : 1;
  const out: Nutrients = {};
  for (const key of NUTRIENT_KEYS) {
    const value = perServing[key];
    if (value !== undefined) out[key] = value * servings;
  }
  return out;
}

/**
 * Rekent het recepttotaal naar gewicht toe aan de losse ingredienten.
 *
 * De solver werkt per ingredient, dus die heeft per regel een getal nodig — het
 * recepttotaal alleen is niet genoeg. Naar gewicht verdelen is daarvoor de
 * eerlijkste verdeelsleutel die zonder productgegevens te maken is: 400 g kip
 * krijgt vier keer zoveel als 100 g, en een handvol peterselie bijna niets.
 *
 * Het is nadrukkelijk een verdeling en geen meting. Wat er per regel staat is
 * een aandeel; wat optelt tot AH's cijfer is het geheel. Daarom mag het plan een
 * recept ook alleen als geheel schalen — zie `planRecipe`.
 *
 * Water, zout en peper krijgen niets: die dragen echt nul bij, en meetellen zou
 * hun aandeel van de rest afsnoepen.
 */
export function resolveRecipe(recipe: Recipe): ResolvedRecipe {
  const total = recipeTotal(recipe);

  const ingredients: ResolvedIngredient[] = recipe.ingredients.map((raw) => {
    const { grams, source } = toGrams(raw);
    return {
      raw,
      grams,
      product: null,
      gramsSource: source,
      matchScore: 0,
      nutrients: {},
      nutrientSource: isNutritionFree(raw.name) ? "nul" : total ? "geschat" : "onbekend",
    };
  });

  if (total) {
    const shares = ingredients.filter((i) => i.nutrientSource === "geschat");
    const totalGrams = shares.reduce((sum, i) => sum + i.grams, 0);
    for (const ingredient of shares) {
      const share = totalGrams > 0 ? ingredient.grams / totalGrams : 1 / shares.length;
      for (const key of NUTRIENT_KEYS) {
        const value = total[key];
        if (value !== undefined) ingredient.nutrients[key] = value * share;
      }
    }
  }

  return {
    recipe,
    ingredients,
    // Het totaal blijft AH's opgave, ook als er geen enkele regel iets kreeg
    // (een recept van alleen water bestaat niet, maar afronding hoort nooit een
    // recept stiekem lichter te maken dan AH zegt).
    total: total ?? {},
    source: total ? "ah" : "products",
  };
}

/**
 * Woorden die niets over het ingredient zelf zeggen: bereidingswijze,
 * hoedanigheid, vulwoorden. Ze staan de vergelijking met de vrijstellingslijst
 * hieronder in de weg — "een snuf zout" is gewoon zout.
 */
const NOISE = new Set([
  "verse", "vers", "rijpe", "rijp", "grote", "groot", "kleine", "klein", "fijne", "fijn",
  "gesneden", "gesnipperde", "gesnipperd", "geraspte", "geraspt", "gehakte", "gehakt",
  "gepelde", "gepeld", "geschilde", "geschild", "blokjes", "reepjes", "plakjes", "ringen",
  "in", "van", "de", "het", "een", "of", "en", "met", "naar", "smaak", "extra",
  "biologische", "bio", "ongezouten", "gezouten", "magere", "volle", "halfvolle",
  "ah", "huismerk", "voor", "erbij", "eventueel", "optioneel", "stuks", "stuk",
]);

export function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ") // drop parenthetical asides
    .split(/[^a-zàâäçéèêëîïôöûüù]+/i)
    .map((t) => t.trim())
    .filter((t) => t.length > 2 && !NOISE.has(t));
}

/**
 * Ingredienten waarvoor geen voedingswaarde hoeft te bestaan.
 *
 * Water, zout en peper staan in bijna elk recept en leveren niets. Ze uit de
 * verdeling houden scheelt niet alleen nauwkeurigheid: het voorkomt ook dat een
 * plan "minder water" voorstelt om calorieen te besparen.
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
 * Of dit ingredient geen voedingswaarde hoort te hebben. Vergelijkt op hele
 * woorden, zodat "peperoni" geen peper is en "water" in "kokoswater" niet
 * meetelt.
 */
export function isNutritionFree(name: string): boolean {
  const words = tokenize(name).filter((word) => !AMOUNT_WORDS.has(word));
  if (words.length === 0) return false;
  // Elk overgebleven woord moet vrijgesteld zijn: "water met citroen" is dat niet.
  return words.every((word) => NUTRITION_FREE.has(word));
}
