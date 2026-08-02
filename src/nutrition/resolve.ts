import {
  NUTRIENT_KEYS,
  type NutrientKey,
  type Nutrients,
  type Product,
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
 * De productkoppeling is ooit bewust verwijderd: het matchen op naam raadde te
 * vaak mis, veel verse producten hebben geen voedingswaardetabel en het kostte
 * per recept tientallen verzoeken aan ah.nl. De koppeling die overblijft is
 * daarom niet een zoekactie hier, maar eerder gevonden matches die van buiten
 * worden meegegeven: `resolveRecipe` accepteert een map van genormaliseerde
 * ingredientnamen naar producten en gebruikt die gemeten per-100g-waarden als
 * verdeelsleutel per regel. AH's recepttotaal blijft daarbij het anker.
 *
 * Deze module doet zelf geen enkele aanroep: hij is een pure berekening.
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
 * Een eerder gevonden koppeling tussen een ingredientnaam en een AH-product.
 * De map die `resolveRecipe` meekrijgt, is gesleuteld op de genormaliseerde
 * ingredientnaam (lowercase) — zo slaat `putMatch` hem in de database op.
 */
export interface ProductMatch {
  product: Product;
  /** 0..1 confidence van de oorspronkelijke ingredient→product-match. */
  score: number;
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
 *
 * Sinds de producten terug zijn, gaat de verdeling per macro-key anders te werk
 * wanneer een regel een gematcht product met voedingswaarden heeft: die regel
 * telt zijn gemeten waarden (`scaleNutrients`) in plaats van een aandeel. Het
 * recepttotaal blijft het anker — een key zonder totaal houdt alleen de
 * gemeten waarden, en als de producten samen hoger schatten dan AH, wordt hun
 * bijdrage voor die key teruggeschaald.
 */
export function resolveRecipe(recipe: Recipe, products?: Map<string, ProductMatch>): ResolvedRecipe {
  const total = recipeTotal(recipe);

  const ingredients: ResolvedIngredient[] = recipe.ingredients.map((raw) => {
    const { grams, source } = toGrams(raw);
    const match = products?.get(raw.name.toLowerCase());

    if (isNutritionFree(raw.name)) {
      return {
        raw,
        grams,
        product: null,
        gramsSource: source,
        matchScore: 0,
        nutrients: {},
        nutrientSource: "nul",
      };
    }

    // Een product zonder per-100g-waarden telt niet mee als bron; zo'n regel
    // valt dan gewoon terug op de schatting uit het recepttotaal.
    if (
      match !== undefined &&
      NUTRIENT_KEYS.some((key) => (match.product.per100g[key] ?? 0) > 0)
    ) {
      return {
        raw,
        grams,
        product: match.product,
        gramsSource: source,
        matchScore: match.score,
        nutrients: scaleNutrients(match.product.per100g, grams),
        nutrientSource: "product",
      };
    }

    return {
      raw,
      grams,
      product: null,
      gramsSource: source,
      matchScore: 0,
      nutrients: {},
      nutrientSource: total ? "geschat" : "onbekend",
    };
  });

  if (total) {
    const measured = ingredients.filter((i) => i.nutrientSource === "product");
    const estimated = ingredients.filter((i) => i.nutrientSource === "geschat");

    for (const key of NUTRIENT_KEYS) {
      const target = total[key];
      if (target === undefined) continue;

      // Wat de producten voor deze key samen zeggen; regels zonder waarde
      // dragen nul bij.
      let measuredSum = 0;
      for (const ingredient of measured) {
        const value = ingredient.nutrients[key];
        if (value !== undefined) measuredSum += value;
      }

      if (measuredSum === 0) {
        // Niets gemeten: de vertrouwde gewichtsverdeling over de geschatte regels.
        distributeByWeight(estimated, key, target);
      } else if (target < measuredSum) {
        // De producten schatten hoger dan AH; schaal ze allemaal terug en geef
        // de geschatte regels niets, zodat het totaal blijft kloppen.
        const factor = target / measuredSum;
        for (const ingredient of measured) {
          const value = ingredient.nutrients[key];
          if (value !== undefined) ingredient.nutrients[key] = value * factor;
        }
        for (const ingredient of estimated) ingredient.nutrients[key] = 0;
      } else {
        // Gemeten waarden blijven staan; het gat eronder gaat naar gewicht
        // naar de geschatte regels.
        distributeByWeight(estimated, key, target - measuredSum);
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
 * Verdeelt `amount` van één macro-key naar gewicht over de geschatte regels.
 * Regels zonder gewicht krijgen gelijke delen, zodat delen door nul er nooit
 * uit kan komen.
 */
function distributeByWeight(estimated: ResolvedIngredient[], key: NutrientKey, amount: number): void {
  const totalGrams = estimated.reduce((sum, i) => sum + i.grams, 0);
  for (const ingredient of estimated) {
    const share = totalGrams > 0 ? ingredient.grams / totalGrams : 1 / estimated.length;
    ingredient.nutrients[key] = amount * share;
  }
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
