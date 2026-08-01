import { describe, expect, it } from "vitest";
import { collectRecipes, parseKeywords, parseNutritionLd } from "../src/ah/client";
import { extractEmbeddedJson } from "../src/ah/scrape";
import { deriveTags } from "../src/nutrition/diet";

/**
 * Deze vorm is overgenomen uit een echte, gearchiveerde ah.nl-receptpagina — het
 * scrape-archief van de live worker. AH is inmiddels over op de Next.js App
 * Router, dus `__NEXT_DATA__` bestaat niet meer en is `ld+json` de enige
 * betrouwbare bron. Verandert dat weer, dan valt precies deze test om.
 */
const REAL_LD_JSON = {
  "@context": "https://schema.org",
  "@type": "Recipe",
  isPartOf: { "@id": "https://www.ah.nl/allerhande#website" },
  author: { "@type": "Organization", "@id": "https://www.ah.nl/allerhande#organization", name: "Albert Heijn" },
  name: "Sandwich met kip en avocado - 12+ maanden (Opperdepop)",
  url: "https://www.ah.nl/allerhande/recept/R-R1202646/sandwich-met-kip-en-avocado-12-maanden-opperdepop",
  totalTime: "PT15M",
  image: ["", "https://static.ah.nl/static/recepten/img_RAM_PRD222797_1224x900_JPG.jpg"],
  description: "Met deze smaakvolle sandwich met kip en avocado leert je kleintje verschillende smaken proeven.",
  keywords: "gezond, vooraf te maken, brood/sandwiches, tussendoortje, grillen, Opperdepop",
  recipeYield: "8",
  nutrition: {
    "@type": "NutritionInformation",
    calories: "135 kcal energie",
    fatContent: "8 g vet",
    saturatedFatContent: "2 g waarvan verzadigd",
    carbohydrateContent: "7 g koolhydraten",
    proteinContent: "8 g eiwit",
  },
  recipeIngredient: [
    "150 g scharrelkipfilets",
    "1 el milde olijfolie",
    "2 snoepgroente komkommers",
    "1 eetrijpe avocado",
    "50 g zuivelspread naturel",
    "4 sneetjes waldkorn volkorenbrood",
  ],
};

const pageWith = (data: unknown) =>
  `<!DOCTYPE html><html><head><script type="application/ld+json">${JSON.stringify(data)}</script></head><body></body></html>`;

describe("parsing a real Allerhande recipe page", () => {
  const parsed = collectRecipes(extractEmbeddedJson(pageWith(REAL_LD_JSON)));

  it("finds the recipe even though the page has no id field", () => {
    // Het schema.org-blok draagt geen "id" of "@id"; het recept-id zit alleen in
    // de url. Zonder die terugval vindt de parser helemaal niets.
    expect(parsed).toHaveLength(1);
    expect(parsed[0]!.id).toBe("R-R1202646");
  });

  it("reads the ingredients from the free-text list", () => {
    const ingredients = parsed[0]!.ingredients;
    expect(ingredients).toHaveLength(6);
    expect(ingredients[0]).toEqual({ name: "scharrelkipfilets", quantity: 150, unit: "g" });
    expect(ingredients[1]).toEqual({ name: "milde olijfolie", quantity: 1, unit: "el" });
  });

  it("takes the serving count from recipeYield", () => {
    expect(parsed[0]!.servings).toBe(8);
  });

  it("keeps AH's own labels", () => {
    const keywords = parsed[0]!.keywords ?? [];
    expect(keywords).toContain("tussendoortje");
    // "brood/sandwiches" is één keyword met twee varianten erin.
    expect(keywords).toContain("brood");
    expect(keywords).toContain("sandwiches");
  });

  it("takes the nutrition AH already worked out", () => {
    expect(parsed[0]!.nutritionPerServing).toEqual({
      kcal: 135,
      fat: 8,
      carbs: 7,
      protein: 8,
    });
  });

  it("labels the eating moment from AH rather than from the title", () => {
    // De titel zegt "Sandwich", onze eigen heuristiek zou daar "lunch" van maken.
    // AH zegt "tussendoortje", en AH wint.
    const tags = deriveTags(parsed[0]!);
    expect(tags).toContain("snack");
    expect(tags).not.toContain("lunch");
  });
});

/**
 * Een echte receptpagina bevat het recept twee keer: de paginastate met de rijke
 * ingredienten, en de ld+json met AH's eigen voedingswaarde. Ze dragen niet
 * hetzelfde id — `1202636` tegenover `R-R1202636` — en de een weet wat de ander
 * mist. Gaat dat mis, dan houdt het recept geen voedingswaarde over en sneuvelt
 * het op het eerste ingredient zonder productlabel (courgette, limoen).
 */
describe("de paginastate en de ld+json van dezelfde pagina", () => {
  const FLIGHT_RECIPE = {
    id: 1202636,
    title: "Courgette-frittata met roomkaas en citroen",
    href: "https://www.ah.nl/allerhande/recept/R-R1202636/courgette-frittata",
    serving: { number: 4 },
    ingredients: [
      { name: { singular: "courgette", plural: "courgettes" }, quantity: 2, quantityUnit: { singular: "stuk" } },
      { name: { singular: "milde olijfolie" }, quantity: 4, quantityUnit: { singular: "el" } },
    ],
  };

  const LD_JSON = {
    "@context": "https://schema.org",
    "@type": "Recipe",
    name: "Courgette-frittata met roomkaas en citroen",
    url: "https://www.ah.nl/allerhande/recept/R-R1202636/courgette-frittata",
    recipeYield: "4",
    keywords: "vegetarisch, lunch",
    nutrition: {
      "@type": "NutritionInformation",
      calories: "320 kcal energie",
      fatContent: "27 g vet",
      carbohydrateContent: "5 g koolhydraten",
      proteinContent: "14 g eiwit",
    },
    recipeIngredient: ["2 courgettes", "4 el milde olijfolie"],
  };

  // De App Router levert de state als flight-stream; de chunk is een
  // JSON-string met daarin een regel "<id>:<payload>".
  const flightChunk = JSON.stringify("\n3:" + JSON.stringify(FLIGHT_RECIPE) + "\n");
  const page =
    `<!DOCTYPE html><html><head><script type="application/ld+json">${JSON.stringify(LD_JSON)}</script>` +
    `</head><body><script>self.__next_f.push([1,${flightChunk}])</script></body></html>`;

  const parsed = collectRecipes(extractEmbeddedJson(page));

  it("houdt er één recept aan over, niet twee", () => {
    expect(parsed).toHaveLength(1);
    expect(parsed[0]!.id).toBe("R-R1202636");
  });

  it("neemt de rijke ingredienten van de paginastate", () => {
    // Met de eenheid apart; de ld+json heeft alleen "4 el milde olijfolie".
    expect(parsed[0]!.ingredients[1]).toMatchObject({ name: "milde olijfolie", quantity: 4, unit: "el" });
  });

  it("neemt de voedingswaarde uit de ld+json, die de paginastate niet heeft", () => {
    expect(parsed[0]!.nutritionPerServing).toEqual({ kcal: 320, fat: 27, carbs: 5, protein: 14 });
  });
});

describe("parseKeywords", () => {
  it("splits on commas and slashes and lowercases", () => {
    expect(parseKeywords("Gezond, brood/sandwiches, Ontbijt")).toEqual([
      "gezond",
      "brood",
      "sandwiches",
      "ontbijt",
    ]);
  });

  it("accepts an array as well as a string", () => {
    expect(parseKeywords(["Ontbijt", "snel"])).toEqual(["ontbijt", "snel"]);
  });

  it("returns nothing for a missing or odd value", () => {
    expect(parseKeywords(undefined)).toEqual([]);
    expect(parseKeywords(42)).toEqual([]);
    expect(parseKeywords("")).toEqual([]);
  });
});

describe("parseNutritionLd", () => {
  it("pulls the numbers out of AH's labelled strings", () => {
    expect(parseNutritionLd(REAL_LD_JSON.nutrition)).toEqual({
      kcal: 135,
      fat: 8,
      carbs: 7,
      protein: 8,
    });
  });

  it("refuses a block without calories, so we fall back to the products", () => {
    // Zonder kcal kan de planner er niets mee; dan is zelf uitrekenen beter.
    expect(parseNutritionLd({ proteinContent: "8 g eiwit" })).toBeNull();
    expect(parseNutritionLd(null)).toBeNull();
    expect(parseNutritionLd("135 kcal")).toBeNull();
  });

  it("handles a decimal comma, which AH uses", () => {
    expect(parseNutritionLd({ calories: "135 kcal", fatContent: "8,5 g vet" })?.fat).toBe(8.5);
  });
});

describe("moment labels from AH keywords", () => {
  const withKeywords = (keywords: string) =>
    deriveTags(collectRecipes(extractEmbeddedJson(pageWith({ ...REAL_LD_JSON, keywords })))[0]!);

  it("maps AH's course names onto our eating moments", () => {
    expect(withKeywords("ontbijt")).toContain("ontbijt");
    expect(withKeywords("brunch")).toContain("ontbijt");
    expect(withKeywords("hoofdgerecht")).toContain("diner");
    expect(withKeywords("borrelhapje")).toContain("snack");
  });

  it("falls back to the title heuristic when AH says nothing about the moment", () => {
    // Geen menugang-woord in de keywords, dus onze eigen gok mag het overnemen.
    const tags = withKeywords("gezond, snel");
    expect(tags).toContain("lunch"); // uit "Sandwich" in de titel
  });
});
