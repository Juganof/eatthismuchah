import { describe, expect, it } from "vitest";
import {
  collectRecipes,
  parseIngredientText,
  parseNutrition,
  parseNutritionHtml,
  parseRecipeCards,
  productIdFrom,
} from "../src/ah/client";
import { deepFind, extractEmbeddedJson, extractFlightJson } from "../src/ah/scrape";

describe("extractEmbeddedJson", () => {
  it("pulls the Next.js state out of a page", () => {
    const html = `<html><body><script id="__NEXT_DATA__" type="application/json">{"a":1}</script></body></html>`;
    expect(extractEmbeddedJson(html)).toEqual([{ a: 1 }]);
  });

  it("collects every ld+json block, not just the first", () => {
    const html = `<script type="application/ld+json">{"a":1}</script>
                  <script type="application/ld+json">{"b":2}</script>`;
    expect(extractEmbeddedJson(html)).toEqual([{ a: 1 }, { b: 2 }]);
  });

  it("skips unparseable blocks instead of throwing", () => {
    const html = `<script id="__NEXT_DATA__">not json</script>`;
    expect(extractEmbeddedJson(html)).toEqual([]);
  });
});

describe("deepFind", () => {
  it("finds a nested value", () => {
    expect(deepFind({ a: { b: [{ c: 42 }] } }, (v) => v === 42)).toBe(42);
  });

  it("returns null when nothing matches", () => {
    expect(deepFind({ a: 1 }, (v) => v === 99)).toBeNull();
  });

  it("terminates on a cyclic object", () => {
    const cyclic: Record<string, unknown> = { a: 1 };
    cyclic.self = cyclic;
    expect(deepFind(cyclic, (v) => v === 1)).toBe(1);
    expect(deepFind(cyclic, (v) => v === 99)).toBeNull();
  });
});

describe("parseIngredientText", () => {
  it("splits quantity, unit and name", () => {
    expect(parseIngredientText("250 g kipfilet")).toEqual({
      quantity: 250,
      unit: "g",
      name: "kipfilet",
    });
  });

  it("handles a comma decimal", () => {
    expect(parseIngredientText("1,5 l water").quantity).toBe(1.5);
  });

  it("handles a line with no quantity", () => {
    expect(parseIngredientText("peper en zout")).toEqual({
      quantity: null,
      unit: null,
      name: "peper en zout",
    });
  });

  it("doesn't mistake a leading adjective for a unit", () => {
    // "rijpe" is not a unit, so it stays part of the name and there is no unit —
    // this used to get parsed as unit "rijpe", which then failed every unit lookup
    // and silently fell back to a guessed gram weight.
    expect(parseIngredientText("3 rijpe bananen")).toEqual({
      quantity: 3,
      unit: null,
      name: "rijpe bananen",
    });
  });

  it("still recognises a real unit followed by a descriptive name", () => {
    expect(parseIngredientText("2 el gehakte peterselie")).toEqual({
      quantity: 2,
      unit: "el",
      name: "gehakte peterselie",
    });
  });
});

describe("parseNutrition", () => {
  it("reads AH's label/value rows", () => {
    const payload = {
      nutritionalInformation: [
        { name: "Energie", value: "1050 kJ" },
        { name: "Energie (kcal)", value: "250 kcal" },
        { name: "Eiwitten", value: "31,0 g" },
        { name: "Koolhydraten", value: "0 g" },
        { name: "Vetten", value: "3,6 g" },
        { name: "waarvan verzadigde vetzuren", value: "1,1 g" },
        { name: "Vezels", value: "2,0 g" },
      ],
    };
    expect(parseNutrition(payload)).toEqual({
      kcal: 250,
      protein: 31,
      carbs: 0,
      fat: 3.6,
      fiber: 2,
    });
  });

  it("prefers kcal over the kJ row listed before it", () => {
    const payload = { rows: [{ name: "Energie kJ", value: 1050 }, { name: "Energie kcal", value: 250 }] };
    expect(parseNutrition(payload).kcal).toBe(250);
  });

  it("does not mistake saturated fat for total fat", () => {
    const payload = { rows: [{ name: "waarvan verzadigd", value: 1.1 }, { name: "Vetten", value: 3.6 }] };
    expect(parseNutrition(payload).fat).toBe(3.6);
  });

  it("returns an empty object for a payload with no nutrition", () => {
    expect(parseNutrition({ title: "AH Kipfilet" })).toEqual({});
  });

  it("takes kcal rather than kJ from a combined energy value", () => {
    expect(parseNutrition({ rows: [{ name: "Energie", value: "538 kJ (128 kcal)" }] }).kcal).toBe(
      128,
    );
  });
});

describe("parseNutritionHtml", () => {
  it("reads AH's current product-page table", () => {
    const html = `<table data-testid="nutrition-table"><tbody>
      <tr><td><span>Energie</span></td><td><span>538 kJ (128 kcal)</span></td></tr>
      <tr><td><span>Vet</span></td><td><span>4,5 g</span></td></tr>
      <tr><td><span>Koolhydraten</span></td><td><span>2,7 g</span></td></tr>
      <tr><td><span>Eiwitten</span></td><td><span>19 g</span></td></tr>
    </tbody></table>`;
    expect(parseNutritionHtml(html)).toMatchObject({
      kcal: 128,
      fat: 4.5,
      carbs: 2.7,
      protein: 19,
    });
  });
});

describe("toProductStub", () => {
  it("ignores virtual multi-packs without a single nutrition table", async () => {
    const { toProductStub } = await import("../src/ah/client");
    expect(
      toProductStub({
        webshopId: 123,
        title: "Kipfilet 4-pack",
        isVirtualBundle: true,
        bundleItems: [{}],
      }),
    ).toBeNull();
  });
});

describe("collectRecipes", () => {
  it("finds recipes wherever they sit in the payload", () => {
    const payload = {
      props: {
        pageProps: {
          data: {
            recipe: {
              id: 1193067,
              title: "Kip met rijst",
              servings: 4,
              ingredients: [
                { name: "kipfilet", quantity: 500, unit: { name: "g" } },
                { name: "rijst", quantity: 300, unit: "g" },
              ],
            },
          },
        },
      },
    };
    const [recipe] = collectRecipes(payload);
    expect(recipe?.id).toBe("R-R1193067");
    expect(recipe?.servings).toBe(4);
    expect(recipe?.ingredients).toEqual([
      { name: "kipfilet", quantity: 500, unit: "g", productId: null },
      { name: "rijst", quantity: 300, unit: "g", productId: null },
    ]);
  });

  it("keeps an id that is already prefixed", () => {
    const [recipe] = collectRecipes({ id: "R-R42", title: "X", servings: 2, ingredients: [] });
    expect(recipe?.id).toBe("R-R42");
  });

  it("de-duplicates the same recipe appearing twice", () => {
    const one = { id: "R-R42", title: "X", servings: 2, ingredients: [] };
    expect(collectRecipes({ a: one, b: { ...one } })).toHaveLength(1);
  });

  it("defaults servings when AH omits them", () => {
    const [recipe] = collectRecipes({ id: "9", title: "X", ingredients: [] });
    expect(recipe?.servings).toBe(4);
  });

  it("ignores objects that are not recipes", () => {
    expect(collectRecipes({ id: "1", title: "just a heading" })).toEqual([]);
  });

  it("normalises a schema.org Recipe from AH's detail page", () => {
    const [recipe] = collectRecipes({
      "@type": "Recipe",
      name: "Kip 'saltimbocca'",
      url: "https://www.ah.nl/allerhande/recept/R-R1202672/kip-saltimbocca",
      recipeYield: "4",
      recipeIngredient: ["500 g kipfilet", "2 el olijfolie"],
    });
    expect(recipe).toMatchObject({
      id: "R-R1202672",
      title: "Kip 'saltimbocca'",
      servings: 4,
      ingredients: [
        { name: "kipfilet", quantity: 500, unit: "g" },
        { name: "olijfolie", quantity: 2, unit: "el" },
      ],
    });
  });
});

describe("parseRecipeCards", () => {
  it("reads current server-rendered AH recipe cards", () => {
    const html = `<a title="Recept: Kip &#x27;saltimbocca&#x27;"
      data-testid="recipe-card"
      href="/allerhande/recept/R-R1202672/kip-saltimbocca"></a>`;
    expect(parseRecipeCards(html)).toEqual([
      {
        id: "R-R1202672",
        title: "Kip 'saltimbocca'",
        url: "https://www.ah.nl/allerhande/recept/R-R1202672/kip-saltimbocca",
        servings: 4,
        imageUrl: null,
        ingredients: [],
      },
    ]);
  });
});

describe("productkoppeling per ingredient", () => {
  it("leest de flight-chunks van AH's App Router als paginastate", () => {
    const payload = JSON.stringify({ recipe: { id: 5, title: "X", ingredients: [] } });
    const row = JSON.stringify(`3:${payload}\n`);
    const html = `<script>self.__next_f.push([1,${row}])</script>`;
    expect(extractFlightJson(html)).toEqual([{ recipe: { id: 5, title: "X", ingredients: [] } }]);
  });

  it("plakt een payload die over twee chunks verdeeld is weer aan elkaar", () => {
    const html =
      `<script>self.__next_f.push([1,${JSON.stringify('4:{"a":')}])</script>` +
      `<script>self.__next_f.push([1,${JSON.stringify("1}\n")}])</script>`;
    expect(extractFlightJson(html)).toEqual([{ a: 1 }]);
  });

  it("neemt het webshop-id over dat AH zelf aan de receptregel hangt", () => {
    const [recipe] = collectRecipes({
      id: 42,
      title: "Kwarkontbijt",
      servings: 1,
      ingredients: [
        { name: "kwark", quantity: 150, unit: "g", productId: 445566 },
        { name: "banaan", quantity: 1, unit: null, product: { webshopId: "778899" } },
        { name: "peper", quantity: null, unit: null },
      ],
    });
    expect(recipe?.ingredients.map((i) => i.productId)).toEqual(["445566", "778899", null]);
  });

  it("negeert een id dat geen webshop-id kan zijn", () => {
    expect(productIdFrom({ productId: "abc" })).toBeNull();
    expect(productIdFrom({ productId: 0 })).toBeNull();
    expect(productIdFrom({ products: [{ id: 12 }] })).toBe("12");
  });
});

describe("de paginastate van AH's receptpagina", () => {
  /** Zoals AH het in de flight-chunks zet; overgenomen uit een echte scrape. */
  const flightRecipe = {
    id: 1202571,
    title: "Vegaballetjes met yoghurtdip",
    href: "/allerhande/recept/R-R1202571/vegaballetjes",
    servings: 4,
    tags: [
      { __typename: "RecipeTag", key: "speciale-wensen", value: "vegetarisch" },
      { __typename: "RecipeTag", key: "menugang", value: "borrelhapje" },
    ],
    classifications: ["vegetarisch", "zonder vlees/vis"],
    ingredients: [
      {
        __typename: "RecipeIngredient",
        id: 3200,
        name: { __typename: "SingularPluralName", singular: "milde olijfolie", plural: null },
        quantity: 2,
        quantityUnit: { __typename: "SingularPluralName", singular: "el", plural: "el" },
        text: "2 el milde olijfolie",
      },
      {
        __typename: "RecipeIngredient",
        id: 3575,
        name: { __typename: "SingularPluralName", singular: "pijnboompitten", plural: null },
        quantity: 15,
        quantityUnit: { __typename: "SingularPluralName", singular: "g", plural: "g" },
        text: "15 g pijnboompitten",
      },
    ],
  };

  it("leest naam en eenheid uit de {singular, plural}-velden", () => {
    const [recipe] = collectRecipes(flightRecipe);
    expect(recipe?.ingredients).toEqual([
      { name: "milde olijfolie", quantity: 2, unit: "el", productId: null },
      { name: "pijnboompitten", quantity: 15, unit: "g", productId: null },
    ]);
  });

  it("valt terug op de kant-en-klare regel als de losse velden ontbreken", () => {
    const [recipe] = collectRecipes({
      ...flightRecipe,
      ingredients: [{ name: { singular: "milde olijfolie" }, text: "2 el milde olijfolie" }],
    });
    expect(recipe?.ingredients[0]).toEqual({
      name: "milde olijfolie",
      quantity: 2,
      unit: "el",
      productId: null,
    });
  });

  it("neemt AH's eigen labels over uit tags en classifications", () => {
    const [recipe] = collectRecipes(flightRecipe);
    // Zonder dit zou het flight-object (dat eerst komt) de ld+json-keywords
    // verdringen en bleef het recept zonder AH-labels achter.
    expect(recipe?.keywords).toEqual(
      expect.arrayContaining(["vegetarisch", "borrelhapje", "zonder vlees", "vis"]),
    );
  });
});
