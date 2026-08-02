import { describe, expect, it } from "vitest";
import type { Plan, PlannedIngredient } from "../src/optimize/plan";
import { buildShoppingList, packagesFor, parsePackSize, piecesFor } from "../src/plan/shopping";

const ingredient = (over: Partial<PlannedIngredient> & { name: string }): PlannedIngredient => ({
  grams: 100,
  originalGrams: 100,
  scale: 1,
  nutrients: {},
  productTitle: "AH " + over.name,
  productId: null,
  productSize: null,
  matchScore: 1,
  unmatched: false,
  nutrientSource: "product",
  originalQuantity: null,
  unit: null,
  gramsSource: "explicit",
  ...over,
});

const plan = (ingredients: PlannedIngredient[]): Plan => ({
  recipeId: "R-R1",
  title: "Test",
  url: "https://www.ah.nl/allerhande/recept/R-R1",
  imageUrl: null,
  portions: 1,
  ingredients,
  perPortion: {},
  totals: {},
  cost: 0,
  coverage: 1,
});

describe("buildShoppingList", () => {
  it("adds up the same ingredient across meals", () => {
    const lines = buildShoppingList([
      { label: "Ontbijt", plan: plan([ingredient({ name: "kwark", grams: 250 })]) },
      { label: "Diner", plan: plan([ingredient({ name: "kwark", grams: 150 })]) },
    ]);

    expect(lines).toHaveLength(1);
    expect(lines[0]!.grams).toBe(400);
    expect(lines[0]!.usedIn).toEqual(["Ontbijt", "Diner"]);
  });

  it("treats spelling variants of one ingredient as one line", () => {
    const lines = buildShoppingList([
      { label: "Lunch", plan: plan([ingredient({ name: "verse kipfilet", grams: 200 })]) },
      { label: "Diner", plan: plan([ingredient({ name: "Kipfilet (bio)", grams: 100 })]) },
    ]);

    expect(lines).toHaveLength(1);
    expect(lines[0]!.grams).toBe(300);
  });

  it("keeps genuinely different ingredients apart", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([
          ingredient({ name: "kipfilet", grams: 200 }),
          ingredient({ name: "rijst", grams: 90 }),
        ]),
      },
    ]);

    expect(lines.map((l) => l.name).sort()).toEqual(["kipfilet", "rijst"]);
  });

  it("sorts by weight so the real shopping comes first", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([
          ingredient({ name: "peper", grams: 2 }),
          ingredient({ name: "kipfilet", grams: 400 }),
          ingredient({ name: "rijst", grams: 90 }),
        ]),
      },
    ]);

    expect(lines.map((l) => l.name)).toEqual(["kipfilet", "rijst", "peper"]);
  });

  it("links to the AH product when the id is known, and not when it is not", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([
          ingredient({ name: "kipfilet", grams: 400 }),
          ingredient({ name: "rijst", grams: 90 }),
        ]),
        webshopIds: { kipfilet: "123456" },
      },
    ]);

    expect(lines.find((l) => l.name === "kipfilet")?.productUrl).toBe(
      "https://www.ah.nl/producten/product/wi123456",
    );
    expect(lines.find((l) => l.name === "rijst")?.productUrl).toBeNull();
  });

  it("flags a line as unmatched as soon as any use of it was unmatched", () => {
    const lines = buildShoppingList([
      { label: "Ontbijt", plan: plan([ingredient({ name: "kwark", grams: 100 })]) },
      {
        label: "Diner",
        plan: plan([ingredient({ name: "kwark", grams: 100, unmatched: true, productTitle: null })]),
      },
    ]);

    expect(lines[0]!.unmatched).toBe(true);
  });

  it("leaves out ingredients the solver scaled away to nothing", () => {
    const lines = buildShoppingList([
      { label: "Diner", plan: plan([ingredient({ name: "boter", grams: 0 })]) },
    ]);
    expect(lines).toEqual([]);
  });

  it("copes with no meals at all", () => {
    expect(buildShoppingList([])).toEqual([]);
  });
});

describe("parsePackSize", () => {
  it("leest grammen uit de gebruikelijke AH-etiketten", () => {
    expect(parsePackSize("330 g")).toEqual({ kind: "grams", grams: 330 });
    expect(parsePackSize("500 gram")).toEqual({ kind: "grams", grams: 500 });
    expect(parsePackSize("1 kg")).toEqual({ kind: "grams", grams: 1000 });
    expect(parsePackSize("100 ml")).toEqual({ kind: "grams", grams: 100 });
  });

  it("rekent liters om naar grammen (1 l is 1000 g)", () => {
    expect(parsePackSize("1 l")).toEqual({ kind: "grams", grams: 1000 });
  });

  it("leest een aantal stuks uit een stuksverpakking", () => {
    expect(parsePackSize("2 stuks")).toEqual({ kind: "pieces", pieces: 2 });
  });

  it("negeert een ca.-voorvoegsel: ca. 400 g is 400 g (vastgelegde tolerantie)", () => {
    expect(parsePackSize("ca. 400 g")).toEqual({ kind: "grams", grams: 400 });
    expect(parsePackSize("± 250 g")).toEqual({ kind: "grams", grams: 250 });
  });

  it("geeft null terug voor onbruikbare of ontbrekende etiketten", () => {
    expect(parsePackSize(null)).toBeNull();
    expect(parsePackSize("")).toBeNull();
    expect(parsePackSize("pakje verse basilicum")).toBeNull();
    expect(parsePackSize("1-2 stuks")).toBeNull();
  });
});

describe("packagesFor", () => {
  it("rondt altijd naar boven af, met minstens één verpakking", () => {
    expect(packagesFor(600, 330)).toBe(2);
    expect(packagesFor(660, 330)).toBe(2);
    expect(packagesFor(661, 330)).toBe(3);
  });
});

describe("piecesFor", () => {
  it("telt de aantallen op als alle gebruiken een aantal hadden", () => {
    expect(piecesFor([3, 2])).toBe(5);
  });

  it("geeft null terug zolang er een aantal ontbreekt", () => {
    expect(piecesFor([3, null])).toBeNull();
    expect(piecesFor([])).toBeNull();
  });
});

describe("buildShoppingList: verpakkingen", () => {
  const product = (salesUnitSize: string | null) => ({ title: "AH product", salesUnitSize });

  it("geeft het aantal verpakkingen bij een uniek product met gram-pak", () => {
    const lines = buildShoppingList([
      {
        label: "Dag 1",
        plan: plan([ingredient({ name: "kikkererwten", grams: 175 })]),
        webshopIds: { kikkererwten: "168813" },
        products: { "168813": { title: "AH Terra Biologisch kikkererwten", salesUnitSize: "330 g" } },
      },
      { label: "Dag 2", plan: plan([ingredient({ name: "kikkererwten", grams: 425 })]) },
    ]);

    expect(lines).toHaveLength(1);
    expect(lines[0]!.grams).toBe(600);
    expect(lines[0]!.packages).toBe(2);
    expect(lines[0]!.packagesLabel).toBe("2 × 330 g");
    // De producttitel komt uit de productenkaart, niet uit het plan.
    expect(lines[0]!.productTitle).toBe("AH Terra Biologisch kikkererwten");
    expect(lines[0]!.productUrl).toBe("https://www.ah.nl/producten/product/wi168813");
  });

  it("rekent het aantal verpakkingen uit de geschaalde grammen", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([ingredient({ name: "kikkererwten", grams: 700 })]),
        webshopIds: { kikkererwten: "168813" },
        products: { "168813": product("330 g") },
      },
    ]);

    expect(lines[0]!.packages).toBe(3);
    expect(lines[0]!.packagesLabel).toBe("3 × 330 g");
  });

  it("mengt gram-, liter- en stuksverpakkingen in één lijst", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([
          ingredient({ name: "kikkererwten", grams: 500 }),
          ingredient({ name: "melk", grams: 1500 }),
          ingredient({ name: "avocado", grams: 200, gramsSource: "piece", originalQuantity: 3 }),
        ]),
        webshopIds: { kikkererwten: "wi-1", melk: "wi-2", avocado: "wi-3" },
        products: {
          "wi-1": product("330 g"),
          "wi-2": product("1 l"),
          "wi-3": product("2 stuks"),
        },
      },
    ]);

    const byName = (name: string) => lines.find((l) => l.name === name)!;
    expect(byName("kikkererwten").packages).toBe(2);
    expect(byName("kikkererwten").packagesLabel).toBe("2 × 330 g");
    expect(byName("melk").packages).toBe(2);
    expect(byName("melk").packagesLabel).toBe("2 × 1 l");
    expect(byName("avocado").pieces).toBe(3);
  });

  it("telt losse verse producten in stuks op over de dagen", () => {
    const lines = buildShoppingList([
      { label: "Dag 1", plan: plan([ingredient({ name: "banaan", grams: 100, gramsSource: "piece", originalQuantity: 3 })]) },
      { label: "Dag 2", plan: plan([ingredient({ name: "banaan", grams: 80, gramsSource: "piece", originalQuantity: 2 })]) },
    ]);

    expect(lines).toHaveLength(1);
    expect(lines[0]!.pieces).toBe(5);
    expect(lines[0]!.packages).toBeNull();
  });

  it("laat grammen zien zolang er geen verpakking bekend is", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([ingredient({ name: "rijst", grams: 90 })]),
        webshopIds: { rijst: "123456" },
        products: { "123456": product(null) },
      },
    ]);

    expect(lines[0]!.productUrl).toBe("https://www.ah.nl/producten/product/wi123456");
    expect(lines[0]!.packages).toBeNull();
    expect(lines[0]!.packagesLabel).toBeNull();
    expect(lines[0]!.pieces).toBeNull();
    expect(lines[0]!.grams).toBe(90);
  });

  it("laat grammen zien als een regel uit verschillende producten is opgebouwd", () => {
    const lines = buildShoppingList([
      {
        label: "Diner",
        plan: plan([
          ingredient({ name: "verse kipfilet", grams: 200 }),
          ingredient({ name: "Kipfilet (bio)", grams: 100 }),
        ]),
        webshopIds: { "verse kipfilet": "wi-1", "Kipfilet (bio)": "wi-2" },
        products: { "wi-1": product("330 g"), "wi-2": product("400 g") },
      },
    ]);

    expect(lines).toHaveLength(1);
    expect(lines[0]!.packages).toBeNull();
    expect(lines[0]!.grams).toBe(300);
  });
});
