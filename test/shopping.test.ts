import { describe, expect, it } from "vitest";
import type { Plan, PlannedIngredient } from "../src/optimize/plan";
import { buildShoppingList } from "../src/plan/shopping";

const ingredient = (over: Partial<PlannedIngredient> & { name: string }): PlannedIngredient => ({
  grams: 100,
  originalGrams: 100,
  scale: 1,
  nutrients: {},
  productTitle: "AH " + over.name,
  matchScore: 1,
  unmatched: false,
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
