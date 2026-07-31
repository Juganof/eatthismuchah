import { describe, expect, it } from "vitest";
import { buildTargets, planRecipe, rankPlans, targetCost } from "../src/optimize/plan";
import type { ResolvedRecipe } from "../src/ah/types";

/** A 4-serving recipe: 500 g chicken, 300 g rice, 20 g oil, plus salt. */
const recipe: ResolvedRecipe = {
  recipe: {
    id: "R-R1",
    title: "Kip met rijst",
    url: "https://www.ah.nl/allerhande/recept/R-R1",
    servings: 4,
    imageUrl: null,
    ingredients: [],
  },
  ingredients: [
    {
      raw: { name: "kipfilet", quantity: 500, unit: "g" },
      grams: 500,
      product: { webshopId: "1", title: "AH Kipfilet", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 825, protein: 155, carbs: 0, fat: 18 },
      gramsSource: "explicit",
      matchScore: 0.9,
    },
    {
      raw: { name: "rijst", quantity: 300, unit: "g" },
      grams: 300,
      product: { webshopId: "2", title: "AH Rijst", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 1050, protein: 21, carbs: 234, fat: 1.8 },
      gramsSource: "explicit",
      matchScore: 0.9,
    },
    {
      raw: { name: "olijfolie", quantity: 20, unit: "g" },
      grams: 20,
      product: { webshopId: "3", title: "AH Olijfolie", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 177, protein: 0, carbs: 0, fat: 20 },
      gramsSource: "explicit",
      matchScore: 0.9,
    },
    {
      raw: { name: "peper en zout", quantity: null, unit: null },
      grams: 5,
      product: null,
      nutrients: {},
      gramsSource: "fallback",
      matchScore: 0,
    },
  ],
  total: { kcal: 2052, protein: 176, carbs: 234, fat: 39.8 },
};

// `per100g` is empty above because planRecipe reads the pre-scaled `nutrients`;
// the ingredients are still flagged matched via a non-null product.
const matched = {
  ...recipe,
  ingredients: recipe.ingredients.map((i) =>
    i.product ? { ...i, product: { ...i.product, per100g: { kcal: 1 } } } : i,
  ),
};

describe("planRecipe", () => {
  it("hits per-portion targets for a single portion", () => {
    // Within 5%: the default shape penalty deliberately trades a little accuracy
    // for staying recognisably close to the original recipe.
    const plan = planRecipe(matched, buildTargets({ protein: 60, kcal: 700 }), { portions: 1 });
    expect(plan.perPortion.protein!).toBeGreaterThan(57);
    expect(plan.perPortion.protein!).toBeLessThan(63);
    expect(plan.perPortion.kcal!).toBeGreaterThan(665);
    expect(plan.perPortion.kcal!).toBeLessThan(735);
  });

  it("scales the shopping amounts with the portion count", () => {
    const one = planRecipe(matched, buildTargets({ protein: 60, kcal: 700 }), { portions: 1 });
    const two = planRecipe(matched, buildTargets({ protein: 60, kcal: 700 }), { portions: 2 });
    // Per portion is unchanged by cooking more of it.
    expect(two.perPortion.protein).toBeCloseTo(one.perPortion.protein!, 0);
    // Same per portion, twice the food.
    expect(two.totals.protein).toBeCloseTo(one.totals.protein! * 2, 0);
    const chickenOne = one.ingredients.find((i) => i.name === "kipfilet")!;
    const chickenTwo = two.ingredients.find((i) => i.name === "kipfilet")!;
    expect(chickenTwo.grams).toBeCloseTo(chickenOne.grams * 2, 0);
  });

  it("reports grams for the original recipe divided by its own servings", () => {
    // 500 g chicken across 4 servings is 125 g for one portion.
    const plan = planRecipe(matched, [], { portions: 1 });
    expect(plan.ingredients.find((i) => i.name === "kipfilet")!.originalGrams).toBeCloseTo(125);
  });

  it("keeps locked ingredients at their original amount", () => {
    const plan = planRecipe(matched, buildTargets({ protein: 90 }), {
      portions: 1,
      locked: ["rijst"],
    });
    expect(plan.ingredients.find((i) => i.name === "rijst")!.scale).toBe(1);
    expect(plan.ingredients.find((i) => i.name === "kipfilet")!.scale).toBeGreaterThan(1);
  });

  it("holds seasonings near their original amount", () => {
    const plan = planRecipe(matched, buildTargets({ protein: 200 }), { portions: 1 });
    const salt = plan.ingredients.find((i) => i.name === "peper en zout")!;
    expect(salt.scale).toBeLessThanOrEqual(1.5);
    expect(salt.scale).toBeGreaterThanOrEqual(0.75);
  });

  it("flags ingredients with no nutrition data", () => {
    const plan = planRecipe(matched, [], { portions: 1 });
    expect(plan.ingredients.find((i) => i.name === "peper en zout")!.unmatched).toBe(true);
    expect(plan.ingredients.find((i) => i.name === "kipfilet")!.unmatched).toBe(false);
  });

  it("reports coverage by weight", () => {
    // Only the 5 g of seasoning is unmatched, out of 825 g.
    const plan = planRecipe(matched, [], { portions: 1 });
    expect(plan.coverage).toBeCloseTo(820 / 825, 2);
  });

  it("treats kcal as a ceiling in max mode", () => {
    // The original is 513 kcal per portion. A small overshoot is expected: at the
    // ceiling the shape penalty pushes back against cutting further.
    const plan = planRecipe(matched, buildTargets({ kcal: 500, kcalMode: "max" }), { portions: 1 });
    expect(plan.perPortion.kcal!).toBeLessThanOrEqual(525);
    expect(plan.perPortion.kcal!).toBeLessThan(planRecipe(matched, [], { portions: 1 }).perPortion.kcal!);
  });

  it("leaves the recipe alone when there are no targets", () => {
    const plan = planRecipe(matched, [], { portions: 1 });
    for (const i of plan.ingredients) expect(i.scale).toBe(1);
  });

  it("carries the recipe's own quantity and unit through for display", () => {
    const plan = planRecipe(matched, [], { portions: 1 });
    const chicken = plan.ingredients.find((i) => i.name === "kipfilet")!;
    expect(chicken.gramsSource).toBe("explicit");
    expect(chicken.unit).toBe("g");
    expect(chicken.originalQuantity).toBeCloseTo(125); // 500 g / 4 servings
  });

  it("keeps a non-gram quantity in its own unit, not converted to grams", () => {
    const bananas: ResolvedRecipe = {
      ...recipe,
      ingredients: [
        {
          raw: { name: "bananen", quantity: 3, unit: null },
          grams: 360,
          product: { webshopId: "9", title: "AH Bananen", salesUnitSize: null, per100g: { kcal: 1 } },
          nutrients: { kcal: 320, protein: 4, carbs: 80, fat: 0.4 },
          gramsSource: "piece",
          matchScore: 0.9,
        },
      ],
    };
    const plan = planRecipe(bananas, [], { portions: 2 });
    const bananen = plan.ingredients.find((i) => i.name === "bananen")!;
    // The fixture recipe is for 4 servings; 3 bananen becomes 1.5 for 2 portions.
    expect(bananen.originalQuantity).toBeCloseTo(1.5);
    expect(bananen.unit).toBeNull();
    expect(bananen.gramsSource).toBe("piece");
  });
});

describe("targetCost", () => {
  it("is zero for a perfect match", () => {
    const targets = buildTargets({ protein: 60, kcal: 700 });
    expect(targetCost({ protein: 60, kcal: 700 }, targets)).toBe(0);
  });

  it("grows with how far off the values are", () => {
    const targets = buildTargets({ protein: 60, kcal: 700 });
    const close = targetCost({ protein: 55, kcal: 680 }, targets);
    const far = targetCost({ protein: 20, kcal: 300 }, targets);
    expect(close).toBeGreaterThan(0);
    expect(far).toBeGreaterThan(close);
  });
});

describe("buildTargets", () => {
  it("omits macros that were left blank", () => {
    expect(buildTargets({ protein: 60 })).toEqual([
      { key: "protein", value: 60, mode: "target", weight: 2 },
    ]);
  });

  it("treats fiber as a floor, not an exact target", () => {
    expect(buildTargets({ fiber: 10 })[0]!.mode).toBe("min");
  });
});

describe("rankPlans", () => {
  it("prefers a slightly worse fit with trustworthy data over a perfect fit without", () => {
    const base = planRecipe(matched, buildTargets({ protein: 60 }), { portions: 1 });
    const good = { ...base, recipeId: "good", cost: 0.5, coverage: 1 };
    const guessy = { ...base, recipeId: "guessy", cost: 0.1, coverage: 0.2 };
    expect(rankPlans([guessy, good])[0]!.recipeId).toBe("good");
  });
});
