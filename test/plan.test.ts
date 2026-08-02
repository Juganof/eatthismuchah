import { describe, expect, it } from "vitest";
import { buildTargets, isScalable, planRecipe, rankPlans, sameDensity, targetCost } from "../src/optimize/plan";
import type { ResolvedRecipe } from "../src/ah/types";
import type { MacroTarget } from "../src/optimize/solver";

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
      nutrientSource: "product",
    },
    {
      raw: { name: "rijst", quantity: 300, unit: "g" },
      grams: 300,
      product: { webshopId: "2", title: "AH Rijst", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 1050, protein: 21, carbs: 234, fat: 1.8 },
      gramsSource: "explicit",
      matchScore: 0.9,
      nutrientSource: "product",
    },
    {
      raw: { name: "olijfolie", quantity: 20, unit: "g" },
      grams: 20,
      product: { webshopId: "3", title: "AH Olijfolie", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 177, protein: 0, carbs: 0, fat: 20 },
      gramsSource: "explicit",
      matchScore: 0.9,
      nutrientSource: "product",
    },
    {
      raw: { name: "peper en zout", quantity: null, unit: null },
      grams: 5,
      product: null,
      nutrients: {},
      gramsSource: "fallback",
      matchScore: 0,
      // Geen product en geen cijfers: dit is de regel waarop "unmatched" en de
      // dekkingsgraad slaan.
      nutrientSource: "onbekend",
    },
  ],
  total: { kcal: 2052, protein: 176, carbs: 234, fat: 39.8 },
  source: "products",
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
          nutrientSource: "product",
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

// --- Cyclus C5: per-ingrediënt schalen op basis van echte productdichtheden ---

/** Kwark (57 kcal/100 g) plus banaan (89 kcal/100 g), beide met productlabel. */
const kwarkBanaan: ResolvedRecipe = {
  recipe: {
    id: "R-R10",
    title: "Kwark met banaan",
    url: "https://www.ah.nl/allerhande/recept/R-R10",
    servings: 1,
    imageUrl: null,
    ingredients: [],
  },
  ingredients: [
    {
      raw: { name: "kwark", quantity: 250, unit: "g" },
      grams: 250,
      product: { webshopId: "10", title: "AH Magere kwark", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 142.5, protein: 25, carbs: 10, fat: 0.5 },
      gramsSource: "explicit",
      matchScore: 0.9,
      nutrientSource: "product",
    },
    {
      raw: { name: "banaan", quantity: 100, unit: "g" },
      grams: 100,
      product: { webshopId: "11", title: "AH Bananen", salesUnitSize: null, per100g: {} },
      nutrients: { kcal: 89, protein: 1.1, carbs: 23, fat: 0.3 },
      gramsSource: "explicit",
      matchScore: 0.9,
      nutrientSource: "product",
    },
  ],
  total: { kcal: 231.5, protein: 26.1, carbs: 33, fat: 0.8 },
  source: "products",
};

describe("per-ingrediënt schalen (C5)", () => {
  it("schaalt kwark omhoog en fruit omlaag bij een eiwitdoel met kcal-plafond", () => {
    // Puur eiwit zou fruit op ~1 laten staan; het kcal-plafond dwingt fruit
    // omlaag omdat banaan veel kcal draagt en bijna geen eiwit.
    const plan = planRecipe(kwarkBanaan, buildTargets({ protein: 45, kcal: 230, kcalMode: "max" }), {
      portions: 1,
    });
    const kwark = plan.ingredients.find((i) => i.name === "kwark")!;
    const banaan = plan.ingredients.find((i) => i.name === "banaan")!;
    expect(kwark.scale).toBeGreaterThan(1.05);
    expect(banaan.scale).toBeLessThan(0.95);
    // De nieuwe per-regel ondergrens: geen enkel ingredient mag verder dan de
    // helft omlaag, ook fruit niet.
    expect(banaan.scale).toBeGreaterThanOrEqual(0.5);
  });

  it("houdt alle scales binnen de per-regel bounds bij een onhaalbaar doel", () => {
    // Maximaal haalbaar is 52 g eiwit; een vraag van 500 g duwt alles tegen
    // de nieuwe bovengrens van 2.0 aan (niet de oude 3.0).
    const plan = planRecipe(kwarkBanaan, buildTargets({ protein: 500 }), { portions: 1 });
    for (const ing of plan.ingredients) {
      expect(ing.scale).toBeGreaterThanOrEqual(0.5 - 1e-9);
      expect(ing.scale).toBeLessThanOrEqual(2 + 1e-9);
    }
  });

  it("laat 'nul'- en 'onbekend'-regels en locked regels op scale 1", () => {
    const metLegeRegels: ResolvedRecipe = {
      ...kwarkBanaan,
      ingredients: [
        ...kwarkBanaan.ingredients,
        {
          raw: { name: "water", quantity: 100, unit: "ml" },
          grams: 100,
          product: null,
          nutrients: {},
          gramsSource: "explicit",
          matchScore: 0,
          nutrientSource: "nul",
        },
        {
          // Heeft wél cijfers, maar geen bron: de solver mag er niet op rekenen.
          raw: { name: "sojasaus", quantity: 10, unit: "ml" },
          grams: 12,
          product: null,
          nutrients: { kcal: 10, protein: 1.5 },
          gramsSource: "volume",
          matchScore: 0,
          nutrientSource: "onbekend",
        },
      ],
    };
    const plan = planRecipe(metLegeRegels, buildTargets({ protein: 60 }), { portions: 1, locked: ["kwark"] });
    const byName = Object.fromEntries(plan.ingredients.map((i) => [i.name, i.scale]));
    expect(byName["water"]).toBe(1);
    expect(byName["sojasaus"]).toBe(1);
    expect(byName["kwark"]).toBe(1);
    expect(byName["banaan"]).toBeGreaterThan(1);
  });

  it("schaalt exact uniform zodra alle schaalbare regels dezelfde dichtheid hebben", () => {
    // Drie productregels met dezelfde voedingswaarde per gram. Ook de kruidenregel
    // doet dan mee: per-regel schalen heeft geen zin als elke regel in verhouding
    // identiek is — één factor brengt het gerecht dan het dichtst bij het doel.
    const zelfdeDichtheid: ResolvedRecipe = {
      recipe: {
        id: "R-R11",
        title: "Drie dezelfde",
        url: "https://www.ah.nl/allerhande/recept/R-R11",
        servings: 1,
        imageUrl: null,
        ingredients: [],
      },
      ingredients: [
        {
          raw: { name: "kwark", quantity: 100, unit: "g" },
          grams: 100,
          product: { webshopId: "20", title: "Product A", salesUnitSize: null, per100g: {} },
          nutrients: { kcal: 200, protein: 20, carbs: 0, fat: 0 },
          gramsSource: "explicit",
          matchScore: 0.9,
          nutrientSource: "product",
        },
        {
          raw: { name: "banaan", quantity: 200, unit: "g" },
          grams: 200,
          product: { webshopId: "21", title: "Product B", salesUnitSize: null, per100g: {} },
          nutrients: { kcal: 400, protein: 40, carbs: 0, fat: 0 },
          gramsSource: "explicit",
          matchScore: 0.9,
          nutrientSource: "product",
        },
        {
          raw: { name: "kaneel", quantity: 150, unit: "g" },
          grams: 150,
          product: { webshopId: "22", title: "Product C", salesUnitSize: null, per100g: {} },
          nutrients: { kcal: 300, protein: 30, carbs: 0, fat: 0 },
          gramsSource: "explicit",
          matchScore: 0.9,
          nutrientSource: "product",
        },
      ],
      total: { kcal: 900, protein: 90 },
      source: "products",
    };
    const plan = planRecipe(zelfdeDichtheid, buildTargets({ protein: 150 }), { portions: 1 });
    const scales = plan.ingredients.map((i) => i.scale);
    expect(new Set(scales).size).toBe(1);
    expect(scales[0]!).toBeGreaterThan(1);
  });

  it("is deterministisch voor per-regel schalen", () => {
    const targets = buildTargets({ protein: 45, kcal: 230, kcalMode: "max" });
    const a = planRecipe(kwarkBanaan, targets, { portions: 1 });
    const b = planRecipe(kwarkBanaan, targets, { portions: 1 });
    expect(a.ingredients.map((i) => i.scale)).toEqual(b.ingredients.map((i) => i.scale));
  });

  it("houdt kruiden binnen [0.75, 1.5], ook onder maximale druk", () => {
    const metKruiden: ResolvedRecipe = {
      ...kwarkBanaan,
      ingredients: [
        ...kwarkBanaan.ingredients,
        {
          raw: { name: "gemalen kaneel", quantity: 5, unit: "g" },
          grams: 5,
          product: { webshopId: "30", title: "AH Kaneel", salesUnitSize: null, per100g: {} },
          nutrients: { kcal: 20, protein: 0.3 },
          gramsSource: "explicit",
          matchScore: 0.9,
          nutrientSource: "product",
        },
      ],
    };
    const omhoog: MacroTarget[] = [{ key: "protein", value: 500, mode: "target" }];
    const omlaag: MacroTarget[] = [{ key: "kcal", value: 0, mode: "target" }];
    const kaneelUp = planRecipe(metKruiden, omhoog, { portions: 1 }).ingredients.find(
      (i) => i.name === "gemalen kaneel",
    )!;
    const kaneelDown = planRecipe(metKruiden, omlaag, { portions: 1 }).ingredients.find(
      (i) => i.name === "gemalen kaneel",
    )!;
    expect(kaneelUp.scale).toBeLessThanOrEqual(1.5);
    expect(kaneelUp.scale).toBeGreaterThanOrEqual(0.75);
    expect(kaneelDown.scale).toBeLessThanOrEqual(1.5);
    expect(kaneelDown.scale).toBeGreaterThanOrEqual(0.75);
  });

  it("houdt 'naar smaak'-regels binnen [0.5, 1.5]", () => {
    const metNaarSmaak: ResolvedRecipe = {
      ...kwarkBanaan,
      ingredients: [
        ...kwarkBanaan.ingredients,
        {
          raw: { name: "olijfolie naar smaak", quantity: null, unit: null },
          grams: 5,
          product: { webshopId: "31", title: "AH Olijfolie", salesUnitSize: null, per100g: {} },
          nutrients: { kcal: 45, protein: 3, fat: 5 },
          gramsSource: "fallback",
          matchScore: 0.9,
          nutrientSource: "product",
        },
      ],
    };
    const omhoog: MacroTarget[] = [{ key: "protein", value: 500, mode: "target" }];
    const omlaag: MacroTarget[] = [{ key: "kcal", value: 0, mode: "target" }];
    const olieUp = planRecipe(metNaarSmaak, omhoog, { portions: 1 }).ingredients.find(
      (i) => i.name === "olijfolie naar smaak",
    )!;
    const olieDown = planRecipe(metNaarSmaak, omlaag, { portions: 1 }).ingredients.find(
      (i) => i.name === "olijfolie naar smaak",
    )!;
    expect(olieUp.scale).toBeLessThanOrEqual(1.5);
    expect(olieUp.scale).toBeGreaterThanOrEqual(0.5);
    expect(olieDown.scale).toBeLessThanOrEqual(1.5);
    expect(olieDown.scale).toBeGreaterThanOrEqual(0.5);
  });

  it("laat een onhaalbaar doel eerlijk tekortschieten zonder te crashen", () => {
    const plan = planRecipe(kwarkBanaan, buildTargets({ protein: 200 }), { portions: 1 });
    expect(plan.perPortion.protein!).toBeLessThan(200);
    const kwark = plan.ingredients.find((i) => i.name === "kwark")!;
    const banaan = plan.ingredients.find((i) => i.name === "banaan")!;
    // Het eiwitrijkste ingredient staat op zijn nieuwe bovengrens (was 3.0).
    expect(kwark.scale).toBe(2);
    expect(banaan.scale).toBeLessThanOrEqual(2);
    expect(banaan.scale).toBeGreaterThanOrEqual(0.5);
  });
});

describe("sameDensity en isScalable (C5)", () => {
  it("herkent 'product' en 'geschat' als schaalbaar, 'nul' en 'onbekend' niet", () => {
    expect(isScalable(kwarkBanaan.ingredients[0]!)).toBe(true);
    expect(
      isScalable({
        ...kwarkBanaan.ingredients[0]!,
        nutrientSource: "geschat",
      }),
    ).toBe(true);
    expect(
      isScalable({
        ...kwarkBanaan.ingredients[0]!,
        nutrientSource: "nul",
      }),
    ).toBe(false);
    expect(
      isScalable({
        ...kwarkBanaan.ingredients[0]!,
        nutrientSource: "onbekend",
      }),
    ).toBe(false);
  });

  it("vindt identieke en verschillende dichtheidsvectoren", () => {
    const a = { ...kwarkBanaan.ingredients[0]! };
    // Twee keer zoveel gram, twee keer zoveel van elke macro: zelfde dichtheid.
    const zelfde = {
      ...a,
      grams: 500,
      nutrients: { kcal: 285, protein: 50, carbs: 20, fat: 1 },
    };
    const anders = { ...a, nutrients: { ...a.nutrients, kcal: 500 } };
    expect(sameDensity([a, zelfde])).toBe(true);
    expect(sameDensity([a, anders])).toBe(false);
    // Regels zonder bron tellen niet mee in de vergelijking.
    const onbekend = { ...a, nutrientSource: "onbekend" as const, nutrients: {} };
    expect(sameDensity([onbekend])).toBe(true);
    expect(sameDensity([a, zelfde, onbekend])).toBe(true);
  });
});
