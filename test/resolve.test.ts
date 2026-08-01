import { describe, expect, it } from "vitest";
import { isNutritionFree, recipeTotal, resolveRecipe, tokenize } from "../src/nutrition/resolve";
import type { Nutrients, Recipe } from "../src/ah/types";

/**
 * Sinds het loslaten van de productkoppeling is dit pure rekenkunde: AH's
 * voedingswaarde per portie in, cijfers per ingredient uit. Wat hier vastligt is
 * dat het totaal precies blijft wat AH zegt, en dat de verdeling erover naar
 * gewicht gaat.
 */

const recipe = (over: Partial<Recipe> = {}): Recipe => ({
  id: "R-R1",
  title: "Testrecept",
  url: "https://www.ah.nl/allerhande/recept/R-R1",
  servings: 1,
  imageUrl: null,
  ingredients: [],
  ...over,
});

const gram = (name: string, grams: number) => ({ name, quantity: grams, unit: "g" });

describe("recipeTotal", () => {
  it("rekent AH's cijfers per portie om naar het hele recept", () => {
    const total = recipeTotal(recipe({ servings: 4, nutritionPerServing: { kcal: 320, protein: 14 } }));
    expect(total).toEqual({ kcal: 1280, protein: 56 });
  });

  it("geeft niets terug zonder calorieen, want daar valt niet mee te plannen", () => {
    expect(recipeTotal(recipe({ nutritionPerServing: { protein: 14 } }))).toBeNull();
    expect(recipeTotal(recipe({ nutritionPerServing: null }))).toBeNull();
  });
});

describe("resolveRecipe", () => {
  const courgetteFrittata = recipe({
    servings: 4,
    ingredients: [gram("courgette", 600), gram("olijfolie", 40), gram("scharrelei", 250)],
    nutritionPerServing: { kcal: 320, protein: 14, fat: 27, carbs: 5 },
  });

  it("verdeelt het recepttotaal naar gewicht over de ingredienten", () => {
    const resolved = resolveRecipe(courgetteFrittata);
    const kcal = resolved.ingredients.map((i) => i.nutrients.kcal!);
    // 890 g in totaal; de courgette is 600 daarvan, dus 600/890 van 1280 kcal.
    expect(kcal[0]).toBeCloseTo((1280 * 600) / 890, 5);
    expect(kcal[1]).toBeCloseTo((1280 * 40) / 890, 5);
    expect(kcal.reduce((a, b) => a + b, 0)).toBeCloseTo(1280, 5);
  });

  it("houdt het totaal exact op wat AH zegt", () => {
    const resolved = resolveRecipe(courgetteFrittata);
    expect(resolved.total).toEqual({ kcal: 1280, protein: 56, fat: 108, carbs: 20 });
    expect(resolved.source).toBe("ah");
  });

  it("markeert elke regel als toegerekend, niet als gemeten", () => {
    const resolved = resolveRecipe(courgetteFrittata);
    for (const ingredient of resolved.ingredients) {
      expect(ingredient.nutrientSource).toBe("geschat");
      expect(ingredient.product).toBeNull();
    }
  });

  it("geeft water en zout niets, en snoept hun aandeel dus niet van de rest af", () => {
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("havermout", 100), gram("water", 200), { name: "snuf zout", quantity: null, unit: null }],
        nutritionPerServing: { kcal: 375 },
      }),
    );
    expect(resolved.ingredients[0]!.nutrients.kcal).toBeCloseTo(375, 5);
    expect(resolved.ingredients[1]!.nutrients).toEqual({});
    expect(resolved.ingredients[1]!.nutrientSource).toBe("nul");
    expect(resolved.ingredients[2]!.nutrients).toEqual({});
  });

  it("laat de macro's weg die AH zelf niet noemt", () => {
    // AH geeft zelden vezels op. Dan nul invullen zou een recept vezelvrij
    // laten lijken; niets invullen laat de solver het gewoon niet meewegen.
    const resolved = resolveRecipe(
      recipe({ ingredients: [gram("havermout", 100)], nutritionPerServing: { kcal: 375, protein: 13 } }),
    );
    expect(resolved.ingredients[0]!.nutrients.fiber).toBeUndefined();
    expect(resolved.total.fiber).toBeUndefined();
  });

  it("laat de regels leeg als AH geen voedingswaarde geeft", () => {
    const resolved = resolveRecipe(recipe({ ingredients: [gram("havermout", 100)] }));
    expect(resolved.total).toEqual({} as Nutrients);
    expect(resolved.ingredients[0]!.nutrientSource).toBe("onbekend");
  });

  it("valt terug op gelijk verdelen als geen enkele regel gewicht heeft", () => {
    const resolved = resolveRecipe(
      recipe({
        // Zonder hoeveelheid schat `toGrams` een gewicht, dus dit is een
        // randgeval dat in de praktijk nauwelijks voorkomt; delen door nul mag
        // er hoe dan ook niet uit komen.
        ingredients: [{ name: "iets", quantity: 0, unit: "g" }, { name: "iets anders", quantity: 0, unit: "g" }],
        nutritionPerServing: { kcal: 100 },
      }),
    );
    expect(resolved.ingredients[0]!.nutrients.kcal).toBeCloseTo(50, 5);
    expect(resolved.ingredients[1]!.nutrients.kcal).toBeCloseTo(50, 5);
  });
});

describe("isNutritionFree", () => {
  it("herkent ingredienten die geen voedingswaarde hébben", () => {
    expect(isNutritionFree("water")).toBe(true);
    expect(isNutritionFree("200 ml kraanwater")).toBe(true);
    expect(isNutritionFree("een snuf zout")).toBe(true);
    expect(isNutritionFree("versgemalen zwarte peper")).toBe(true);
  });

  it("laat zich niet vangen door een woord dat er alleen op lijkt", () => {
    // Anders zou "peperoni" als nul tellen en het recept stilletjes te laag uitkomen.
    expect(isNutritionFree("peperoni")).toBe(false);
    expect(isNutritionFree("kokoswater")).toBe(false);
    expect(isNutritionFree("water met citroen")).toBe(false);
    expect(isNutritionFree("zoute pinda's")).toBe(false);
  });
});

describe("tokenize", () => {
  it("laat bereidingswoorden en tussenzinnen vallen", () => {
    expect(tokenize("2 rijpe trostomaten, in blokjes (geschild)")).toEqual(["trostomaten"]);
  });
});
