import { describe, expect, it } from "vitest";
import { resolveRecipe, type ProductMatch } from "../src/nutrition/resolve";
import type { Nutrients, Product, Recipe } from "../src/ah/types";

/**
 * `resolveRecipe` kan sinds deze cyclus per ingredient een gematcht product
 * meekrijgen. Is er een product met per-100g-waarden, dan tellen die gemeten
 * waarden en blijft AH's recepttotaal het anker: wat de producten samen tekort
 * of te veel doen, wordt per macro-key rechtgetrokken.
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

const product = (title: string, per100g: Nutrients): Product => ({
  webshopId: "P-1",
  title,
  salesUnitSize: null,
  per100g,
});

const match = (product: Product, score = 1): ProductMatch => ({ product, score });

describe("resolveRecipe met producten", () => {
  it("rekent per regel de gemeten voedingswaarden uit het product", () => {
    const kwarkProduct = product("AH Magere kwark", { kcal: 57, protein: 10 });
    const banaanProduct = product("AH Banaan", { kcal: 89, protein: 1 });
    const products = new Map([
      ["kwark", match(kwarkProduct, 0.9)],
      ["banaan", match(banaanProduct, 0.8)],
    ]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("kwark", 200), gram("banaan", 100)],
        // 57*2 + 89 = 203 kcal, 10*2 + 1 = 21 g eiwit: het totaal klopt precies.
        nutritionPerServing: { kcal: 203, protein: 21 },
      }),
      products,
    );

    const [kwark, banaan] = resolved.ingredients;
    expect(kwark!.nutrients).toEqual({ kcal: 114, protein: 20 });
    expect(banaan!.nutrients).toEqual({ kcal: 89, protein: 1 });
    expect(kwark!.nutrientSource).toBe("product");
    expect(banaan!.nutrientSource).toBe("product");
    expect(kwark!.product).toBe(kwarkProduct);
    expect(kwark!.matchScore).toBe(0.9);
    expect(banaan!.matchScore).toBe(0.8);

    const sumKcal = resolved.ingredients.reduce((s, i) => s + (i.nutrients.kcal ?? 0), 0);
    const sumProtein = resolved.ingredients.reduce((s, i) => s + (i.nutrients.protein ?? 0), 0);
    expect(sumKcal).toBe(203);
    expect(sumProtein).toBe(21);
  });

  it("de kern: gemeten waarden winnen van de gewichtsverdeling", () => {
    const products = new Map([
      ["olijfolie", match(product("AH Olijfolie", { kcal: 884 }))],
      ["courgette", match(product("AH Courgette", { kcal: 17 }))],
    ]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("olijfolie", 100), gram("courgette", 100)],
        // Samen schatten de producten 901 kcal; AH zegt 450. De oude
        // gewichtsverdeling zou 225/225 geven — hier moet de verhouding van
        // de producten (52:1) overeind blijven.
        nutritionPerServing: { kcal: 450 },
      }),
      products,
    );

    const [olijfolie, courgette] = resolved.ingredients;
    expect(olijfolie!.nutrients.kcal!).toBeCloseTo(441.5, 1);
    expect(courgette!.nutrients.kcal!).toBeCloseTo(8.5, 1);
    expect(olijfolie!.nutrients.kcal! / courgette!.nutrients.kcal!).toBeGreaterThan(40);
    expect(resolved.source).toBe("ah");
  });

  it("verdeelt het gat dat de producten openlaten over de geschatte regels", () => {
    const products = new Map([
      ["kip", match(product("AH Kipfilet", { kcal: 100 }))],
      ["rijst", match(product("AH Rijst", { kcal: 100 }))],
    ]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("kip", 100), gram("rijst", 100), gram("peultjes", 100)],
        nutritionPerServing: { kcal: 300 },
      }),
      products,
    );

    const [kip, rijst, peultjes] = resolved.ingredients;
    expect(kip!.nutrients.kcal).toBeCloseTo(100, 5);
    expect(rijst!.nutrients.kcal).toBeCloseTo(100, 5);
    expect(peultjes!.nutrientSource).toBe("geschat");
    // De enige geschatte regel krijgt het hele gat van 100.
    expect(peultjes!.nutrients.kcal).toBeCloseTo(100, 5);
  });

  it("schaalt gemeten waarden naar beneden als de producten hoger schatten dan AH", () => {
    const products = new Map([
      ["olijfolie", match(product("AH Olijfolie", { kcal: 884 }))],
      ["courgette", match(product("AH Courgette", { kcal: 17 }))],
    ]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("olijfolie", 100), gram("courgette", 100)],
        nutritionPerServing: { kcal: 450 },
      }),
      products,
    );

    const [olijfolie, courgette] = resolved.ingredients;
    expect(olijfolie!.nutrients.kcal).toBeCloseTo(884 * (450 / 901), 5);
    expect(courgette!.nutrients.kcal).toBeCloseTo(17 * (450 / 901), 5);
    expect(olijfolie!.nutrients.kcal! + courgette!.nutrients.kcal!).toBe(450);
    expect(courgette!.nutrientSource).toBe("product");
  });

  it("regressie: zonder producten blijft de oude gewichtsverdeling", () => {
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("olijfolie", 100), gram("courgette", 100)],
        nutritionPerServing: { kcal: 450 },
      }),
    );

    const [olijfolie, courgette] = resolved.ingredients;
    expect(olijfolie!.nutrientSource).toBe("geschat");
    expect(olijfolie!.product).toBeNull();
    expect(olijfolie!.nutrients.kcal).toBeCloseTo(225, 5);
    expect(courgette!.nutrients.kcal).toBeCloseTo(225, 5);
  });

  it("een product zonder voedingswaarden telt als geschat", () => {
    const products = new Map([["kwark", match(product("AH Kwark zonder label", {}))]]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("kwark", 200), gram("banaan", 100)],
        nutritionPerServing: { kcal: 300 },
      }),
      products,
    );

    const [kwark, banaan] = resolved.ingredients;
    expect(kwark!.nutrientSource).toBe("geschat");
    expect(kwark!.product).toBeNull();
    expect(kwark!.matchScore).toBe(0);
    expect(kwark!.nutrients.kcal).toBeCloseTo(200, 5);
    expect(banaan!.nutrients.kcal).toBeCloseTo(100, 5);
  });

  it("water en zout blijven nul, ook als er een product in de map staat", () => {
    const products = new Map([
      ["kwark", match(product("AH Magere kwark", { kcal: 57 }))],
      ["water", match(product("AH Water", { kcal: 999 }))],
    ]);
    const resolved = resolveRecipe(
      recipe({
        ingredients: [gram("kwark", 200), gram("water", 100)],
        nutritionPerServing: { kcal: 114 },
      }),
      products,
    );

    const [kwark, water] = resolved.ingredients;
    expect(kwark!.nutrients.kcal).toBeCloseTo(114, 5);
    expect(water!.nutrientSource).toBe("nul");
    expect(water!.nutrients).toEqual({});
    expect(water!.product).toBeNull();
    expect(water!.matchScore).toBe(0);
    const sumKcal = resolved.ingredients.reduce((s, i) => s + (i.nutrients.kcal ?? 0), 0);
    expect(sumKcal).toBeCloseTo(114, 5);
  });
});
