import { describe, expect, it } from "vitest";
import { AhClient } from "../src/ah/client";

// Hits ah.nl for real, so it stays off by default: a plain `npm test` must pass
// offline. Run it deliberately with LIVE_AH=1 npm test when the parsers look stale.
describe.skipIf(!process.env.LIVE_AH)("live AH integration", () => {
  const client = new AhClient("Appie/8.22.3 Model/phone Android/13-M");

  it(
    "authenticates and searches products and recipes",
    async () => {
      const probe = await client.probe();

      expect(probe.auth).toMatch(/^ok/);
      expect(probe.productSearch).toMatch(/^ok/);
      expect(probe.recipeSearch).toMatch(/^ok/);
      expect(probe.productSearch).not.toContain("(0)");
      expect(probe.recipeSearch).not.toContain("(0)");

      const recipes = await client.searchRecipes("kip", 3);
      const recipe = await client.getRecipe(recipes[0]!.id);
      expect(recipe?.ingredients.length).toBeGreaterThan(0);

      const products = await client.searchProducts("kipfilet", 3);
      const product = await client.getProduct(products[0]!.webshopId);
      expect(product?.per100g.kcal).toBeTypeOf("number");
      expect(product?.per100g.protein).toBeTypeOf("number");
    },
    45_000,
  );
});
