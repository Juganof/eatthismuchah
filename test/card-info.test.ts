import { describe, expect, it } from "vitest";
import { kcalPer100g } from "../src/ui/card-info";

/**
 * De pure omzettingen achter de receptkaarten leven in een eigen module zodat
 * ze rechtstreeks in Node getest kunnen worden; de clientcode embedt ze via
 * toString(), net als shoppingLinesToText.
 */
describe("kcalPer100g", () => {
  it("leidt kcal per 100 g af uit de kcal en gram van de regel", () => {
    expect(kcalPer100g(200, 118)).toBe(59);
  });

  it("rondt af op hele kcal per 100 g", () => {
    expect(kcalPer100g(150, 100)).toBe(67);
  });

  it("geeft null als de regel geen gewicht draagt", () => {
    expect(kcalPer100g(0, 118)).toBeNull();
    expect(kcalPer100g(-5, 118)).toBeNull();
  });

  it("geeft null als de kcal van de regel ontbreken", () => {
    expect(kcalPer100g(200, undefined)).toBeNull();
    expect(kcalPer100g(200, null)).toBeNull();
  });
});
