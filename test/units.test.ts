import { describe, expect, it } from "vitest";
import { densityFor, pieceWeightFor, toGrams } from "../src/nutrition/units";

describe("toGrams", () => {
  it("passes mass units straight through", () => {
    expect(toGrams({ name: "kipfilet", quantity: 250, unit: "g" })).toEqual({
      grams: 250,
      source: "explicit",
    });
    expect(toGrams({ name: "meel", quantity: 1.5, unit: "kg" }).grams).toBe(1500);
  });

  it("applies density to volume units", () => {
    // 100 ml water is 100 g; 100 ml olive oil is not.
    expect(toGrams({ name: "water", quantity: 100, unit: "ml" }).grams).toBeCloseTo(100);
    expect(toGrams({ name: "olijfolie", quantity: 100, unit: "ml" }).grams).toBeCloseTo(92);
  });

  it("converts spoons and flags them as such", () => {
    const el = toGrams({ name: "olijfolie", quantity: 2, unit: "el" });
    expect(el.grams).toBeCloseTo(2 * 15 * 0.92);
    expect(el.source).toBe("spoon");
    expect(toGrams({ name: "suiker", quantity: 1, unit: "tl" }).grams).toBeCloseTo(5 * 0.85);
  });

  it("treats a bare number as a count of pieces", () => {
    expect(toGrams({ name: "ui", quantity: 2, unit: null })).toEqual({ grams: 220, source: "piece" });
    expect(toGrams({ name: "eieren", quantity: 3, unit: "stuks" }).grams).toBe(165);
  });

  it("reads garlic cloves as cloves, not as whole bulbs", () => {
    expect(toGrams({ name: "knoflook", quantity: 2, unit: "tenen" }).grams).toBe(10);
  });

  it("gives seasonings a small non-zero weight when no quantity is given", () => {
    const r = toGrams({ name: "peper en zout", quantity: null, unit: null });
    expect(r.grams).toBeGreaterThan(0);
    expect(r.grams).toBeLessThan(10);
    expect(r.source).toBe("fallback");
  });

  it("falls back rather than throwing on an unknown unit", () => {
    expect(toGrams({ name: "iets", quantity: 2, unit: "handjevol" }).source).toBe("fallback");
  });

  it("picks the most specific density and piece weight", () => {
    // "zonnebloemolie" must not be resolved by the shorter "olie" entry alone,
    // and "trostomaat" must beat the generic "tomaat".
    expect(densityFor("zonnebloemolie")).toBeCloseTo(0.92);
    expect(pieceWeightFor("trostomaat")).toBe(90);
    expect(pieceWeightFor("tomaat")).toBe(100);
  });
});
