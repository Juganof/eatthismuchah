import { describe, expect, it } from "vitest";
import { bestMatch, scoreMatch, searchTermFor, tokenize } from "../src/nutrition/match";
import type { Product } from "../src/ah/types";

const product = (title: string): Product => ({
  webshopId: title,
  title,
  salesUnitSize: null,
  per100g: { kcal: 100 },
});

describe("tokenize", () => {
  it("drops preparation words and parentheticals", () => {
    expect(tokenize("2 rijpe trostomaten, in blokjes (geschild)")).toEqual(["trostomaten"]);
  });
});

describe("scoreMatch", () => {
  it("scores an exact product higher than a loose one", () => {
    expect(scoreMatch("kipfilet", "AH Kipfilet")).toBeGreaterThan(
      scoreMatch("kipfilet", "AH Kipfilet kerriesalade"),
    );
  });

  it("matches across Dutch plurals", () => {
    expect(scoreMatch("trostomaten", "AH Trostomaat")).toBeGreaterThan(0.5);
  });

  it("penalises ready-made products for a raw ingredient", () => {
    expect(scoreMatch("tomaat", "AH Tomatensoep")).toBeLessThan(scoreMatch("tomaat", "AH Tomaten"));
  });

  it("does not penalise when the ingredient itself asks for it", () => {
    expect(scoreMatch("sojasaus", "AH Sojasaus")).toBeGreaterThan(0.7);
  });
});

describe("bestMatch", () => {
  it("picks the closest candidate", () => {
    const match = bestMatch("kipfilet", [
      product("AH Kipfilet kerriesalade"),
      product("AH Kipfilet"),
      product("AH Kipdijfilet"),
    ]);
    expect(match?.product.title).toBe("AH Kipfilet");
  });

  it("returns null when nothing is close enough", () => {
    expect(bestMatch("kipfilet", [product("AH Wasmiddel")])).toBeNull();
  });

  it("returns null for an empty candidate list", () => {
    expect(bestMatch("kipfilet", [])).toBeNull();
  });
});

describe("searchTermFor", () => {
  it("strips the noise a webshop search would choke on", () => {
    expect(searchTermFor("2 rijpe trostomaten, in blokjes")).toBe("trostomaten");
  });

  it("falls back to the original text when everything is noise", () => {
    expect(searchTermFor("in de")).toBe("in de");
  });
});
