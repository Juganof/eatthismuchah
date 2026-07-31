import { describe, expect, it } from "vitest";
import type { Recipe } from "../src/ah/types";
import { deriveTags, forbiddenTags, matchesDiet } from "../src/nutrition/diet";

const recipe = (title: string, ...names: string[]): Recipe => ({
  id: "R-R1",
  title,
  url: "",
  servings: 4,
  imageUrl: null,
  ingredients: names.map((name) => ({ name, quantity: 1, unit: "g" })),
});

const tagsOf = (title: string, ...names: string[]) => deriveTags(recipe(title, ...names));

describe("deriveTags", () => {
  it("recognises meat, fish and dairy", () => {
    expect(tagsOf("Kip met rijst", "kipfilet")).toContain("vlees");
    expect(tagsOf("Zalm uit de oven", "zalmfilet")).toContain("vis");
    expect(tagsOf("Pasta", "mozzarella")).toContain("zuivel");
  });

  it("labels pork as both pork and meat, so both diets catch it", () => {
    const tags = tagsOf("Saltimbocca", "prosciutto di parma");
    expect(tags).toContain("varken");
    expect(tags).toContain("vlees");
  });

  /**
   * Dit is waar het Nederlands de tagger sloopt: samengestelde woorden. Een
   * regex met \b ervoor mist ze allemaal, en dan lekt er brood door een
   * glutenvrij filter.
   */
  it("sees ingredients inside Dutch compound words", () => {
    expect(tagsOf("Broodje", "waldkorn volkorenbrood"), "volkorenbrood").toContain("gluten");
    expect(tagsOf("Panzanella", "afbakciabatta"), "afbakciabatta").toContain("gluten");
    expect(tagsOf("Balletjes", "kipgehakt naturel"), "kipgehakt").toContain("vlees");
    expect(tagsOf("Spread", "zuivelspread naturel"), "zuivelspread").toContain("zuivel");
    expect(tagsOf("Brood", "knoflookboter"), "knoflookboter").toContain("zuivel");
  });

  it("does not read free-range eggs as chicken meat", () => {
    // AH schrijft scharreleieren als "kip buiteneieren".
    const tags = tagsOf("Panzanella met ei", "kip buiteneieren");
    expect(tags).toContain("ei");
    expect(tags).not.toContain("vlees");
  });

  it("does not read a meat substitute as meat", () => {
    expect(tagsOf("Vegabowl", "vega gehakt")).not.toContain("vlees");
    expect(tagsOf("Bowl", "vegetarische kipstuckjes")).not.toContain("vlees");
  });

  it("labels the eating moment from title and ingredients", () => {
    expect(tagsOf("Overnight oats", "havermout")).toContain("ontbijt");
    expect(tagsOf("Tosti", "brood")).toContain("lunch");
    expect(tagsOf("Ovenschotel", "aardappel")).toContain("diner");
  });

  it("returns nothing rather than guessing on an unrecognised recipe", () => {
    expect(tagsOf("Iets onbekends", "sterrenstof")).toEqual([]);
  });
});

describe("forbiddenTags", () => {
  it("collects the tags a set of diets rules out", () => {
    expect(forbiddenTags(["vegetarisch"]).sort()).toEqual(["varken", "vis", "vlees"]);
    expect(forbiddenTags(["geen_vis"])).toEqual(["vis"]);
  });

  it("merges several diets without duplicating", () => {
    const tags = forbiddenTags(["vegetarisch", "geen_vis", "lactosevrij"]);
    expect(tags.filter((t) => t === "vis")).toHaveLength(1);
    expect(tags).toContain("zuivel");
  });

  it("ignores a diet it does not know", () => {
    expect(forbiddenTags(["paleo-maandag"])).toEqual([]);
  });
});

describe("matchesDiet", () => {
  it("rejects a recipe that carries a forbidden tag", () => {
    expect(matchesDiet(["vlees", "diner"], ["vegetarisch"])).toBe(false);
    expect(matchesDiet(["zuivel", "ontbijt"], ["vegetarisch"])).toBe(true);
  });

  it("accepts everything when no diet is set", () => {
    expect(matchesDiet(["vlees", "vis", "varken"], [])).toBe(true);
  });

  it("lets a recipe without labels through", () => {
    // Onbekend is niet hetzelfde als verboden; het filter mag niet alles wegvagen.
    expect(matchesDiet([], ["veganistisch"])).toBe(true);
  });
});
