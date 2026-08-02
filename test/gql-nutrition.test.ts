import { describe, expect, it } from "vitest";
import { parseTradeItem } from "../src/ah/gql-nutrition";

/**
 * De parser voor AH's `product(id) { tradeItem { nutritions } }`-payload.
 * De fixtures hieronder zijn overgenomen uit échte antwoorden van
 * `https://www.ah.nl/gql` (gevalideerd op 2026-08-02): Philadelphia original
 * (384175), AH Terra kikkererwten (168813) en de virtuele 2-pack (562458).
 */

const PHILADELPHIA = {
  __typename: "ProductTradeItem",
  gtin: "05410068237303",
  nutritions: [
    {
      __typename: "ProductTradeItemNutrition",
      basisQuantity: "100.0 Gram",
      basisQuantityDescription: "",
      preparationState: "Onbereide",
      servingSize: null,
      servingSizeDescription: null,
      nutrients: [
        { type: "ENER-", name: "Energie", value: "933.0 kJ (226.0 kcal)" },
        { type: "FAT", name: "Vet", value: "21.0 g" },
        { type: "FASAT", name: "waarvan verzadigd", value: "14.0 g" },
        { type: "CHOAVL", name: "Koolhydraten", value: "4.3 g" },
        { type: "SUGAR-", name: "waarvan suikers", value: "4.3 g" },
        { type: "FIBTG", name: "Voedingsvezel", value: "0.2 g" },
        { type: "PRO-", name: "Eiwitten", value: "5.4 g" },
        { type: "SALTEQ", name: "Zout", value: "0.75 g" },
      ],
    },
    {
      __typename: "ProductTradeItemNutrition",
      basisQuantity: "30.0 Gram",
      basisQuantityDescription: "1 portie = 30 g.",
      preparationState: "Onbereide",
      servingSize: "30.0 Gram",
      servingSizeDescription: "1 portie = 30 g.",
      nutrients: [
        { type: "ENER-", name: "Energie", value: "280.0 kJ (68.0 kcal)" },
        { type: "FAT", name: "Vet", value: "6.2 g" },
        { type: "FASAT", name: "waarvan verzadigd", value: "4.1 g" },
        { type: "CHOAVL", name: "Koolhydraten", value: "1.3 g" },
        { type: "SUGAR-", name: "waarvan suikers", value: "1.3 g" },
        { type: "FIBTG", name: "Voedingsvezel", value: "0.1 g" },
        { type: "PRO-", name: "Eiwitten", value: "1.6 g" },
        { type: "SALTEQ", name: "Zout", value: "0.23 g" },
      ],
    },
  ],
};

describe("parseTradeItem", () => {
  it("leest de voedingswaarde per 100 g uit AH's tradeItem-payload", () => {
    const result = parseTradeItem(PHILADELPHIA);

    expect(result).not.toBeNull();
    expect(result!.gtin).toBe("05410068237303");
    expect(result!.per100g).toEqual({
      kcal: 226,
      fat: 21,
      carbs: 4.3,
      protein: 5.4,
      fiber: 0.2,
    });
  });

  it("kiest het 100-gram-blok boven het portieblok", () => {
    const result = parseTradeItem(PHILADELPHIA);

    // Het 30 g-blok zou 68 kcal opleveren; het 100 g-blok 226.
    expect(result!.per100g.kcal).toBe(226);
  });

  it("haalt kcal uit '933.0 kJ (226.0 kcal)' zonder de kJ te tellen", () => {
    const result = parseTradeItem({
      gtin: "1",
      nutritions: [
        {
          basisQuantity: "100.0 Gram",
          nutrients: [{ type: "ENER-", name: "Energie", value: "933.0 kJ (226.0 kcal)" }],
        },
      ],
    });

    expect(result!.per100g.kcal).toBe(226);
  });

  it("leest komma-getallen ('4,3 g') als decimaal", () => {
    const result = parseTradeItem({
      gtin: "1",
      nutritions: [
        {
          basisQuantity: "100.0 Gram",
          nutrients: [{ type: "CHOAVL", name: "Koolhydraten", value: "4,3 g" }],
        },
      ],
    });

    expect(result!.per100g.carbs).toBe(4.3);
  });

  it("telt sub-rijen niet dubbel: verzadigd vet is geen totaal vet", () => {
    const result = parseTradeItem({
      gtin: "1",
      nutritions: [
        {
          basisQuantity: "100.0 Gram",
          nutrients: [
            { type: "FAT", name: "Vet", value: "21.0 g" },
            { type: "FASAT", name: "waarvan verzadigd", value: "14.0 g" },
          ],
        },
      ],
    });

    expect(result!.per100g.fat).toBe(21);
  });

  it("negeert onbekende nutrient-types zoals onverzadigd vet (X_FUNS)", () => {
    const result = parseTradeItem({
      gtin: "1",
      nutritions: [
        {
          basisQuantity: "100.0 Gram",
          nutrients: [
            { type: "X_FUNS", name: "waarvan onverzadigd", value: "2.3 g" },
            { type: "PRO-", name: "Eiwitten", value: "6.5 g" },
          ],
        },
      ],
    });

    expect(result!.per100g.protein).toBe(6.5);
    expect(result!.per100g.fat).toBeUndefined();
  });

  it("laat zout (SALTEQ) buiten de macro's, want de planner rekent er niet in", () => {
    const result = parseTradeItem({
      gtin: "1",
      nutritions: [
        {
          basisQuantity: "100.0 Gram",
          nutrients: [{ type: "SALTEQ", name: "Zout", value: "0.75 g" }],
        },
      ],
    });

    expect(result!.per100g).toEqual({});
  });

  it("geeft null voor een virtuele bundel zonder tradeItem", () => {
    expect(parseTradeItem(null)).toBeNull();
    expect(parseTradeItem(undefined)).toBeNull();
  });

  it("geeft null als er geen enkel bruikbaar voedingsblok is", () => {
    expect(parseTradeItem({ gtin: "1", nutritions: [] })).toBeNull();
    expect(parseTradeItem({ gtin: "1", nutritions: [{ basisQuantity: "1.0 Liter", nutrients: [] }] })).toBeNull();
  });

  it("verwerkt het echte kikkererwten-antwoord (AH Terra, 168813)", () => {
    const result = parseTradeItem({
      __typename: "ProductTradeItem",
      gtin: "08719587084892",
      nutritions: [
        {
          __typename: "ProductTradeItemNutrition",
          basisQuantity: "100.0 Gram",
          basisQuantityDescription: "215 gram",
          preparationState: "Onbereide",
          servingSize: "100.0 Gram",
          servingSizeDescription: "215 gram",
          nutrients: [
            { type: "ENER-", name: "Energie", value: "504.0 kJ (120.0 kcal)" },
            { type: "FAT", name: "Vet", value: "2.7 g" },
            { type: "FASAT", name: "waarvan verzadigd", value: "0.4 g" },
            { type: "X_FUNS", name: "waarvan onverzadigd", value: "2.3 g" },
            { type: "CHOAVL", name: "Koolhydraten", value: "14.0 g" },
            { type: "SUGAR-", name: "waarvan suikers", value: "0.4 g" },
            { type: "FIBTG", name: "Voedingsvezel", value: "7.0 g" },
            { type: "PRO-", name: "Eiwitten", value: "6.5 g" },
            { type: "SALTEQ", name: "Zout", value: "0.03 g" },
          ],
        },
      ],
    });

    expect(result!.gtin).toBe("08719587084892");
    expect(result!.per100g).toEqual({
      kcal: 120,
      fat: 2.7,
      carbs: 14,
      protein: 6.5,
      fiber: 7,
    });
  });
});
