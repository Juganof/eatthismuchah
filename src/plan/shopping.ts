import type { Plan, PlannedIngredient } from "../optimize/plan";

/**
 * Telt de ingredienten van een reeks maaltijdplannen op tot één boodschappenlijst.
 *
 * De plannen zijn al herschaald, dus hier hoeft niets meer gerekend te worden aan
 * porties — alleen opgeteld. Samenvoegen gebeurt op de genormaliseerde naam, want
 * "kipfilet" uit het ontbijt en "Kipfilet" uit het diner zijn dezelfde boodschap.
 */

const PRODUCT_PAGE = "https://www.ah.nl/producten/product/wi";

export interface ShoppingLine {
  name: string;
  grams: number;
  /** De AH-producttitel waar de voedingswaarde vandaan kwam, als die er is. */
  productTitle: string | null;
  productUrl: string | null;
  /** Uit welke maaltijden deze regel is opgebouwd. */
  usedIn: string[];
  /** True als er geen product bij gevonden is; controleer die regels zelf. */
  unmatched: boolean;
}

/** Kleine verschillen in schrijfwijze mogen niet tot twee regels leiden. */
function normaliseName(name: string): string {
  return name
    .toLowerCase()
    .replace(/\([^)]*\)/g, " ")
    .replace(/\b(verse?|vers|gesneden|fijngesneden|geraspte?|biologische?|ah|naar smaak)\b/g, " ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Haalt het webshop-id uit een producttitel-koppeling. De plannen dragen alleen de
 * titel, dus zonder id maken we geen link — een gokje zou naar het verkeerde
 * product wijzen, en dat is erger dan geen link.
 */
function productUrlFor(webshopId: string | null): string | null {
  return webshopId ? `${PRODUCT_PAGE}${webshopId}` : null;
}

export interface ShoppingInput {
  /** Naam van de maaltijd, voor "gebruikt in". */
  label: string;
  plan: Plan;
  /** Optioneel: ingredientnaam -> webshop-id, voor productlinks. */
  webshopIds?: Record<string, string>;
}

export function buildShoppingList(inputs: ShoppingInput[]): ShoppingLine[] {
  const lines = new Map<string, ShoppingLine>();

  for (const { label, plan, webshopIds } of inputs) {
    for (const ingredient of plan.ingredients as PlannedIngredient[]) {
      if (ingredient.grams <= 0) continue;
      const key = normaliseName(ingredient.name) || ingredient.name.toLowerCase();
      const existing = lines.get(key);

      if (existing) {
        existing.grams += ingredient.grams;
        if (!existing.usedIn.includes(label)) existing.usedIn.push(label);
        // Een regel is pas betrouwbaar als élk gebruik een product had.
        existing.unmatched = existing.unmatched || ingredient.unmatched;
        existing.productTitle ??= ingredient.productTitle;
        continue;
      }

      const webshopId = webshopIds?.[ingredient.name] ?? null;
      lines.set(key, {
        name: ingredient.name,
        grams: ingredient.grams,
        productTitle: ingredient.productTitle,
        productUrl: productUrlFor(webshopId),
        usedIn: [label],
        unmatched: ingredient.unmatched,
      });
    }
  }

  return [...lines.values()]
    .map((line) => ({ ...line, grams: Math.round(line.grams * 10) / 10 }))
    .sort((a, b) => b.grams - a.grams);
}
