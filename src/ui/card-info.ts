/**
 * Pure omzettingen voor de receptkaarten, los van de clientcode zodat ze
 * rechtstreeks in Node getest kunnen worden. De clientcode embedt de functies
 * via toString(), net als shoppingLinesToText.
 */

/**
 * Kcal per 100 g voor één planregel, afgeleid uit wat de regel als geheel
 * bijdraagt: kcal gedeeld door gram, maal honderd. Alleen als de regel gewicht
 * draagt; anders is er geen eerlijke "per 100 g"-uitspraak te doen.
 */
export function kcalPer100g(grams: number, kcal?: number | null): number | null {
  if (grams <= 0 || kcal === undefined || kcal === null) return null;
  return Math.round((kcal / grams) * 100);
}
