import type { Nutrients } from "./types";

/**
 * De voedingswaarde zoals AH die in de productpagina-data zet, per 100 g.
 *
 * De bron is de `product(id) { tradeItem { nutritions } }`-query op het
 * (onofficiele) `www.ah.nl/gql`-endpoint. Een tradeItem kan meerdere blokken
 * hebben: één per 100 gram en één per portie ("1 portie = 30 g"). Wij plannen
 * per gram, dus alleen het 100-gram-blok telt.
 */

export interface TradeItemNutrition {
  /** EAN van het product, voor wie er iets mee wil. */
  gtin: string | null;
  /** Voedingswaarde per 100 g (of per 100 ml voor vloeistoffen). */
  per100g: Nutrients;
  /** Verpakkingsgrootte zoals AH hem op het blok zet ("215 gram"). */
  packSizeLabel: string | null;
}

/** Welke nutrient-code van AH aan welke macro van ons hangt. */
const NUTRIENT_TYPES: Record<string, keyof Nutrients> = {
  "ENER-": "kcal",
  FAT: "fat",
  CHOAVL: "carbs",
  FIBTG: "fiber",
  "PRO-": "protein",
};

/** Sub-rijen van een macro die we al hebben; die tellen nooit dubbel. */
const SUB_ROW_TYPES = new Set(["FASAT", "SUGAR-", "X_FUNS"]);

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function num(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const m = v.replace(",", ".").match(/-?\d+(\.\d+)?/);
    if (m) return Number(m[0]);
  }
  return null;
}

function str(v: unknown): string | null {
  return typeof v === "string" && v.trim() !== "" ? v.trim() : null;
}

/**
 * Haalt kcal uit een waarde die zowel kJ als kcal vermeldt
 * ("933.0 kJ (226.0 kcal)") — de kJ-regel is dezelfde energie in een andere
 * eenheid en zou anders dubbel tellen.
 */
function kcalOf(raw: unknown): number | null {
  const text = String(raw ?? "").replace(",", ".");
  const match = text.match(/(\d+(?:\.\d+)?)\s*kcal/i);
  return match?.[1] ? Number(match[1]) : null;
}

/** De voedingswaarde uit één nutrition-blok, gekoppeld aan onze macro's. */
function nutrientsOf(block: Record<string, unknown>): Nutrients {
  const out: Nutrients = {};
  const rows = block["nutrients"];
  if (!Array.isArray(rows)) return out;

  for (const row of rows) {
    if (!isRecord(row)) continue;
    const type = str(row["type"]);
    if (!type || SUB_ROW_TYPES.has(type)) continue;
    const key = NUTRIENT_TYPES[type];
    if (!key) continue;

    const raw = row["value"];
    const value = key === "kcal" ? kcalOf(raw) ?? num(raw) : num(raw);
    if (value !== null && Number.isFinite(value)) out[key] = value;
  }
  return out;
}

/**
 * Welk blok de voedingswaarde per 100 g draagt. AH zet dat in `basisQuantity`
 * ("100.0 Gram"); ontbreekt een 100-gram-blok, dan is het eerste blok met
 * bruikbare cijfers de beste gok.
 */
function pickBlock(nutritions: unknown): Record<string, unknown> | null {
  if (!Array.isArray(nutritions)) return null;

  const usable: Record<string, unknown>[] = [];
  for (const block of nutritions) {
    if (!isRecord(block)) continue;
    const nutrients = block["nutrients"];
    if (Array.isArray(nutrients) && nutrients.length > 0) usable.push(block);
  }
  if (usable.length === 0) return null;

  return (
    usable.find((block) => {
      const quantity = num(block["basisQuantity"]);
      return quantity !== null && Math.abs(quantity - 100) < 0.001;
    }) ?? usable[0]!
  );
}

/**
 * De voedingswaarde per 100 g uit een tradeItem, of null als er geen is
 * (virtuele bundels hebben geen tradeItem, en een product zonder cijfers is
 * voor ons net zo onbruikbaar).
 */
export function parseTradeItem(tradeItem: unknown): TradeItemNutrition | null {
  if (!isRecord(tradeItem)) return null;

  const block = pickBlock(tradeItem["nutritions"]);
  if (!block) return null;

  return {
    gtin: str(tradeItem["gtin"]),
    per100g: nutrientsOf(block),
    packSizeLabel: str(block["basisQuantityDescription"]),
  };
}
