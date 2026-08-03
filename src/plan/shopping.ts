import type { Plan, PlannedIngredient } from "../optimize/plan";

/**
 * Telt de ingredienten van een reeks maaltijdplannen op tot één boodschappenlijst.
 *
 * De plannen zijn al herschaald, dus hier hoeft niets meer gerekend te worden aan
 * porties — alleen opgeteld. Samenvoegen gebeurt op de genormaliseerde naam, want
 * "kipfilet" uit het ontbijt en "Kipfilet" uit het diner zijn dezelfde boodschap.
 *
 * Waar het automatisch gevulde product bekend is (via de webshop-id uit de
 * ingredient_matches en de verpakking uit de products-tabel) zegt de lijst ook
 * wát je pakt: "2 × 330 g" in plaats van alleen 660 g. Eén uniek product per
 * regel is daarvoor de voorwaarde — komen er verschillende producten in één
 * regel samen, dan blijven de grammen staan.
 */

const PRODUCT_PAGE = "https://www.ah.nl/producten/product/wi";

/** Een verpakkingsetiket omgerekend naar iets waarop gerekend kan worden. */
export type PackSize =
  | { kind: "grams"; grams: number }
  | { kind: "pieces"; pieces: number };

/**
 * Parseert een AH-verpakkingsetiket: "330 g"/"500 gram" zijn grammen, "1 kg" en
 * "1 l" zijn 1000 gram (liters rekenen we als grammen, net als per-100-ml
 * voedingswaarde), "100 ml" is 100 gram, en "2 stuks" is een aantal. Alles wat
 * niet zo in elkaar zit — null, leeg, "pakje verse basilicum" — is null.
 *
 * Tolerantie: een "ca."/"circa"/"ongeveer"/±-voorvoegsel verandert niets aan
 * het getal; "ca. 400 g" is gewoon 400 g. Dat is bewust de enige vorm van
 * onnauwkeurigheid die we accepteren — "1-2 stuks" laten we liggen, want daar
 * valt geen vaste verpakking uit af te leiden.
 */
export function parsePackSize(size: string | null): PackSize | null {
  if (!size) return null;
  const zonderVoorvoegsel = size.trim().replace(/^(?:ca\.?|circa|ongeveer|±)\s*/i, "");
  const match = /^([0-9]+(?:\.[0-9]+)?)\s*(g|gram|kg|kilo|l|liter|ml|milliliter|stuks?|st)$/i.exec(
    zonderVoorvoegsel,
  );
  if (!match) return null;
  const value = Number(match[1]);
  switch (match[2]!.toLowerCase()) {
    case "g":
    case "gram":
      return { kind: "grams", grams: value };
    case "kg":
    case "kilo":
      return { kind: "grams", grams: value * 1000 };
    case "l":
    case "liter":
      return { kind: "grams", grams: value * 1000 };
    case "ml":
    case "milliliter":
      return { kind: "grams", grams: value };
    case "stuk":
    case "stuks":
    case "st":
      return { kind: "pieces", pieces: value };
    default:
      return null;
  }
}

/**
 * Hoeveel verpakkingen je nodig hebt voor een totaalgewicht: altijd naar boven
 * afgerond en minstens één — je koopt geen half blik. 600 g uit 330 g-blikken
 * is 2 blikken, net als precies 660 g; bij 661 g wordt het 3.
 */
export function packagesFor(totalGrams: number, packGrams: number): number {
  if (!(totalGrams > 0) || !(packGrams > 0)) return 1;
  return Math.max(1, Math.ceil(totalGrams / packGrams));
}

/**
 * Het aantal losse stuks voor regels die in stuks zijn opgeschreven
 * (gramsSource "piece"): de opgetelde originalQuantity van de gebruiken.
 * Ontbreekt er bij één gebruik een aantal, dan valt er niets op te tellen en
 * is het antwoord null.
 */
export function piecesFor(uses: Array<number | null>): number | null {
  if (uses.length === 0) return null;
  let totaal = 0;
  for (const use of uses) {
    if (use === null) return null;
    totaal += use;
  }
  return Math.round(totaal * 10) / 10;
}

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
  /**
   * Hoeveel verpakkingen er in de winkel gepakt moeten worden, bv. 2 bij 600 g
   * uit 330 g-blikken. Null zolang er geen uniek gram-pak bekend is.
   */
  packages: number | null;
  /** De verpakking zoals de winkel ze verkoopt, bv. "2 × 330 g". */
  packagesLabel: string | null;
  /**
   * Aantal losse stuks (verse producten die het recept in stuks opschreef),
   * bv. 5 bananen. Null zolang dat niet te bepalen is.
   */
  pieces: number | null;
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

/**
 * Zet de boodschappenlijst om naar platte tekst: één regel per item, om te
 * kopieren voor in de winkel of om te delen. "Kikkererwten — 2 × 330 g".
 *
 * Per regel dezelfde keuze als de UI maakt: verpakkingen als die bekend zijn,
 * anders het aantal stuks, anders de grammen. Een regel zonder product krijgt
 * "(controleer zelf)" erachter; een bekende productlink komt er tussen haakjes
 * bij. Deze functie blijft vrij van backticks en ${-tekens, want de UI schrijft
 * de bron ervan in een template-string (zie src/ui/script.ts).
 */
export function shoppingLinesToText(lines: ShoppingLine[]): string {
  return lines
    .map((line) => {
      const amt = line.packagesLabel
        ? line.packagesLabel
        : line.pieces != null
          ? Math.round(line.pieces) + " stuks"
          : Math.round(line.grams) + " g";
      const flag = line.unmatched ? " (controleer zelf)" : "";
      const link = line.productUrl ? " (" + line.productUrl + ")" : "";
      return line.name + " — " + amt + flag + link;
    })
    .join("\n");
}

export interface ShoppingInput {
  /** Naam van de maaltijd, voor "gebruikt in". */
  label: string;
  plan: Plan;
  /** Optioneel: ingredientnaam -> webshop-id, voor productlinks. */
  webshopIds?: Record<string, string>;
  /**
   * Optioneel: webshop-id -> productgegevens uit de products-tabel, voor het
   * aantal verpakkingen. De route vult deze automatisch aan uit de ingest.
   */
  products?: Record<string, { title: string; salesUnitSize: string | null }>;
}

/** De interne vorm van een regel, zolang hij nog samengevoegd kan worden. */
interface LineAcc {
  name: string;
  grams: number;
  productTitle: string | null;
  productUrl: string | null;
  usedIn: string[];
  unmatched: boolean;
  /** Alle webshop-ids die de gebruiken van deze regel hebben opgeleverd. */
  ids: Set<string>;
  /** originalQuantity van elke gebruik dat in stuks is opgeschreven. */
  pieceUses: Array<number | null>;
}

/**
 * Bepaalt achteraf per samengevoegde regel wat de winkelier pakt. Eén uniek
 * webshop-id met een parseerbaar gram-pak geeft het aantal verpakkingen;
 * een stuks-pak of stuks-gebruiken geeft het aantal stuks; anders niets, en
 * blijven de grammen zichtbaar.
 */
function packInfoFor(
  line: LineAcc,
  products: Record<string, { title: string; salesUnitSize: string | null }>,
): Pick<ShoppingLine, "productTitle" | "packages" | "packagesLabel" | "pieces"> {
  const id = line.ids.size === 1 ? [...line.ids][0] : null;
  const packSize = id ? (products[id]?.salesUnitSize ?? null) : null;
  // De titel uit de products-tabel is betrouwbaarder dan wat de plannen
  // meedroegen, maar alleen als het echt één product achter de regel is.
  const productTitle = id && products[id] ? products[id]!.title : line.productTitle;

  const parsed = parsePackSize(packSize);
  if (parsed?.kind === "grams") {
    const packages = packagesFor(line.grams, parsed.grams);
    return {
      productTitle,
      packages,
      packagesLabel: packages + " × " + (packSize ?? parsed.grams + " g"),
      pieces: null,
    };
  }
  if (parsed?.kind === "pieces" || line.pieceUses.length > 0) {
    return { productTitle, packages: null, packagesLabel: null, pieces: piecesFor(line.pieceUses) };
  }
  return { productTitle, packages: null, packagesLabel: null, pieces: null };
}

export function buildShoppingList(inputs: ShoppingInput[]): ShoppingLine[] {
  const lines = new Map<string, LineAcc>();

  // De products-kaarten zijn per input hetzelfde (de route geeft één kaart
  // mee); samenvoegen maakt de functie ook voor een mix van inputs robuust.
  const products: Record<string, { title: string; salesUnitSize: string | null }> = {};
  for (const input of inputs) {
    for (const [id, product] of Object.entries(input.products ?? {})) {
      products[id] ??= product;
    }
  }

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
        const webshopId = webshopIds?.[ingredient.name];
        if (webshopId) existing.ids.add(webshopId);
        if (ingredient.gramsSource === "piece") existing.pieceUses.push(ingredient.originalQuantity);
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
        ids: new Set(webshopId ? [webshopId] : []),
        pieceUses: ingredient.gramsSource === "piece" ? [ingredient.originalQuantity] : [],
      });
    }
  }

  return [...lines.values()]
    .map((line) => ({ ...line, ...packInfoFor(line, products), grams: Math.round(line.grams * 10) / 10 }))
    .sort((a, b) => b.grams - a.grams)
    .map(({ ids, pieceUses, ...line }) => line);
}
