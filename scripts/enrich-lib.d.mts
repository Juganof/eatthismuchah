// Type-declaraties voor scripts/enrich-lib.mjs, zodat de vitest-imports in
// test/ onder strict typechecken vallen. De implementatie staat in het
// .mjs-bestand; deze export-namen en hun vorm moeten daarmee in de pas
// blijven (zie de header daar voor de volledige toelichting).

import type { Store } from "../src/db/queries";
import type { Recipe } from "../src/ah/types";
import type { ProductSuggestion } from "../src/ah/gql";

export type CurlContext = {
  ensureSession: () => Promise<void>;
  gqlPost: (query: string, variables: Record<string, unknown>) => Promise<unknown>;
};

export type EnrichSummary = {
  gekoppeld: number;
  nieuw: number;
  cached: number;
  fouten: number;
};

/** Zoekt het echte D1-databasebestand onder .wrangler/state/ van de repo. */
export function findLocalDb(rootDir: string): string | null;

/**
 * Opent de lokale D1-database en geeft er een Store op terug (busy_timeout
 * 5000). Sluiten kan met `store.db.close()`.
 */
export function openStore(dbPath: string): Store;

/** Opent de sessie: één rustige GET op de homepage die de cookie-jar vult. */
export function ensureSession(): Promise<void>;

/** Eén POST op /gql via curl.exe; retry bij 403/429, ≥1 s tempo. */
export function curlJson(query: string, variables: Record<string, unknown>): Promise<unknown>;

/** De curl-sessie als één object: ensureSession + gqlPost. */
export function curlContext(): CurlContext;

/**
 * De zoekrespons (productSearch { products }) naar ProductSuggestion-vorm;
 * gooit bij een expliciete GraphQL-foutrespons, geeft [] bij een leeg
 * zoekresultaat of een 200 zonder products-lijst.
 */
export function parseSearchResults(body: unknown): ProductSuggestion[];

/**
 * De niet-vrije ingrediënten van een recept die nog geen productkoppeling
 * hebben; leeg betekent: niets te verrijken.
 */
export function openIngredientNames(store: Store, recipe: Recipe): Promise<string[]>;

/**
 * Of een recept een klaar-markering heeft: een eerdere verrijkingspoging
 * zonder fouten is afgerond, ook als sommige ingrediënten open bleven.
 */
export function isRecipeDone(store: Store, recipeId: string): Promise<boolean>;

/** Zet de klaar-markering; zie isRecipeDone. */
export function markRecipeDone(store: Store, recipeId: string): Promise<void>;

/**
 * Of een recept een verrijkingsronde nodig heeft: nog niet klaar gemarkeerd
 * én met minstens één niet-vrij ingrediënt zonder koppeling. Dit is de
 * selectie die de watcher per ronde maakt.
 */
export function needsEnrichment(store: Store, recipe: Recipe): Promise<boolean>;

/**
 * Verrijkt één recept en geeft een samenvatting terug, of null als alle
 * niet-vrije ingrediënten al gekoppeld waren.
 */
export function enrichOneRecipe(
  store: Store,
  curlCtx: CurlContext,
  recipe: Recipe,
): Promise<EnrichSummary | null>;
