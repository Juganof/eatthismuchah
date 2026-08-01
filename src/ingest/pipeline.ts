import { AhClient, isBudgetError } from "../ah/client";
import type { Recipe } from "../ah/types";
import { Store } from "../db/queries";
import { deriveTags } from "../nutrition/diet";
import { coverageOf, firstIncomplete, resolveRecipe } from "../nutrition/resolve";

/**
 * Het scrapen zelf, los van de routes. Zowel de knoppen in de UI als de
 * automatische ronde uit `auto.ts` lopen hierlangs, zodat er maar één plek is
 * waar bepaald wordt hoe een recept binnenkomt en wordt weggeschreven.
 *
 * Eén regel bepaalt alles hier: een recept wordt in één keer helemaal afgemaakt
 * of het wordt niet opgeslagen. Geen titels zonder ingredienten, geen
 * ingredienten zonder voedingswaarde, geen totalen die later nog moeten worden
 * bijgewerkt. Wat in de database staat, kan de planner gebruiken.
 */

export interface ScrapeEnv {
  DB: D1Database;
  AH_USER_AGENT: string;
}

/**
 * Zoektermen per eetmoment. AH's zoekfunctie kent zijn eigen indeling
 * ("menugang": ontbijt, lunch, tussendoortje, hoofdgerecht), maar die filter zit
 * achter een GraphQL-aanroep die we niet kunnen vastpinnen. Zoeken op deze
 * woorden levert dezelfde hoek van de catalogus op.
 *
 * Wat een recept uiteindelijk ís, bepaalt AH's eigen label (zie `deriveTags`) en
 * niet de zoekterm waarmee we het vonden: een recept dat AH als lunch labelt is
 * ook bruikbaar als de ontbijtronde het tegenkwam.
 */
export const MOMENT_QUERIES: Record<string, string[]> = {
  ontbijt: ["ontbijt", "havermout", "kwark ontbijt", "smoothie", "pannenkoeken", "yoghurt"],
  lunch: ["lunch", "salade", "soep", "broodje", "wrap", "tosti"],
  snack: ["tussendoortje", "snack", "energiereep", "hapje", "dip"],
  diner: ["hoofdgerecht", "pasta", "ovenschotel", "curry", "rijst", "traybake"],
};

export const MOMENTS = Object.keys(MOMENT_QUERIES);

/** Het eetmoment dat AH zelf aan een recept hangt, of null als het niets zegt. */
export function momentOf(recipe: Recipe): string | null {
  for (const tag of deriveTags(recipe)) {
    if (tag === "ontbijt" || tag === "lunch" || tag === "snack" || tag === "diner") return tag;
  }
  return null;
}

/** Een client die alles wat hij ophaalt meteen archiveert. */
export interface ClientOptions {
  minIntervalMs?: number;
  backoffMs?: number;
  maxRetries?: number;
  /** Bovengrens op het aantal verzoeken aan ah.nl; zie SubrequestBudgetError. */
  maxRequests?: number;
  /** Sla de JSON-zoekdienst over; zie RECIPE_JSON_DEAD hieronder. */
  skipRecipeJsonSearch?: boolean;
}

/** app_state-sleutel: de JSON-zoekdienst van Allerhande geeft alleen nog 404. */
export const RECIPE_JSON_DEAD = "ah:recipe_json_dead";

/**
 * Of een antwoord van een JSON-endpoint ook echt JSON is. Een 404 die een
 * HTML-foutpagina teruggeeft telt hier net zo goed als kapot: in beide gevallen
 * heeft het geen zin dit endpoint volgende ronde weer te proberen.
 */
function isUsableJson(raw: { status: number; body: string }): boolean {
  return raw.status < 400 && /^\s*[[{]/.test(raw.body);
}

export function scrapeClient(env: ScrapeEnv, store: Store, options?: ClientOptions) {
  return new AhClient(
    env.AH_USER_AGENT,
    (raw) => {
      // Alleen receptpagina's het archief in. Productpagina's zijn 500-700 kB per
      // stuk en we bewaren er toch alleen de voedingswaarde uit; recept-HTML is
      // waarmee parserfouten te vinden en te repareren zijn.
      if (raw.kind === "recipe" || raw.kind === "recipe_search") void store.putRaw(raw);

      // Eén keer vaststellen dat dit endpoint dood is, en dat onthouden over
      // rondes heen: anders kost het elke twee minuten opnieuw een verzoek uit
      // een budget van veertig, én is het een tweede verzoek binnen een seconde
      // richting een site die juist op tempo let.
      if (raw.url.includes("/service/search/recipes") && !isUsableJson(raw)) {
        void store.setState(RECIPE_JSON_DEAD, "1");
      }
      // Elk verzoek aan ah.nl ook als logregel: de statuscode per verzoek is
      // precies wat je wilt zien als er niets binnenkomt.
      void store.log(raw.status >= 400 ? "warn" : "info", "ah", `${raw.kind} ${raw.ref} -> ${raw.status}`, {
        url: raw.url,
        bytes: raw.body.length,
      });
    },
    options,
  );
}

/**
 * Herkent de blokkade van AH's botbescherming. Let op het verschil met
 * `isBudgetError`: dat is óns eigen plafond en zegt niets over AH, dus daar mag
 * nooit een afkoelperiode op volgen.
 */
export function isBlocked(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return / -> (403|429)$/.test(message);
}

export interface IngestResult {
  /** Recepten die compleet binnenkwamen en zijn opgeslagen. */
  added: number;
  /** Recepten die niet compleet te krijgen waren; die komen nooit meer terug. */
  rejected: number;
  /** Hoe vaak AH ons afknijpte; hierop wordt het tempo bijgesteld. */
  blocked: number;
  errors: string[];
}

/** Wat er met één recept gebeurde. */
export type CompleteOutcome = "opgeslagen" | "afgekeurd" | "overgeslagen";

/**
 * Maakt één recept helemaal af, of keurt het af.
 *
 * Dit is de enige plek waar een recept de database in gaat, en de regel is
 * overal dezelfde: van elk ingredient moet de voedingswaarde bekend zijn. Dat
 * kan op drie manieren, in deze volgorde: een AH-product met een tabel per
 * 100 g, nul omdat het niets bijdraagt (water, zout — zie `isNutritionFree`),
 * of het deel van AH's eigen recepttotaal dat aan die regel toevalt (zie
 * `fillFromRecipeTotal`). Pas als geen van drieën lukt, gaat het recept eruit.
 *
 * Recept en totaal gaan samen naar binnen; een van de twee zonder de ander is
 * precies het halffabricaat dat we niet willen.
 *
 * `cacheOnly` doet hetzelfde zonder één verzoek aan ah.nl — dat is wat
 * /api/reparse gebruikt: alleen recepten waarvan we alle producten al kennen.
 */
export async function completeRecipe(
  store: Store,
  client: AhClient,
  recipe: Recipe,
  options: { cacheOnly?: boolean } = {},
): Promise<CompleteOutcome> {
  if (recipe.ingredients.length === 0) {
    await store.skipRecipe(recipe.id, "geen ingredientenlijst op de pagina");
    return "afgekeurd";
  }

  const resolved = await resolveRecipe(recipe, client, store, options);
  const missing = firstIncomplete(resolved);
  if (missing) {
    // Zonder netwerk is "niet compleet" geen oordeel over het recept maar over
    // wat we toevallig in de cache hebben; dan niets vastleggen.
    if (options.cacheOnly) return "overgeslagen";
    await store.skipRecipe(recipe.id, `geen voedingswaarde voor "${missing}"`);
    await store.log("info", "ingest", `${recipe.title} afgekeurd`, {
      recept: recipe.id,
      ingredient: missing,
      reden: "geen product en geen voedingswaarde van AH zelf",
    });
    return "afgekeurd";
  }

  const geschat = resolved.ingredients.filter((i) => i.nutrientSource === "geschat").length;
  await store.putRecipe(recipe);
  await store.putNutrition(recipe.id, resolved.total, coverageOf(resolved), resolved.source);
  await store.log("info", "ingest", `${recipe.title} opgeslagen`, {
    recept: recipe.id,
    ingredienten: recipe.ingredients.length,
    kcal: Math.round(resolved.total.kcal ?? 0),
    bron: resolved.source === "ah" ? "voedingswaarde van AH" : "opgeteld uit producten",
    ...(geschat > 0 ? { geschatteRegels: geschat } : {}),
  });
  return "opgeslagen";
}

/**
 * Haalt losse recepten op en maakt ze af. Hier hangt /api/search aan: wat je
 * zoekt wordt op de achtergrond compleet gemaakt, of het komt er niet in.
 */
export async function completeRecipeIds(
  env: ScrapeEnv,
  ids: string[],
  clientOptions?: ClientOptions,
): Promise<void> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store, clientOptions);

  for (const id of ids) {
    if (client.budget.max - client.budget.used < 4) break;
    try {
      if (await store.isKnownRecipe(id)) continue;
      const recipe = await client.getRecipe(id);
      if (recipe) await completeRecipe(store, client, recipe);
    } catch {
      // volgende recept; dit draait buiten het antwoord om
    }
  }
}

/**
 * Eén ronde: zoeken, en elk gevonden recept helemaal afmaken.
 *
 * "Helemaal afmaken" betekent: van elk ingredient is de voedingswaarde bekend —
 * uit een AH-product, omdat het nul is, of uit AH's eigen recepttotaal (zie
 * `completeRecipe`). Lukt dat, dan gaan recept én totaal in één keer de database
 * in. Lukt het niet, dan komt het recept in `skipped_recipes` en wordt het nooit
 * opnieuw opgehaald.
 *
 * Raakt het verzoekbudget op midden in een recept, dan gebeurt er niets: niet
 * opslaan, niet afkeuren. Dat is het verschil tussen "past nu niet" en "kan niet".
 */
export async function ingestComplete(
  env: ScrapeEnv,
  queries: string[],
  limit: number,
  clientOptions?: ClientOptions,
): Promise<IngestResult> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store, {
    ...clientOptions,
    skipRecipeJsonSearch: (await store.getState(RECIPE_JSON_DEAD)) === "1",
  });

  let added = 0;
  let rejected = 0;
  let blocked = 0;
  const errors: string[] = [];

  // Vier verzoeken over is het minimum om een recept nog te kúnnen afmaken: de
  // pagina zelf plus één onbekend ingredient.
  const budgetLeft = () => client.budget.max - client.budget.used;

  for (const query of queries) {
    if (budgetLeft() < 4 || added >= limit) break;

    let found: Recipe[];
    try {
      found = await client.searchRecipes(query, Math.max(limit, 10));
    } catch (err) {
      if (isBudgetError(err)) break;
      if (isBlocked(err)) blocked++;
      const message = err instanceof Error ? err.message : String(err);
      errors.push(`zoeken "${query}": ${message}`);
      await store.log("error", "ingest", `zoeken op "${query}" mislukt`, { fout: message });
      continue;
    }

    for (const stub of found) {
      if (budgetLeft() < 4 || added >= limit) break;
      // Al bekend of al afgekeurd: kost geen enkel verzoek om over te slaan.
      if (await store.isKnownRecipe(stub.id)) continue;

      try {
        const recipe = stub.ingredients.length > 0 ? stub : await client.getRecipe(stub.id);
        if (!recipe) {
          await store.skipRecipe(stub.id, "receptpagina gaf geen recept");
          rejected++;
          continue;
        }

        const outcome = await completeRecipe(store, client, recipe);
        if (outcome === "opgeslagen") added++;
        else if (outcome === "afgekeurd") rejected++;
      } catch (err) {
        // Budget op: niets vastleggen. Volgende ronde begint dit recept opnieuw.
        if (isBudgetError(err)) break;
        if (isBlocked(err)) blocked++;
        const message = err instanceof Error ? err.message : String(err);
        errors.push(`${stub.id}: ${message}`);
        await store.log("error", "ingest", `recept ${stub.id} mislukt`, { fout: message });
      }
    }
  }

  await store.log(
    blocked > 0 ? "warn" : "info",
    "ingest",
    `${added} recepten opgeslagen, ${rejected} afgekeurd`,
    { queries, blocked, fouten: errors.length, verzoeken: `${client.budget.used}/${client.budget.max}` },
  );
  return { added, rejected, blocked, errors };
}
