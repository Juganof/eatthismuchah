import { AhClient, isBudgetError } from "../ah/client";
import type { Recipe } from "../ah/types";
import { Store } from "../db/queries";
import { recipeTotal } from "../nutrition/resolve";
import { enrichRecipeWithProducts } from "./enrich";

/**
 * Het scrapen zelf, los van de routes. Zowel de knoppen in de UI als de
 * automatische ronde uit `auto.ts` lopen hierlangs, zodat er maar één plek is
 * waar bepaald wordt hoe een recept binnenkomt en wordt weggeschreven.
 *
 * Eén regel bepaalt alles hier: een recept gaat er compleet in of helemaal niet.
 * Compleet is sinds het loslaten van de productkoppeling eenvoudig geworden: een
 * ingredientenlijst en AH's eigen voedingswaarde, allebei van dezelfde pagina.
 * Wat in de database staat, kan de planner gebruiken.
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

// Het eetmoment van een recept is tag-logica en woont daarom in
// src/nutrition/diet.ts; deze export houdt de oude publieke plek in stand.
export { momentOf } from "../nutrition/diet";

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

/**
 * Verrijken met echte producten na het opslaan van een recept; zie
 * `enrichRecipeWithProducts`. `perRun` is het aantal recepten per ronde dat
 * verrijkt wordt — 0 betekent uit. Verrijken kost een handvol verzoeken per
 * recept, dus het moet bewust aangezet worden en begrensd.
 */
export interface EnrichConfig {
  perRun: number;
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
 * Slaat één recept op, of keurt het af.
 *
 * De regel is er nog maar één: het recept heeft een ingredientenlijst én AH's
 * eigen voedingswaarde. Dat laatste staat op vrijwel elke Allerhande-pagina, dus
 * afkeuren is de uitzondering geworden in plaats van de regel.
 *
 * Er wordt geen product meer bij gezocht. Zie de kop van
 * `src/nutrition/resolve.ts` voor waarom dat eruit is; hier telt vooral het
 * gevolg: dit doet geen enkel extra verzoek aan ah.nl, dus alles wat een ronde
 * kost is de receptpagina zelf.
 */
export async function completeRecipe(store: Store, recipe: Recipe): Promise<CompleteOutcome> {
  if (recipe.ingredients.length === 0) {
    await store.skipRecipe(recipe.id, "geen ingredientenlijst op de pagina");
    return "afgekeurd";
  }

  const total = recipeTotal(recipe);
  if (!total) {
    await store.skipRecipe(recipe.id, "geen voedingswaarde op de receptpagina");
    await store.log("info", "ingest", `${recipe.title} afgekeurd`, {
      recept: recipe.id,
      reden: "AH geeft geen voedingswaarde bij dit recept",
    });
    return "afgekeurd";
  }

  await store.putRecipe(recipe);
  // Dekking 1: de voedingswaarde is compleet en van AH zelf. Er valt niets meer
  // te wegen aan hoeveel we ervan vonden — dat was de vraag toen het uit losse
  // producten opgeteld werd.
  await store.putNutrition(recipe.id, total, 1, "ah");
  await store.log("info", "ingest", `${recipe.title} opgeslagen`, {
    recept: recipe.id,
    ingredienten: recipe.ingredients.length,
    porties: recipe.servings,
    kcal: Math.round(total.kcal ?? 0),
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
  enrich?: EnrichConfig,
): Promise<void> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store, clientOptions);

  let enriched = 0;
  for (const id of ids) {
    if (client.budget.max - client.budget.used < 1) break;
    try {
      if (await store.isKnownRecipe(id)) continue;
      const recipe = await client.getRecipe(id);
      if (recipe) {
        const outcome = await completeRecipe(store, recipe);
        if (outcome === "opgeslagen" && enrich && enriched < enrich.perRun) {
          enriched++;
          await enrichRecipeWithProducts(env, store, recipe, {
            minIntervalMs: clientOptions?.minIntervalMs,
            backoffMs: clientOptions?.backoffMs,
            maxRequests: client.budget.max - client.budget.used,
          });
        }
      }
    } catch {
      // volgende recept; dit draait buiten het antwoord om
    }
  }
}

/**
 * Eén ronde: zoeken, en elk gevonden recept opslaan.
 *
 * Eén verzoek per recept — de receptpagina, en verder niets. Daar staat alles
 * op: de ingredienten én AH's voedingswaarde. Toen er per ingredient nog een
 * product bij gezocht werd kostte één recept er vijftien tot dertig, en pasten
 * er dus maar twee of drie in een ronde; nu passen er tientallen in hetzelfde
 * budget.
 *
 * Raakt het verzoekbudget op, dan gebeurt er niets: niet opslaan, niet afkeuren.
 * Dat is het verschil tussen "past nu niet" en "kan niet".
 */
export async function ingestComplete(
  env: ScrapeEnv,
  queries: string[],
  limit: number,
  clientOptions?: ClientOptions,
  enrich?: EnrichConfig,
): Promise<IngestResult> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store, {
    ...clientOptions,
    skipRecipeJsonSearch: (await store.getState(RECIPE_JSON_DEAD)) === "1",
  });

  let added = 0;
  let rejected = 0;
  let blocked = 0;
  let enriched = 0;
  const errors: string[] = [];

  // Eén verzoek over is genoeg voor nog een recept: de pagina zelf.
  const budgetLeft = () => client.budget.max - client.budget.used;

  for (const query of queries) {
    if (budgetLeft() < 2 || added >= limit) break;

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
      if (budgetLeft() < 1 || added >= limit) break;
      // Al bekend of al afgekeurd: kost geen enkel verzoek om over te slaan.
      if (await store.isKnownRecipe(stub.id)) continue;

      try {
        const recipe = stub.ingredients.length > 0 ? stub : await client.getRecipe(stub.id);
        if (!recipe) {
          await store.skipRecipe(stub.id, "receptpagina gaf geen recept");
          rejected++;
          continue;
        }

        const outcome = await completeRecipe(store, recipe);
        if (outcome === "opgeslagen") {
          added++;
          if (enrich && enriched < enrich.perRun && budgetLeft() > 2) {
            enriched++;
            const enrichedResult = await enrichRecipeWithProducts(env, store, recipe, {
              minIntervalMs: clientOptions?.minIntervalMs,
              backoffMs: clientOptions?.backoffMs,
              maxRequests: budgetLeft(),
            });
            for (const message of enrichedResult.errors.slice(0, 3)) errors.push(message);
          }
        } else if (outcome === "afgekeurd") rejected++;
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
