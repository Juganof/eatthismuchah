import { AhClient } from "../ah/client";
import type { Nutrients, Recipe } from "../ah/types";
import { Store } from "../db/queries";
import { deriveTags } from "../nutrition/diet";
import {
  coverageOf,
  lookupIngredientMatch,
  lookupLinkedProduct,
  resolveRecipe,
} from "../nutrition/resolve";

/**
 * Het scrapen zelf, los van de routes. Zowel de knoppen in de UI als de
 * automatische ronde uit `auto.ts` lopen hierlangs, zodat er maar één plek is
 * waar bepaald wordt hoe een recept binnenkomt en wordt weggeschreven.
 */

export interface ScrapeEnv {
  DB: D1Database;
  AH_USER_AGENT: string;
}

/**
 * Zoektermen per eetmoment. AH's zoekfunctie kent zijn eigen indeling
 * ("menugang": ontbijt, lunch, tussendoortje, hoofdgerecht), maar die filter zit
 * achter een GraphQL-aanroep die we niet kunnen vastpinnen. Zoeken op deze
 * woorden levert dezelfde hoek van de catalogus op, en de controle achteraf
 * gebeurt op AH's eigen labels — zie `momentOf`.
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
}

export function scrapeClient(env: ScrapeEnv, store: Store, options?: ClientOptions) {
  return new AhClient(
    env.AH_USER_AGENT,
    (raw) => {
      void store.putRaw(raw);
      // Elk verzoek aan ah.nl ook als logregel: de statuscode per verzoek is
      // precies wat je wilt zien als er niets binnenkomt, en het archief is
      // daar te log voor — daar staat de hele payload in.
      void store.log(raw.status >= 400 ? "warn" : "info", "ah", `${raw.kind} ${raw.ref} -> ${raw.status}`, {
        url: raw.url,
        bytes: raw.body.length,
      });
    },
    options,
  );
}

/** Herkent de blokkade van AH's botbescherming in een foutmelding. */
export function isBlocked(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return / -> (403|429)$/.test(message);
}

/**
 * Bewaart de voedingswaarde die AH zelf bij het recept vermeldt. Dat scheelt een
 * productzoekopdracht per ingredient én is nauwkeuriger, dus dit is het pad dat
 * we het liefst nemen. AH geeft de waarden per portie; de rest van de app rekent
 * met het hele recept.
 */
export async function storeAhNutrition(store: Store, recipe: Recipe): Promise<boolean> {
  const perServing = recipe.nutritionPerServing;
  if (!perServing) return false;

  const servings = recipe.servings > 0 ? recipe.servings : 1;
  const total: Nutrients = {};
  for (const [key, value] of Object.entries(perServing)) {
    total[key as keyof Nutrients] = value * servings;
  }
  // Coverage 1: dit komt van de bron zelf, daar valt niets aan te schatten.
  await store.putNutrition(recipe.id, total, 1, "ah");
  return true;
}

/** Voedingswaarde vastleggen: die van AH als die er is, anders zelf opbouwen. */
export async function computeNutrition(
  store: Store,
  client: AhClient,
  recipe: Recipe,
): Promise<void> {
  if (await storeAhNutrition(store, recipe)) return;
  const resolved = await resolveRecipe(recipe, client, store);
  await store.putNutrition(recipe.id, resolved.total, coverageOf(resolved));
}

export interface IngestResult {
  added: number;
  /** Opgehaald en bewaard, maar niet van het gevraagde eetmoment. */
  skipped: number;
  /** Hoe vaak AH ons afknijpte; hierop wordt het tempo bijgesteld. */
  blocked: number;
  errors: string[];
}

/**
 * `wantedMoment` maakt van een brede zoekopdracht een gerichte: alles wordt
 * opgehaald en bewaard (weggooien van een scrape doen we nooit), maar alleen wat
 * AH als dat moment labelt telt mee voor de limiet.
 */
export async function ingestQueries(
  env: ScrapeEnv,
  queries: string[],
  limit: number,
  wantedMoment?: string,
  clientOptions?: ClientOptions,
): Promise<IngestResult> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store, clientOptions);
  const perQuery = Math.max(1, Math.ceil(limit / Math.max(1, queries.length)));

  let added = 0;
  let skipped = 0;
  let blocked = 0;
  const errors: string[] = [];

  for (const query of queries) {
    let found: Recipe[];
    try {
      found = await client.searchRecipes(query, perQuery);
    } catch (err) {
      if (isBlocked(err)) blocked++;
      const message = err instanceof Error ? err.message : String(err);
      errors.push(`search "${query}": ${message}`);
      await store.log("error", "ingest", `zoeken op "${query}" mislukt`, { fout: message });
      continue;
    }

    // Zoekresultaten dragen geen ingredienten, maar wel titel en id. Die eerst
    // wegschrijven: valt het ophalen van de detailpagina daarna om, dan weten we
    // in elk geval nog dat dit recept bestaat.
    for (const stub of found) {
      try {
        await store.putRecipe(stub);
      } catch (err) {
        errors.push(`opslaan ${stub.id}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    for (const stub of found) {
      if (added >= limit) break;
      try {
        const full = stub.ingredients.length > 0 ? stub : await client.getRecipe(stub.id);
        if (!full || full.ingredients.length === 0) {
          await store.log("warn", "ingest", `${stub.id} kwam zonder ingredienten binnen`, {
            titel: stub.title,
          });
          continue;
        }
        await store.putRecipe(full);
        await computeNutrition(store, client, full);
        // Bewaren doen we altijd; meetellen alleen als het echt dit moment is.
        if (wantedMoment && momentOf(full) !== wantedMoment) {
          skipped++;
          continue;
        }
        added++;
      } catch (err) {
        if (isBlocked(err)) blocked++;
        const message = err instanceof Error ? err.message : String(err);
        errors.push(`recipe ${stub.id}: ${message}`);
        await store.log("error", "ingest", `recept ${stub.id} mislukt`, { fout: message });
      }
    }
    if (added >= limit) break;
  }

  await store.log(
    blocked > 0 ? "warn" : "info",
    "ingest",
    `${added} recepten toegevoegd voor ${wantedMoment ?? "alles"}`,
    { queries, skipped, blocked, fouten: errors.length },
  );
  return { added, skipped, blocked, errors };
}

export interface RepairResult {
  examined: number;
  repaired: number;
  blocked: number;
  errors: string[];
}

/**
 * Haalt recepten op die alleen als titel bestaan. Dat zijn er meestal een hoop:
 * een zoekopdracht levert alleen titels, en als AH's botbescherming daarna de
 * detailpagina blokkeert, blijft het recept leeg staan.
 */
export async function repairEmptyRecipes(
  env: ScrapeEnv,
  limit: number,
  clientOptions?: ClientOptions,
): Promise<RepairResult> {
  const store = new Store(env.DB);
  const ids = await store.recipesWithoutIngredients(limit);
  if (ids.length === 0) return { examined: 0, repaired: 0, blocked: 0, errors: [] };

  const client = scrapeClient(env, store, clientOptions);
  let repaired = 0;
  let blocked = 0;
  const errors: string[] = [];

  for (const id of ids) {
    try {
      const recipe = await client.getRecipe(id);
      if (!recipe || recipe.ingredients.length === 0) {
        errors.push(`${id}: nog steeds geen ingredienten`);
        await store.log("warn", "repair", `${id} heeft nog steeds geen ingredienten`);
        continue;
      }
      await store.putRecipe(recipe);
      await computeNutrition(store, client, recipe);
      repaired++;
    } catch (err) {
      if (isBlocked(err)) blocked++;
      const message = err instanceof Error ? err.message : String(err);
      errors.push(`${id}: ${message}`);
      await store.log("error", "repair", `${id} aanvullen mislukt`, { fout: message });
    }
  }

  await store.log("info", "repair", `${repaired} van ${ids.length} lege recepten aangevuld`, {
    blocked,
  });
  return { examined: ids.length, repaired, blocked, errors };
}

export interface EnrichResult {
  /** Namen geprobeerd. */
  examined: number;
  /** Aan een product gekoppeld. */
  matched: number;
  /** Gezocht, maar niets bruikbaars gevonden — ook een resultaat, wordt onthouden. */
  unmatched: number;
  blocked: number;
  errors: string[];
}

/**
 * Vult ingredient -> product koppelingen aan voor namen die nog nooit opgezocht
 * zijn. Dit is bewust GEEN onderdeel van het binnenhalen van een recept
 * (`computeNutrition` hierboven): daar nemen we AH's eigen voedingswaarde over
 * en doen we nul productzoekopdrachten. Zonder deze aparte, lager-prioriteit
 * ronde blijft `ingredient_matches` voor bijna elk recept leeg, en kan de
 * planner dus nooit "meer kwark, minder granola". Erger nog: zonder koppelingen
 * komt zo'n recept helemaal niet in een dagmenu, want `planFor` in
 * `src/optimize/day.ts` slaat alles over waarvan de voedingswaarde niet per
 * ingredient bekend is.
 */
export async function enrichIngredientMatches(
  env: ScrapeEnv,
  limit: number,
  clientOptions?: ClientOptions,
): Promise<EnrichResult> {
  const store = new Store(env.DB);
  const names = await store.ingredientNamesWithoutMatch(limit);
  if (names.length === 0) return { examined: 0, matched: 0, unmatched: 0, blocked: 0, errors: [] };

  const client = scrapeClient(env, store, clientOptions);
  let matched = 0;
  let unmatched = 0;
  let blocked = 0;
  const errors: string[] = [];

  for (const { name, productId } of names) {
    try {
      // Heeft AH bij het recept zelf al een product aangewezen, dan is dat het:
      // één detailaanroep in plaats van zoeken plus raden, en zonder foutmarge.
      if (productId) {
        const linked = await lookupLinkedProduct(name, productId, client, store);
        if (linked) {
          matched++;
          await store.log("info", "enrich", `"${name}" via AH's eigen productlink`, {
            product: linked.title,
            webshopId: linked.webshopId,
          });
          continue;
        }
      }
      const { outcome, product, score } = await lookupIngredientMatch(name, client, store);
      if (outcome === "matched") {
        matched++;
        await store.log("info", "enrich", `"${name}" gekoppeld aan "${product?.title}"`, {
          webshopId: product?.webshopId,
          score,
        });
      } else {
        unmatched++;
        await store.log("warn", "enrich", `"${name}" leverde geen bruikbaar product op`);
      }
    } catch (err) {
      if (isBlocked(err)) blocked++;
      const message = err instanceof Error ? err.message : String(err);
      errors.push(`${name}: ${message}`);
      await store.log("error", "enrich", `"${name}" opzoeken mislukt`, { fout: message });
    }
  }

  return { examined: names.length, matched, unmatched, blocked, errors };
}
