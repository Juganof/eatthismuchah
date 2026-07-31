import { Store } from "../db/queries";
import {
  MOMENTS,
  MOMENT_QUERIES,
  enrichIngredientMatches,
  ingestQueries,
  recomputeNutrition,
  repairEmptyRecipes,
  type ScrapeEnv,
} from "./pipeline";

/**
 * Vult de database de hele dag door zichzelf aan, in kleine porties.
 *
 * De opzet volgt uit hoe ah.nl zich gedraagt. Akamai reageert op tempo, niet op
 * aantallen, dus één grote nachtelijke ingest is precies de verkeerde vorm: die
 * loopt gegarandeerd tegen 403's aan. Veel kleine rondes met rust ertussen komen
 * er wél door, en leveren over een dag genomen veel meer recepten op.
 *
 * Elke ronde doet één ding:
 *   1. staan er lege recepten? die eerst — een titel zonder ingredienten is
 *      waardeloos voor de planner, en het aanvullen kost één pagina per recept;
 *   2. anders: nieuwe recepten voor het eetmoment dat aan de beurt is.
 *
 * Verder houdt hij zich in: een dagbudget zodat we niet eindeloos blijven
 * hameren, en een afkoelperiode zodra AH ons alsnog blokkeert.
 */

/** Sleutels in app_state. */
const CURSOR = "auto:cursor";
const COOLDOWN_UNTIL = "auto:cooldown_until";
const BLOCK_STREAK = "auto:block_streak";
const ENRICH_TURN = "auto:enrich_turn";

export interface AutoConfig {
  /** Recepten per ronde. Klein houden: een ronde moet binnen de worker passen. */
  batch: number;
  /** Bovengrens per dag, zodat de automaat niet eindeloos doorgaat. */
  dailyMax: number;
  /** Rust tussen twee verzoeken binnen een ronde. */
  minIntervalMs: number;
  /** Hoe lang we niets doen nadat AH ons geblokkeerd heeft. */
  cooldownMs: number;
  /**
   * Bovengrens voor de afkoelperiode. Blijft AH blokkeren zodra we het weer
   * proberen, dan verdubbelt de afkoelperiode elke keer — anders blijft de
   * automaat de hele dag om de vaste `cooldownMs` AH opnieuw porren, wat een
   * blokkade die langer duurt dan dat alleen maar verlengt.
   */
  maxCooldownMs: number;
  /** Wachttijd voor de eerste herkansing binnen een ronde; verdubbelt daarna. */
  backoffMs: number;
  /** Ingredientnamen per enrichment-ronde. */
  enrichBatch: number;
  /**
   * Hoe lang een recept de tijd krijgt om bruikbaar te worden voordat het wordt
   * opgeruimd. Een net binnengehaald recept heeft de herstel- en koppelrondes nog
   * niet gehad; pas daarna is "geen ingredienten of geen echte voedingswaarde"
   * ook echt een eindstand.
   */
  purgeGraceMs: number;
  /** Hoeveel onbruikbare recepten er per ronde weggaan. */
  purgeBatch: number;
  /** Hoeveel logregels er bewaard blijven; oudere gaan elke ronde weg. */
  logKeep: number;
  /**
   * Bovengrens op het aantal verzoeken aan ah.nl per ronde. Een worker mag er
   * maar een beperkt aantal doen (50 op het gratis plan); ging dat op, dan kapte
   * Cloudflare de ronde middenin af en faalde elk resterend recept met "Too many
   * subrequests". Onder de grens blijven is de enige manier om dat te vermijden.
   */
  maxRequests: number;
  /** Recepten die per ronde opnieuw doorgerekend worden; kost geen verzoeken. */
  recomputeBatch: number;
  /**
   * Eén op de zoveel niet-repareer-rondes gaat naar ingredient-koppelingen in
   * plaats van nieuwe recepten. Een vaste plek (bv. altijd laatste prioriteit)
   * zou enrichment nooit laten draaien, want de eetmoment-rotatie heeft altijd
   * werk; een vast aandeel laat de dekking wel geleidelijk verbeteren.
   */
  enrichEvery: number;
}

export const DEFAULT_AUTO_CONFIG: AutoConfig = {
  batch: 8,
  dailyMax: 250,
  minIntervalMs: 700,
  cooldownMs: 30 * 60 * 1000,
  maxCooldownMs: 4 * 60 * 60 * 1000,
  backoffMs: 1500,
  enrichBatch: 6,
  enrichEvery: 3,
  purgeGraceMs: 24 * 60 * 60 * 1000,
  purgeBatch: 200,
  logKeep: 2000,
  maxRequests: 40,
  recomputeBatch: 50,
};

export interface AutoResult {
  ran: boolean;
  /** Waarom er niets gebeurde, als er niets gebeurde. */
  reason?: string;
  mode?: "repair" | "moment" | "enrich";
  /** Recepten die deze ronde zijn weggegooid omdat er niets bruikbaars in stond. */
  purged?: number;
  /** Recepten waarvan het totaal opnieuw is opgeteld uit inmiddels bekende producten. */
  recomputed?: number;
  detail?: string;
  added?: number;
  repaired?: number;
  enriched?: number;
  blocked?: number;
  /** Tot wanneer we ons gedeisd houden, als AH ons net geblokkeerd heeft. */
  cooldownUntil?: number;
}

const startOfToday = () => new Date().setUTCHours(0, 0, 0, 0);

export function configFrom(env: Record<string, unknown>): AutoConfig {
  const read = (key: string, fallback: number) => {
    const value = Number(env[key]);
    return Number.isFinite(value) && value > 0 ? value : fallback;
  };
  return {
    batch: read("AUTO_BATCH", DEFAULT_AUTO_CONFIG.batch),
    dailyMax: read("AUTO_DAILY_MAX", DEFAULT_AUTO_CONFIG.dailyMax),
    minIntervalMs: read("AUTO_MIN_INTERVAL_MS", DEFAULT_AUTO_CONFIG.minIntervalMs),
    cooldownMs: read("AUTO_COOLDOWN_MS", DEFAULT_AUTO_CONFIG.cooldownMs),
    maxCooldownMs: read("AUTO_MAX_COOLDOWN_MS", DEFAULT_AUTO_CONFIG.maxCooldownMs),
    backoffMs: read("AUTO_BACKOFF_MS", DEFAULT_AUTO_CONFIG.backoffMs),
    enrichBatch: read("AUTO_ENRICH_BATCH", DEFAULT_AUTO_CONFIG.enrichBatch),
    enrichEvery: read("AUTO_ENRICH_EVERY", DEFAULT_AUTO_CONFIG.enrichEvery),
    purgeGraceMs: read("AUTO_PURGE_GRACE_MS", DEFAULT_AUTO_CONFIG.purgeGraceMs),
    purgeBatch: read("AUTO_PURGE_BATCH", DEFAULT_AUTO_CONFIG.purgeBatch),
    logKeep: read("AUTO_LOG_KEEP", DEFAULT_AUTO_CONFIG.logKeep),
    maxRequests: read("AUTO_MAX_REQUESTS", DEFAULT_AUTO_CONFIG.maxRequests),
    recomputeBatch: read("AUTO_RECOMPUTE_BATCH", DEFAULT_AUTO_CONFIG.recomputeBatch),
  };
}

/**
 * Eén ronde. Geeft altijd een antwoord terug in plaats van te gooien: dit draait
 * op een cron waar niemand naar kijkt, dus een fout hoort in `ingest_runs` te
 * belanden en niet in een log dat je nooit leest.
 */
export async function runAutoIngest(
  env: ScrapeEnv & Record<string, unknown>,
  config: AutoConfig = DEFAULT_AUTO_CONFIG,
  options: { force?: boolean } = {},
): Promise<AutoResult> {
  const store = new Store(env.DB);
  const now = Date.now();

  if (!options.force) {
    const until = Number((await store.getState(COOLDOWN_UNTIL)) ?? 0);
    if (until > now) {
      await store.log("info", "auto", "ronde overgeslagen: afkoelen na blokkade", {
        tot: new Date(until).toISOString(),
      });
      return { ran: false, reason: "afkoelen na blokkade", cooldownUntil: until };
    }

    const today = await store.runTotalsSince(startOfToday());
    if (today.added + today.repaired >= config.dailyMax) {
      await store.log("info", "auto", `ronde overgeslagen: dagbudget van ${config.dailyMax} bereikt`);
      return { ran: false, reason: `dagbudget van ${config.dailyMax} recepten bereikt` };
    }
  }

  // Opruimen eerst, en elke ronde: het kost geen enkel verzoek aan ah.nl en
  // houdt alles wat daarna komt (herstellen, koppelen, plannen) aan het werk op
  // recepten waar echt iets in zit. Zie `purgeUnusableRecipes` voor wat er weg
  // mag — favorieten en opgeslagen dagen blijven altijd staan.
  const purged = await store.purgeUnusableRecipes(config.purgeGraceMs, config.purgeBatch);
  if (purged > 0) {
    await store.log("info", "auto", `${purged} onbruikbare recepten opgeruimd`, {
      respijtUren: Math.round(config.purgeGraceMs / 3600000),
    });
  }
  // De log begrensd houden, anders groeit hij oneindig door in een database
  // waar verder alles een bovengrens heeft.
  await store.trimLogs(config.logKeep);

  // Opnieuw doorrekenen wat inmiddels gekoppeld is. Puur database, dus dit hoort
  // net als het opruimen vóór het netwerkwerk: het maakt recepten bruikbaar
  // zonder één verzoek aan ah.nl.
  const recomputed = await recomputeNutrition(env, config.recomputeBatch);

  const clientOptions = {
    minIntervalMs: config.minIntervalMs,
    backoffMs: config.backoffMs,
    maxRequests: config.maxRequests,
  };
  // Lege recepten eerst: die kosten één pagina en leveren meteen een bruikbaar
  // recept op, terwijl nieuw zoeken pas na de detailpagina iets oplevert.
  const empty = await store.countWithoutIngredients();

  if (empty > 0) {
    await store.log("info", "auto", `ronde: ${empty} lege recepten aanvullen`);
    const runId = await store.startRun("repair", `${empty} open`);
    const result = await repairEmptyRecipes(env, config.batch, clientOptions);
    await store.finishRun(runId, { repaired: result.repaired, blocked: result.blocked, errors: result.errors });
    const cooldownUntil = await applyCooldown(store, result.blocked, config, now);
    return {
      ran: true,
      mode: "repair",
      purged,
      recomputed,
      detail: `${empty} lege recepten open`,
      repaired: result.repaired,
      blocked: result.blocked,
      ...(cooldownUntil ? { cooldownUntil } : {}),
    };
  }

  // Eén op de `enrichEvery` niet-repareer-rondes gaat naar ingredient-koppelingen
  // in plaats van nieuwe recepten — zie EnrichConfig hierboven voor waarom dit
  // een vast aandeel is en geen vaste, nooit-bereikte laatste prioriteit.
  const enrichTurn = Number((await store.getState(ENRICH_TURN)) ?? 0);
  await store.setState(ENRICH_TURN, String((enrichTurn + 1) % config.enrichEvery));

  if (enrichTurn === 0) {
    const openNames = await store.countIngredientNamesWithoutMatch();
    if (openNames > 0) {
      await store.log("info", "auto", `ronde: ${openNames} ingredient-koppelingen open`);
      const runId = await store.startRun("enrich", `${openNames} koppelingen open`);
      const result = await enrichIngredientMatches(env, config.enrichBatch, clientOptions);
      await store.finishRun(runId, { repaired: result.matched, blocked: result.blocked, errors: result.errors });
      const cooldownUntil = await applyCooldown(store, result.blocked, config, now);
      return {
        ran: true,
        mode: "enrich",
        purged,
        recomputed,
        detail: `${openNames} koppelingen open`,
        enriched: result.matched,
        blocked: result.blocked,
        ...(cooldownUntil ? { cooldownUntil } : {}),
      };
    }
  }

  // Rouleren over de eetmomenten, en binnen een moment over de zoektermen, zodat
  // de database over de dag gelijkmatig alle hoeken raakt in plaats van vijftig
  // pastarecepten achter elkaar.
  const cursor = Number((await store.getState(CURSOR)) ?? 0) || 0;
  const moment = MOMENTS[cursor % MOMENTS.length]!;
  const queries = MOMENT_QUERIES[moment]!;
  const query = queries[Math.floor(cursor / MOMENTS.length) % queries.length]!;
  await store.setState(CURSOR, String((cursor + 1) % (MOMENTS.length * queries.length)));

  await store.log("info", "auto", `ronde: nieuwe recepten voor ${moment}`, { query });
  const runId = await store.startRun("moment", `${moment}: ${query}`);
  const result = await ingestQueries(env, [query], config.batch, moment, clientOptions);
  await store.finishRun(runId, { added: result.added, blocked: result.blocked, errors: result.errors });
  const cooldownUntil = await applyCooldown(store, result.blocked, config, now);

  return {
    ran: true,
    mode: "moment",
    purged,
    recomputed,
    detail: `${moment}: ${query}`,
    added: result.added,
    blocked: result.blocked,
    ...(cooldownUntil ? { cooldownUntil } : {}),
  };
}

/**
 * Na blokkades even niets doen. Doorgaan alsof er niets aan de hand is maakt het
 * alleen erger: Akamai kijkt naar tempo, dus stilte is de enige manier terug.
 *
 * Blokkeert AH ons meteen weer zodra de afkoelperiode voorbij is, dan duurde de
 * blokkade zelf langer dan die periode — nog eens dezelfde `cooldownMs` wachten
 * betekent dan alleen maar opnieuw tegen dezelfde muur lopen. Elke opeenvolgende
 * blokkade verdubbelt daarom de afkoelperiode, tot aan `maxCooldownMs`. Een
 * schone ronde zet de teller weer op nul.
 */
async function applyCooldown(
  store: Store,
  blocked: number,
  config: AutoConfig,
  now: number,
): Promise<number | null> {
  if (blocked === 0) {
    await store.setState(BLOCK_STREAK, "0");
    return null;
  }
  const streak = Number((await store.getState(BLOCK_STREAK)) ?? 0) + 1;
  await store.setState(BLOCK_STREAK, String(streak));
  const cooldownMs = Math.min(config.cooldownMs * 2 ** (streak - 1), config.maxCooldownMs);
  const until = now + cooldownMs;
  await store.setState(COOLDOWN_UNTIL, String(until));
  await store.log("warn", "auto", `AH blokkeerde ${blocked}x; ${Math.round(cooldownMs / 60000)} min afkoelen`, {
    blokkadesOpEenRij: streak,
    tot: new Date(until).toISOString(),
  });
  return until;
}

/** Stand van zaken voor de UI: loopt het nog, en hoe hard. */
export async function autoStatus(env: ScrapeEnv, config: AutoConfig = DEFAULT_AUTO_CONFIG) {
  const store = new Store(env.DB);
  const cooldownUntil = Number((await store.getState(COOLDOWN_UNTIL)) ?? 0);
  const today = await store.runTotalsSince(startOfToday());

  return {
    vandaag: today,
    dagbudget: config.dailyMax,
    openLegeRecepten: await store.countWithoutIngredients(),
    onbruikbareRecepten: await store.countUnusableRecipes(config.purgeGraceMs),
    openKoppelingen: await store.countIngredientNamesWithoutMatch(),
    afkoelenTot: cooldownUntil > Date.now() ? cooldownUntil : null,
    blokkadesOpEenRij: Number((await store.getState(BLOCK_STREAK)) ?? 0),
    volgende: MOMENTS[Number((await store.getState(CURSOR)) ?? 0) % MOMENTS.length],
    rondes: await store.recentRuns(10),
  };
}
