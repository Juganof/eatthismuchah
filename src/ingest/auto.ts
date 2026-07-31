import { Store } from "../db/queries";
import {
  MOMENTS,
  MOMENT_QUERIES,
  ingestQueries,
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

export interface AutoConfig {
  /** Recepten per ronde. Klein houden: een ronde moet binnen de worker passen. */
  batch: number;
  /** Bovengrens per dag, zodat de automaat niet eindeloos doorgaat. */
  dailyMax: number;
  /** Rust tussen twee verzoeken binnen een ronde. */
  minIntervalMs: number;
  /** Hoe lang we niets doen nadat AH ons geblokkeerd heeft. */
  cooldownMs: number;
  /** Wachttijd voor de eerste herkansing binnen een ronde; verdubbelt daarna. */
  backoffMs: number;
}

export const DEFAULT_AUTO_CONFIG: AutoConfig = {
  batch: 8,
  dailyMax: 250,
  minIntervalMs: 700,
  cooldownMs: 30 * 60 * 1000,
  backoffMs: 1500,
};

export interface AutoResult {
  ran: boolean;
  /** Waarom er niets gebeurde, als er niets gebeurde. */
  reason?: string;
  mode?: "repair" | "moment";
  detail?: string;
  added?: number;
  repaired?: number;
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
    backoffMs: read("AUTO_BACKOFF_MS", DEFAULT_AUTO_CONFIG.backoffMs),
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
      return { ran: false, reason: "afkoelen na blokkade", cooldownUntil: until };
    }

    const today = await store.runTotalsSince(startOfToday());
    if (today.added + today.repaired >= config.dailyMax) {
      return { ran: false, reason: `dagbudget van ${config.dailyMax} recepten bereikt` };
    }
  }

  const clientOptions = { minIntervalMs: config.minIntervalMs, backoffMs: config.backoffMs };
  // Lege recepten eerst: die kosten één pagina en leveren meteen een bruikbaar
  // recept op, terwijl nieuw zoeken pas na de detailpagina iets oplevert.
  const empty = await store.countWithoutIngredients();

  if (empty > 0) {
    const runId = await store.startRun("repair", `${empty} open`);
    const result = await repairEmptyRecipes(env, config.batch, clientOptions);
    await store.finishRun(runId, { repaired: result.repaired, blocked: result.blocked, errors: result.errors });
    const cooldownUntil = await applyCooldown(store, result.blocked, config, now);
    return {
      ran: true,
      mode: "repair",
      detail: `${empty} lege recepten open`,
      repaired: result.repaired,
      blocked: result.blocked,
      ...(cooldownUntil ? { cooldownUntil } : {}),
    };
  }

  // Rouleren over de eetmomenten, en binnen een moment over de zoektermen, zodat
  // de database over de dag gelijkmatig alle hoeken raakt in plaats van vijftig
  // pastarecepten achter elkaar.
  const cursor = Number((await store.getState(CURSOR)) ?? 0) || 0;
  const moment = MOMENTS[cursor % MOMENTS.length]!;
  const queries = MOMENT_QUERIES[moment]!;
  const query = queries[Math.floor(cursor / MOMENTS.length) % queries.length]!;
  await store.setState(CURSOR, String((cursor + 1) % (MOMENTS.length * queries.length)));

  const runId = await store.startRun("moment", `${moment}: ${query}`);
  const result = await ingestQueries(env, [query], config.batch, moment, clientOptions);
  await store.finishRun(runId, { added: result.added, blocked: result.blocked, errors: result.errors });
  const cooldownUntil = await applyCooldown(store, result.blocked, config, now);

  return {
    ran: true,
    mode: "moment",
    detail: `${moment}: ${query}`,
    added: result.added,
    blocked: result.blocked,
    ...(cooldownUntil ? { cooldownUntil } : {}),
  };
}

/**
 * Na blokkades even niets doen. Doorgaan alsof er niets aan de hand is maakt het
 * alleen erger: Akamai kijkt naar tempo, dus stilte is de enige manier terug.
 */
async function applyCooldown(
  store: Store,
  blocked: number,
  config: AutoConfig,
  now: number,
): Promise<number | null> {
  if (blocked === 0) return null;
  const until = now + config.cooldownMs;
  await store.setState(COOLDOWN_UNTIL, String(until));
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
    afkoelenTot: cooldownUntil > Date.now() ? cooldownUntil : null,
    volgende: MOMENTS[Number((await store.getState(CURSOR)) ?? 0) % MOMENTS.length],
    rondes: await store.recentRuns(10),
  };
}
