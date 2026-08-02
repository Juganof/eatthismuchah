import { Store } from "../db/queries";
import { MOMENTS, MOMENT_QUERIES, ingestComplete, type ScrapeEnv } from "./pipeline";

/**
 * Vult de database de hele dag door zichzelf aan, in kleine porties.
 *
 * De opzet volgt uit hoe ah.nl zich gedraagt. Akamai reageert op tempo, niet op
 * aantallen, dus één grote nachtelijke ingest is precies de verkeerde vorm: die
 * loopt gegarandeerd tegen 403's aan. Veel kleine rondes met rust ertussen komen
 * er wél door, en leveren over een dag genomen veel meer recepten op.
 *
 * Elke ronde doet hetzelfde: zoeken op de term die aan de beurt is, en elk
 * gevonden recept opslaan met de voedingswaarde die AH er zelf bij zet. Dat kost
 * één verzoek per recept, dus er passen er tientallen in een ronde. Een recept
 * dat AH niet doorgerekend heeft wordt afgekeurd en nooit meer opgehaald; er
 * zijn dus geen halffabricaten om later te repareren.
 *
 * Verder houdt hij zich in: een dagbudget zodat we niet eindeloos blijven
 * hameren, en een afkoelperiode zodra AH ons alsnog blokkeert.
 */

/** Sleutels in app_state. */
const CURSOR = "auto:cursor";
const COOLDOWN_UNTIL = "auto:cooldown_until";
const BLOCK_STREAK = "auto:block_streak";
const PAUSED = "auto:paused";

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
  /** Hoeveel logregels er bewaard blijven; oudere gaan elke ronde weg. */
  logKeep: number;
  /**
   * Bovengrens op het aantal verzoeken aan ah.nl per ronde. Een worker mag er
   * maar een beperkt aantal doen (50 op het gratis plan); ging dat op, dan kapte
   * Cloudflare de ronde middenin af en faalde elk resterend recept met "Too many
   * subrequests". Onder de grens blijven is de enige manier om dat te vermijden.
   */
  maxRequests: number;
  /**
   * Vanaf hoeveel opeenvolgende rondes mét blokkade we ons gedeisd houden. Eén
   * enkele 403 zegt niets over het tempo — dat is vaak gewoon een recept dat
   * niet meer bestaat — en daar de hele automaat een uur voor stilleggen kost
   * veel meer dan het oplevert. Twee rondes op rij is een ander verhaal.
   */
  cooldownAfterBlocks: number;
  /**
   * Klok voor het tijdstip van deze ronde. Alleen tests sturen hem: met een
   * vaste klok is "afkoelen tot nu + cooldownMs" exact te controleren, zonder
   * dat de duur van de ronde zelf de meting vertekent.
   */
  now?: () => number;
}

export const DEFAULT_AUTO_CONFIG: AutoConfig = {
  // Eén verzoek per recept, dus dit past ruim binnen het budget van veertig:
  // de zoekpagina plus dertig receptpagina's. Toen er per ingredient nog een
  // product bij gezocht werd was acht al te veel gevraagd en kwamen er in de
  // praktijk twee of drie doorheen.
  batch: 30,
  dailyMax: 250,
  minIntervalMs: 700,
  cooldownMs: 30 * 60 * 1000,
  maxCooldownMs: 4 * 60 * 60 * 1000,
  // Ruim: uit de log bleek een blokkade tientallen seconden te duren, dus
  // herkansen na 1,5 en 3 seconden liep gegarandeerd tegen dezelfde muur. Op de
  // cron mag dat wachten — daar kijkt niemand naar, en het scheelt een ronde.
  backoffMs: 8000,
  logKeep: 2000,
  maxRequests: 40,
  cooldownAfterBlocks: 2,
};

export interface AutoResult {
  ran: boolean;
  /** Waarom er niets gebeurde, als er niets gebeurde. */
  reason?: string;
  /** Welke zoekterm deze ronde aan de beurt was. */
  detail?: string;
  /** Complete recepten die zijn opgeslagen. */
  added?: number;
  /** Recepten die niet compleet te krijgen waren; die komen nooit meer terug. */
  rejected?: number;
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
    logKeep: read("AUTO_LOG_KEEP", DEFAULT_AUTO_CONFIG.logKeep),
    maxRequests: read("AUTO_MAX_REQUESTS", DEFAULT_AUTO_CONFIG.maxRequests),
    cooldownAfterBlocks: read("AUTO_COOLDOWN_AFTER_BLOCKS", DEFAULT_AUTO_CONFIG.cooldownAfterBlocks),
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
  const now = config.now?.() ?? Date.now();

  if (!options.force) {
    // Uitgezet betekent uit. Zonder deze schakelaar is de database niet leeg te
    // houden: de cron draait elke twee minuten, dus alles wat je wist staat er
    // een paar tellen later weer in.
    if ((await store.getState(PAUSED)) === "1") {
      return { ran: false, reason: "automatisch bijvullen staat uit" };
    }

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

  // De log begrensd houden, anders groeit hij oneindig door in een database
  // waar verder alles een bovengrens heeft.
  await store.trimLogs(config.logKeep);
  // Eenmalig: alles wat nog uit de tijd van halffabricaten stamt eruit. Zie
  // `dropIncompleteRecipes`.
  await cleanupOnce(store);

  // Rouleren over de eetmomenten, en binnen een moment over de zoektermen, zodat
  // de database over de dag gelijkmatig alle hoeken raakt in plaats van vijftig
  // pastarecepten achter elkaar.
  const cursor = Number((await store.getState(CURSOR)) ?? 0) || 0;
  const moment = MOMENTS[cursor % MOMENTS.length]!;
  const queries = MOMENT_QUERIES[moment]!;
  const query = queries[Math.floor(cursor / MOMENTS.length) % queries.length]!;
  await store.setState(CURSOR, String((cursor + 1) % (MOMENTS.length * queries.length)));

  await store.log("info", "auto", `ronde: ${query}`);
  const runId = await store.startRun("scrape", `${moment}: ${query}`);
  const result = await ingestComplete(env, [query], config.batch, {
    minIntervalMs: config.minIntervalMs,
    backoffMs: config.backoffMs,
    maxRequests: config.maxRequests,
  });
  await store.finishRun(runId, {
    added: result.added,
    repaired: result.rejected,
    blocked: result.blocked,
    errors: result.errors,
  });
  const cooldownUntil = await applyCooldown(store, result.blocked, config, now);

  return {
    ran: true,
    detail: `${moment}: ${query}`,
    added: result.added,
    rejected: result.rejected,
    blocked: result.blocked,
    ...(cooldownUntil ? { cooldownUntil } : {}),
  };
}

/** app_state-sleutel voor de eenmalige opruiming; zie `cleanupOnce`. */
const CLEANUP_DONE = "cleanup:complete_only";

/**
 * Ruimt één keer op wat uit de vorige opzet is blijven staan: recepten zonder
 * ingredienten of zonder doorgerekende voedingswaarde. Vanaf nu kan zoiets niet
 * meer ontstaan — een recept gaat compleet de database in of helemaal niet — dus
 * dit hoeft precies één keer te gebeuren en daarna nooit meer.
 */
async function cleanupOnce(store: Store): Promise<void> {
  if ((await store.getState(CLEANUP_DONE)) === "1") return;
  const removed = await store.dropIncompleteRecipes();
  await store.setState(CLEANUP_DONE, "1");
  if (removed > 0) {
    await store.log("info", "auto", `${removed} onvolledige recepten uit de oude opzet verwijderd`);
  }
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
  // De teller loopt over rondes, niet over verzoeken binnen één ronde. Eén
  // geblokkeerde pagina is ruis — dat kostte ons anderhalf uur stilstand voor
  // één kapot recept — maar twee rondes op rij die tegen een muur lopen is wél
  // een tempo-blokkade, en dan is stil zijn de enige weg terug.
  const streak = Number((await store.getState(BLOCK_STREAK)) ?? 0) + 1;
  await store.setState(BLOCK_STREAK, String(streak));
  if (streak < config.cooldownAfterBlocks) {
    await store.log("info", "auto", `${blocked}x geblokkeerd; nog geen reden om af te koelen`, {
      rondesOpEenRij: streak,
    });
    return null;
  }
  const cooldownMs = Math.min(
    config.cooldownMs * 2 ** (streak - config.cooldownAfterBlocks),
    config.maxCooldownMs,
  );
  const until = now + cooldownMs;
  await store.setState(COOLDOWN_UNTIL, String(until));
  await store.log("warn", "auto", `AH blokkeerde ${blocked}x; ${Math.round(cooldownMs / 60000)} min afkoelen`, {
    blokkadesOpEenRij: streak,
    tot: new Date(until).toISOString(),
  });
  return until;
}

/** Zet het automatisch bijvullen aan of uit. Blijft staan tot je hem omzet. */
export async function setAutoPaused(env: ScrapeEnv, paused: boolean): Promise<void> {
  const store = new Store(env.DB);
  await store.setState(PAUSED, paused ? "1" : "0");
  await store.log("info", "auto", `automatisch bijvullen ${paused ? "uitgezet" : "aangezet"}`);
}

/** Stand van zaken voor de UI: loopt het nog, en hoe hard. */
export async function autoStatus(env: ScrapeEnv, config: AutoConfig = DEFAULT_AUTO_CONFIG) {
  const store = new Store(env.DB);
  const cooldownUntil = Number((await store.getState(COOLDOWN_UNTIL)) ?? 0);
  const today = await store.runTotalsSince(startOfToday());

  return {
    vandaag: today,
    dagbudget: config.dailyMax,
    recepten: await store.countRecipes(),
    afgekeurd: await store.countSkippedRecipes(),
    gepauzeerd: (await store.getState(PAUSED)) === "1",
    afkoelenTot: cooldownUntil > Date.now() ? cooldownUntil : null,
    blokkadesOpEenRij: Number((await store.getState(BLOCK_STREAK)) ?? 0),
    volgende: MOMENTS[Number((await store.getState(CURSOR)) ?? 0) % MOMENTS.length],
    rondes: await store.recentRuns(10),
  };
}
