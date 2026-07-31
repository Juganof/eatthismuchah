import { Hono } from "hono";
import { AhClient, collectRecipes, type RawScrape } from "./ah/client";
import { extractEmbeddedJson } from "./ah/scrape";

import { Store, type SavedDayMeal } from "./db/queries";
import { coverageOf, resolveRecipe } from "./nutrition/resolve";
import { autoStatus, configFrom, runAutoIngest, setAutoPaused } from "./ingest/auto";
import {
  MOMENT_QUERIES,
  computeNutrition,
  enrichIngredientMatches,
  ingestQueries,
  repairEmptyRecipes,
  scrapeClient,
} from "./ingest/pipeline";
import { splitTargets, type MealSlot } from "./nutrition/split";
import { dailyTargets, sanitiseProfile, bmr, tdee, type DailyTargets } from "./nutrition/targets";
import { blankDay, generateDay, rerollSlotOptions, type DayPlan } from "./optimize/day";
import { buildTargets, planRecipe, rankPlans, type Plan } from "./optimize/plan";
import { buildShoppingList, type ShoppingInput } from "./plan/shopping";
import { renderPage } from "./ui/page";

export interface Env {
  DB: D1Database;
  AH_USER_AGENT: string;
  INGEST_QUERIES: string;
  INGEST_LIMIT: string;
  /** Instellingen voor het automatisch bijvullen; zie src/ingest/auto.ts. */
  AUTO_BATCH?: string;
  AUTO_DAILY_MAX?: string;
  AUTO_MIN_INTERVAL_MS?: string;
  AUTO_COOLDOWN_MS?: string;
  AUTO_MAX_COOLDOWN_MS?: string;
  AUTO_ENRICH_BATCH?: string;
  AUTO_ENRICH_EVERY?: string;
  /** Index signature zodat configFrom de bovenstaande vars kan uitlezen. */
  [key: string]: unknown;
}

const app = new Hono<{ Bindings: Env }>();

/**
 * Elke client archiveert zijn scrapes. Dat gebeurt buiten de request-afhandeling
 * om (`waitUntil` waar beschikbaar), zodat het antwoord er niet op hoeft te wachten
 * maar de payload wel bewaard blijft.
 */
/** Alleen `waitUntil` is nodig, en Hono's context type wijkt af van workers-types. */
type Waiter = { waitUntil(promise: Promise<unknown>): void } | undefined;

function clientFor(env: Env, ctx?: Waiter): AhClient {
  const store = new Store(env.DB);
  return new AhClient(env.AH_USER_AGENT, (raw: RawScrape) => {
    const write = store.putRaw(raw);
    if (ctx?.waitUntil) ctx.waitUntil(write);
  });
}

const storeFor = (env: Env) => new Store(env.DB);

/**
 * Zonder dit antwoordt Hono op een onverwachte fout met platte tekst, en dan
 * toont de UI alleen "geen geldige respons" — precies als je wilt weten wat er
 * mis is. Een ontbrekende kolom na een overgeslagen migratie is zo'n geval, dus
 * de melding wijst daar ook naar.
 */
app.onError((err, c) => {
  const message = err instanceof Error ? err.message : String(err);
  const hint = /no such column|no such table|D1_ERROR/i.test(message)
    ? " — draai de migratie: npm run db:migrate"
    : "";
  console.error("unhandled:", message);
  // Ook de log in, zodat een fout die de gebruiker als toast voorbij ziet komen
  // achteraf nog terug te vinden en op te sturen is.
  void storeFor(c.env).log("error", "api", message, { pad: c.req.path });
  return c.json({ error: message + hint }, 500);
});

/**
 * De applicatielog: wat de app deed, in leesbare regels. `?format=text` geeft
 * platte tekst terug — dat is wat de kopieerknop in de UI ophaalt, zodat je het
 * ongewijzigd kunt doorsturen.
 */
app.get("/api/logs", async (c) => {
  const store = storeFor(c.env);
  const limit = Math.min(Number(c.req.query("limit") ?? 200) || 200, 1000);
  const levelParam = c.req.query("level");
  const level = levelParam === "warn" || levelParam === "error" || levelParam === "info"
    ? levelParam
    : undefined;
  const rows = await store.recentLogs(limit, level);

  if (c.req.query("format") === "text") {
    const header = `# app-log, ${rows.length} regels, nieuwste eerst, ${new Date().toISOString()}`;
    const body = rows
      .map((r) => {
        const at = new Date(r.at).toISOString().replace("T", " ").slice(0, 19);
        return `${at} ${r.level.toUpperCase().padEnd(5)} ${r.source.padEnd(7)} ${r.message}` +
          (r.detail ? ` ${r.detail}` : "");
      })
      .join("\n");
    return c.text(`${header}\n${body}\n`);
  }

  return c.json({ rows, total: await store.countLogs() });
});

app.post("/api/logs/clear", async (c) => {
  await storeFor(c.env).clearLogs();
  return c.json({ ok: true });
});

/**
 * Alles weggooien. `scope: "scrape"` (standaard) laat het profiel, de
 * eetmomenten en de opgeslagen dagen staan — dat is jouw data en die is nergens
 * anders vandaan te halen; `scope: "alles"` gooit ook die weg.
 */
app.post("/api/wipe", async (c) => {
  const body = await c.req
    .json<{ scope?: string; pause?: boolean }>()
    .catch(() => ({}) as { scope?: string; pause?: boolean });
  const scope = body.scope === "alles" ? "alles" : "scrape";
  const store = storeFor(c.env);
  const deleted = await store.wipe(scope);
  // Wissen én het automatisch bijvullen laten doorlopen is zinloos: de cron
  // draait elke twee minuten, dus binnen een paar tellen staat de database weer
  // vol. Standaard gaat de automaat daarom uit; `pause: false` laat hem staan.
  const paused = body.pause !== false;
  if (paused) await setAutoPaused(c.env, true);
  const total = Object.values(deleted).reduce((sum, n) => sum + n, 0);
  // Ná het wissen loggen: de log is zelf net leeggegooid, dus dit is meteen de
  // eerste regel van de nieuwe geschiedenis.
  await store.log("warn", "api", `database gewist (${scope}): ${total} rijen`, deleted);
  return c.json({ scope, deleted, total, paused });
});

app.get("/", (c) => c.html(renderPage()));

/** Reports which ah.nl endpoints still work. First stop when results go empty. */
app.get("/api/probe", async (c) => c.json(await clientFor(c.env, c.executionCtx).probe()));

app.get("/api/stats", async (c) => {
  const store = storeFor(c.env);
  const raw = await store.countRaw();
  return c.json({
    recipes: await store.countRecipes(),
    plannable: await store.countPlannable(),
    zonderIngredienten: await store.countWithoutIngredients(),
    onbruikbaar: await store.countUnusableRecipes(configFrom(c.env).purgeGraceMs),
    scrapes: raw.total,
    unparsed: raw.unparsed,
  });
});

/**
 * Live Allerhande search. Anders dan vroeger bewaart dit alles wat het tegenkomt:
 * de ruwe payload gaat het archief in en elk gevonden recept wordt op de
 * achtergrond volledig opgehaald en doorgerekend, zodat het meteen bruikbaar is
 * voor de dagplanner in plaats van pas na een aparte ingest.
 */
app.get("/api/search", async (c) => {
  const q = c.req.query("q");
  if (!q) return c.json({ error: "missing ?q" }, 400);

  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);
  const recipes = await client.searchRecipes(q, Number(c.req.query("size") ?? 20));

  // Per stuk afvangen: één recept dat niet wegschrijft mag de zoekopdracht niet
  // laten mislukken, want het resultaat is verder gewoon bruikbaar.
  const saveErrors: string[] = [];
  for (const stub of recipes) {
    try {
      await store.putRecipe(stub);
    } catch (err) {
      saveErrors.push(`${stub.id}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  // Het doorrekenen doet een productzoekopdracht per nieuw ingredient, dus dat
  // mag de zoekopdracht niet ophouden.
  c.executionCtx?.waitUntil(hydrate(c.env, recipes.map((r) => r.id)));

  return c.json({
    recipes: recipes.map(({ id, title, url, imageUrl }) => ({ id, title, url, imageUrl })),
    saved: recipes.length - saveErrors.length,
    ...(saveErrors.length > 0 ? { errors: saveErrors } : {}),
  });
});

/**
 * Scrapes recipes for the given queries and works out their nutrition, filling the
 * cache that /api/generate searches. This is the slow path — it does a product
 * lookup per new ingredient — so it runs in batches and nightly via cron.
 */
app.post("/api/ingest", async (c) => {
  type IngestBody = { queries?: string[]; limit?: number; moment?: string };
  // An empty body is the normal case: the UI's "fetch new recipes" button posts "{}".
  const body: IngestBody = await c.req.json<IngestBody>().catch(() => ({}));
  const limit = body.limit ?? Number(c.env.INGEST_LIMIT || 20);

  // Scrapen voor één eetmoment: zoek op AH's eigen termen voor dat moment en
  // houd daarna alleen over wat AH zélf zo gelabeld heeft.
  if (body.moment) {
    const queries = MOMENT_QUERIES[body.moment];
    if (!queries) {
      return c.json(
        { error: `onbekend eetmoment "${body.moment}"; kies uit ${Object.keys(MOMENT_QUERIES).join(", ")}` },
        400,
      );
    }
    return c.json({ moment: body.moment, ...(await ingestQueries(c.env, queries, limit, body.moment)) });
  }

  const queries = body.queries ?? c.env.INGEST_QUERIES.split(",").map((s) => s.trim());
  return c.json(await ingestQueries(c.env, queries, limit));
});

/**
 * Bouwt recepten opnieuw op uit het ruwe archief, met de parser van nu. Dit is
 * waarvoor scrape_raw bestaat: gaat AH zijn pagina's om en repareren we de parser,
 * dan hoeft er geen enkele scrape opnieuw.
 */
app.post("/api/reparse", async (c) => {
  const body = await c.req.json<{ limit?: number }>().catch(() => ({}) as { limit?: number });
  const store = storeFor(c.env);
  const rows = await store.latestRawPerRef("recipe", body.limit ?? 200);

  let recovered = 0;
  const failed: string[] = [];
  for (const row of rows) {
    try {
      const found = collectRecipes(extractEmbeddedJson(row.body));
      const recipe = found.find((r) => r.ingredients.length > 0);
      if (!recipe) {
        await store.markRawParsed(row.id, false, "geen recept met ingredienten gevonden");
        failed.push(row.ref);
        continue;
      }
      await store.putRecipe(recipe);
      await store.markRawParsed(row.id, true, null);
      recovered++;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await store.markRawParsed(row.id, false, message);
      failed.push(row.ref);
    }
  }

  // Voedingswaarde bijwerken gebeurt op de achtergrond: dat doet netwerk.
  c.executionCtx?.waitUntil(hydrate(c.env, rows.map((r) => r.ref)));
  return c.json({ examined: rows.length, recovered, failed: failed.slice(0, 20) });
});

/**
 * Haalt recepten opnieuw op die geen ingredienten hebben. Dat zijn er meestal
 * een hoop: een zoekopdracht levert alleen titels, en als AH's botbescherming
 * daarna de detailpagina's blokkeert, blijven ze leeg staan. Dit is de knop die
 * dat rechttrekt — rustig aan, want haast is precies waarom het misging.
 */
app.post("/api/repair", async (c) => {
  const body = await c.req.json<{ limit?: number }>().catch(() => ({}) as { limit?: number });
  const store = storeFor(c.env);
  const result = await repairEmptyRecipes(c.env, body.limit ?? 25);
  if (result.examined === 0) return c.json({ ...result, remaining: 0, message: "niets te herstellen" });

  return c.json({
    ...result,
    remaining: await store.countWithoutIngredients(),
    errors: result.errors.slice(0, 10),
  });
});

/**
 * Vult ingredient -> product koppelingen aan voor recepten die AH's eigen
 * voedingswaarde gebruikten en daardoor nooit een productzoekopdracht deden —
 * zie enrichIngredientMatches in src/ingest/pipeline.ts.
 */
app.post("/api/enrich", async (c) => {
  const body = await c.req.json<{ limit?: number }>().catch(() => ({}) as { limit?: number });
  const store = storeFor(c.env);
  const result = await enrichIngredientMatches(c.env, body.limit ?? 15);
  if (result.examined === 0) {
    return c.json({ ...result, remaining: 0, message: "alle ingredienten zijn al opgezocht" });
  }

  return c.json({
    ...result,
    remaining: await store.countIngredientNamesWithoutMatch(),
    errors: result.errors.slice(0, 10),
  });
});

/**
 * Meteen opruimen in plaats van wachten op de volgende automatische ronde. Weg
 * gaat alles waar de planner niets mee kan: geen ingredienten, geen
 * voedingswaarde, of geen enkel ingredient met echte productcijfers erachter.
 */
app.post("/api/purge", async (c) => {
  const body = await c.req.json<{ graceMs?: number }>().catch(() => ({}) as { graceMs?: number });
  const store = storeFor(c.env);
  const config = configFrom(c.env);
  // graceMs 0 mag: dat is "ruim nu alles op wat nu onbruikbaar is".
  const graceMs = Number.isFinite(body.graceMs) ? Number(body.graceMs) : config.purgeGraceMs;
  const purged = await store.purgeUnusableRecipes(graceMs, config.purgeBatch);
  return c.json({ purged, remaining: await store.countUnusableRecipes(graceMs) });
});

/**
 * De stand van zaken van het automatisch bijvullen: wat er vandaag binnenkwam,
 * of AH ons afknijpt, en wat er nog open staat.
 */
app.get("/api/auto/status", async (c) =>
  c.json(await autoStatus(c.env, configFrom(c.env))),
);

/** Automatisch bijvullen aan- of uitzetten. */
app.post("/api/auto/pause", async (c) => {
  const body = await c.req.json<{ paused?: boolean }>().catch(() => ({}) as { paused?: boolean });
  const paused = body.paused !== false;
  await setAutoPaused(c.env, paused);
  return c.json({ paused });
});

/** Eén ronde nu draaien, in plaats van wachten op de cron. */
app.post("/api/auto/run", async (c) => {
  const body = await c.req.json<{ force?: boolean }>().catch(() => ({}) as { force?: boolean });
  // `force` slaat de afkoelperiode en het dagbudget over; handig om te testen,
  // maar niet iets om te herhalen als AH net aan het blokkeren was.
  return c.json(await runAutoIngest(c.env, configFrom(c.env), { force: body.force === true }));
});

/** Nutrition and ingredient breakdown for one recipe, without any rescaling. */
app.get("/api/recipe/:id", async (c) => {
  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);
  const id = c.req.param("id");

  let recipe = await store.getRecipe(id);
  if (!recipe) {
    recipe = await client.getRecipe(id);
    if (!recipe) return c.json({ error: "recipe not found" }, 404);
    await store.putRecipe(recipe);
  }

  const resolved = await resolveRecipe(recipe, client, store);
  // Ook hier de voedingswaarde bewaren: zonder deze rij blijft het recept
  // onzichtbaar voor de dagplanner, hoe vaak je hem ook opent.
  await store.putNutrition(recipe.id, resolved.total, coverageOf(resolved));

  return c.json({
    recipe: resolved.recipe,
    total: resolved.total,
    coverage: coverageOf(resolved),
    ingredients: resolved.ingredients.map((i) => ({
      name: i.raw.name,
      quantity: i.raw.quantity,
      unit: i.raw.unit,
      grams: Math.round(i.grams * 10) / 10,
      gramsSource: i.gramsSource,
      product: i.product?.title ?? null,
      matchScore: Math.round(i.matchScore * 100) / 100,
      nutrients: i.nutrients,
    })),
  });
});

/** Rescales one specific recipe to hit the given macro targets. */
app.post("/api/plan", async (c) => {
  const body = await c.req.json<PlanRequest & { recipeId: string }>();
  if (!body.recipeId) return c.json({ error: "recipeId is required" }, 400);

  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);

  let recipe = await store.getRecipe(body.recipeId);
  if (!recipe) {
    recipe = await client.getRecipe(body.recipeId);
    if (!recipe) return c.json({ error: "recipe not found" }, 404);
    await store.putRecipe(recipe);
  }

  const resolved = await resolveRecipe(recipe, client, store);
  await store.putNutrition(recipe.id, resolved.total, coverageOf(resolved));

  const plan = planRecipe(resolved, buildTargets(body), {
    portions: body.portions ?? 1,
    locked: body.locked,
    minScale: body.minScale,
    maxScale: body.maxScale,
  });
  return c.json(plan);
});

/**
 * The main entry point: given macro targets, picks the cached recipes that can
 * best be bent to fit and returns each one already rescaled.
 */
app.post("/api/generate", async (c) => {
  const body = await c.req.json<PlanRequest & { candidates?: number; results?: number }>();
  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);

  const shortlist = await store.shortlist(body.candidates ?? 25);
  if (shortlist.length === 0) {
    return c.json(
      { error: "no recipes cached yet — run POST /api/ingest first", plans: [] },
      409,
    );
  }

  const targets = buildTargets(body);
  const plans: Plan[] = [];
  for (const summary of shortlist) {
    const recipe = await store.getRecipe(summary.id);
    if (!recipe) continue;
    // Nooit het netwerk op tijdens plannen: dit loopt over tientallen recepten
    // en elk ongematcht ingredient zou een AH-zoekopdracht kosten.
    const resolved = await resolveRecipe(recipe, client, store, { cacheOnly: true });
    plans.push(
      planRecipe(resolved, targets, {
        portions: body.portions ?? 1,
        locked: body.locked,
        minScale: body.minScale,
        maxScale: body.maxScale,
      }),
    );
  }

  return c.json({ plans: rankPlans(plans).slice(0, body.results ?? 5) });
});

/**
 * Live productzoekopdracht voor het handmatig corrigeren van een koppeling —
 * een bewuste, incidentele actie van de gebruiker, dus geen bezwaar tegen het
 * netwerk op, in tegenstelling tot het plannen zelf.
 */
app.get("/api/products/search", async (c) => {
  const q = c.req.query("q");
  if (!q) return c.json({ error: "missing ?q" }, 400);
  const products = await clientFor(c.env, c.executionCtx).searchProducts(q, Number(c.req.query("size") ?? 8));
  return c.json({ products });
});

/** Manual correction when the automatic ingredient→product match is wrong. */
app.post("/api/match", async (c) => {
  const body = await c.req.json<{ ingredient: string; webshopId: string }>();
  if (!body.ingredient || !body.webshopId) {
    return c.json({ error: "ingredient and webshopId are required" }, 400);
  }
  const store = storeFor(c.env);
  const product = await clientFor(c.env, c.executionCtx).getProduct(body.webshopId);
  if (!product) return c.json({ error: "product not found" }, 404);
  await store.putProduct(product);
  await store.overrideMatch(body.ingredient.toLowerCase(), product.webshopId);
  return c.json({ ok: true, product });
});

// ------------------------------------------------------------------- profiel

app.get("/api/profile", async (c) => {
  const profile = await storeFor(c.env).getProfile();
  return c.json(withDerived(profile));
});

app.put("/api/profile", async (c) => {
  const body = await c.req.json().catch(() => ({}) as Record<string, unknown>);
  const profile = sanitiseProfile(body);
  await storeFor(c.env).putProfile(profile);
  return c.json(withDerived(profile));
});

/** Profiel plus wat eruit volgt, zodat de UI niets hoeft na te rekenen. */
function withDerived(profile: ReturnType<typeof sanitiseProfile>) {
  const targets = dailyTargets(profile);
  return {
    profile,
    bmr: bmr(profile) === null ? null : Math.round(bmr(profile)!),
    tdee: tdee(profile) === null ? null : Math.round(tdee(profile)!),
    targets,
    // Zonder gewicht, lengte, leeftijd en geslacht valt er niets te berekenen.
    complete: targets !== null,
  };
}

// --------------------------------------------------------------- eetmomenten

app.get("/api/slots", async (c) => {
  const store = storeFor(c.env);
  const slots = await store.getSlots();
  const targets = dailyTargets(await store.getProfile());
  return c.json({ slots, split: targets ? splitTargets(targets, slots) : [] });
});

app.put("/api/slots", async (c) => {
  const body = await c.req.json<{ slots?: unknown[] }>().catch(() => ({ slots: [] }));
  const slots = sanitiseSlots(body.slots ?? []);
  if (slots.length === 0) return c.json({ error: "minstens één eetmoment is nodig" }, 400);

  const store = storeFor(c.env);
  await store.putSlots(slots);
  const targets = dailyTargets(await store.getProfile());
  return c.json({ slots, split: targets ? splitTargets(targets, slots) : [] });
});

/** Maakt van wat de UI stuurt iets waar de verdeling mee kan rekenen. */
function sanitiseSlots(input: unknown[]): MealSlot[] {
  const out: MealSlot[] = [];
  for (const [index, item] of input.entries()) {
    if (typeof item !== "object" || item === null) continue;
    const raw = item as Record<string, unknown>;
    const name = String(raw["name"] ?? "").trim();
    if (!name) continue;
    const share = Number(raw["kcalShare"]);
    out.push({
      // Een moment zonder id is nieuw; leid er een af van de naam zodat een
      // hernoemd moment zijn plek in opgeslagen dagen niet kwijtraakt.
      id: String(raw["id"] ?? "").trim() || slugify(name) || `moment-${index}`,
      name,
      position: Number.isFinite(Number(raw["position"])) ? Number(raw["position"]) : index,
      kcalShare: Number.isFinite(share) && share > 0 ? share : 0.1,
      proteinShare:
        raw["proteinShare"] === null || raw["proteinShare"] === undefined || raw["proteinShare"] === ""
          ? null
          : Number(raw["proteinShare"]),
      enabled: raw["enabled"] !== false,
      tags: Array.isArray(raw["tags"])
        ? (raw["tags"] as unknown[]).map((t) => String(t).toLowerCase().trim()).filter(Boolean)
        : [],
      maxKcal: Number.isFinite(Number(raw["maxKcal"])) && Number(raw["maxKcal"]) > 0
        ? Number(raw["maxKcal"])
        : null,
    });
  }
  return out.sort((a, b) => a.position - b.position);
}

function slugify(value: string): string {
  return value.toLowerCase().normalize("NFD").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

// ----------------------------------------------------------------- dagplan

app.post("/api/day/generate", async (c) => {
  const body = await c.req.json<DayRequest>().catch(() => ({}) as DayRequest);
  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);

  const profile = await store.getProfile();
  const targets = body.targets ?? dailyTargets(profile);
  if (!targets) {
    return c.json(
      { error: "vul eerst je profiel in (leeftijd, geslacht, lengte, gewicht) via /api/profile" },
      409,
    );
  }

  const slots = body.slots ? sanitiseSlots(body.slots) : await store.getSlots();
  if ((await store.countPlannable()) === 0) {
    return c.json({ error: "nog geen doorgerekende recepten — draai eerst POST /api/ingest" }, 409);
  }

  const day = await generateDay(store, client, {
    date: body.date ?? today(),
    slots,
    daily: targets,
    diet: profile.diet,
    excludedTerms: await store.getExclusions(),
    excludeRecipeIds: body.excludeRecipeIds ?? [],
    kcalMode: body.kcalMode,
    candidatesPerSlot: body.candidatesPerSlot,
  });

  return c.json(day);
});

/**
 * Het lege dagoverzicht: alle eetmomenten met hun doel, nog zonder recept. Puur
 * uit de database, dus dit is meteen klaar.
 */
app.get("/api/day/blank", async (c) => {
  const store = storeFor(c.env);
  const targets = dailyTargets(await store.getProfile());
  if (!targets) {
    return c.json(
      { error: "vul eerst je profiel in (leeftijd, geslacht, lengte, gewicht)" },
      409,
    );
  }
  return c.json(blankDay(c.req.query("date") ?? today(), await store.getSlots(), targets));
});

const optionCountOf = (body: RerollRequest) => Math.min(Math.max(body.optionCount ?? 1, 1), 6);

/** Vult één eetmoment in. Zelfde werk als herrollen, zonder iets te vervangen. */
app.post("/api/day/slot", async (c) => {
  const body = await c.req.json<RerollRequest>().catch(() => ({}) as RerollRequest);
  if (!body.targets) return c.json({ error: "targets zijn verplicht" }, 400);

  const store = storeFor(c.env);
  const profile = await store.getProfile();
  const options = await rerollSlotOptions(store, clientFor(c.env, c.executionCtx), {
    targets: body.targets,
    excludeRecipeIds: body.excludeRecipeIds ?? [],
    slotTags: body.slotTags,
    diet: profile.diet,
    excludedTerms: await store.getExclusions(),
    kcalMode: body.kcalMode,
    candidates: body.candidates,
    count: optionCountOf(body),
  });

  if (options.length === 0) {
    return c.json(
      { error: "geen passend recept in de database — laat de scraper eerst meer ophalen", plan: null, options: [] },
      409,
    );
  }
  return c.json({ plan: options[0]!.plan, options });
});

/** Ander recept voor één eetmoment, met vergelijkbare macro's. */
app.post("/api/day/reroll", async (c) => {
  const body = await c.req.json<RerollRequest>().catch(() => ({}) as RerollRequest);
  if (!body.targets) return c.json({ error: "targets zijn verplicht" }, 400);

  const store = storeFor(c.env);
  const client = clientFor(c.env, c.executionCtx);
  const profile = await store.getProfile();

  const options = await rerollSlotOptions(store, client, {
    targets: body.targets,
    excludeRecipeIds: body.excludeRecipeIds ?? [],
    similarTo: body.similarTo,
    slotTags: body.slotTags,
    diet: profile.diet,
    excludedTerms: await store.getExclusions(),
    kcalMode: body.kcalMode,
    candidates: body.candidates,
    count: optionCountOf(body),
  });

  if (options.length === 0) {
    return c.json({ error: "geen ander passend recept gevonden — scrape er meer", plan: null, options: [] }, 409);
  }
  return c.json({ plan: options[0]!.plan, options });
});

// -------------------------------------------------------- opgeslagen dagen

app.post("/api/day/save", async (c) => {
  const body = await c.req
    .json<{ day?: DayPlan; id?: string; name?: string }>()
    .catch(() => ({}) as { day?: DayPlan; id?: string; name?: string });
  const day = body.day;
  if (!day || !Array.isArray(day.meals)) return c.json({ error: "day is verplicht" }, 400);

  const meals: SavedDayMeal[] = day.meals
    .filter((meal) => meal.plan)
    .map((meal) => ({
      slotId: meal.slotId,
      slotName: meal.slotName,
      position: meal.position,
      recipeId: meal.plan!.recipeId,
      portions: meal.plan!.portions,
      plan: meal.plan,
    }));
  if (meals.length === 0) return c.json({ error: "deze dag heeft geen maaltijden" }, 400);

  const id = await storeFor(c.env).saveDay({
    id: body.id,
    date: day.date ?? today(),
    name: body.name ?? null,
    targets: day.targets,
    totals: day.totals,
    meals,
  });
  return c.json({ ok: true, id });
});

app.get("/api/days", async (c) => {
  const to = c.req.query("to") ?? today();
  // Standaard de week terug: dat is het venster dat het weekoverzicht toont.
  const from = c.req.query("from") ?? shiftDate(to, -6);
  return c.json({ from, to, days: await storeFor(c.env).listDays(from, to) });
});

app.get("/api/day/:id", async (c) => {
  const day = await storeFor(c.env).getDay(c.req.param("id"));
  if (!day) return c.json({ error: "dag niet gevonden" }, 404);
  return c.json(day);
});

app.delete("/api/day/:id", async (c) => {
  await storeFor(c.env).deleteDay(c.req.param("id"));
  return c.json({ ok: true });
});

/** Boodschappenlijst over een periode opgeslagen dagen, of over één dag. */
app.get("/api/shopping", async (c) => {
  const store = storeFor(c.env);
  const dayId = c.req.query("dayId");
  const days = dayId
    ? [await store.getDay(dayId)].filter((d): d is NonNullable<typeof d> => d !== null)
    : await store.listDays(
        c.req.query("from") ?? shiftDate(today(), -6),
        c.req.query("to") ?? today(),
      );

  const inputs: ShoppingInput[] = [];
  const names = new Set<string>();
  for (const day of days) {
    for (const meal of day.meals) {
      const plan = meal.plan as Plan | null;
      if (!plan?.ingredients) continue;
      inputs.push({ label: `${day.date} · ${meal.slotName}`, plan });
      for (const ingredient of plan.ingredients) names.add(ingredient.name);
    }
  }

  // De productlinks komen uit de onthouden matches, in één query.
  const webshopIds = await store.matchMap([...names]);
  const lines = buildShoppingList(inputs.map((input) => ({ ...input, webshopIds })));
  return c.json({ days: days.length, lines });
});

// --------------------------------------------------------------- overzichten

/** Leest de pagineringsparameters die elk overzicht deelt. */
function browseParams(c: { req: { query: (k: string) => string | undefined } }) {
  const limit = Number(c.req.query("limit") ?? 100);
  const offset = Number(c.req.query("offset") ?? 0);
  return {
    query: c.req.query("q") ?? "",
    // Een ongelimiteerde dump zou de worker over zijn geheugen jagen.
    limit: Number.isFinite(limit) ? Math.min(Math.max(1, limit), 500) : 100,
    offset: Number.isFinite(offset) ? Math.max(0, offset) : 0,
  };
}

app.get("/api/browse/recipes", async (c) =>
  c.json(await storeFor(c.env).listRecipes(browseParams(c))),
);

app.get("/api/browse/products", async (c) =>
  c.json(await storeFor(c.env).listProducts(browseParams(c))),
);

app.get("/api/browse/matches", async (c) =>
  c.json(await storeFor(c.env).listMatches(browseParams(c))),
);

app.get("/api/browse/scrapes", async (c) =>
  c.json(await storeFor(c.env).listScrapes(browseParams(c))),
);

/** De ruwe payload van één archiefregel, als platte tekst. */
app.get("/api/browse/raw/:id", async (c) => {
  const row = await storeFor(c.env).getRawById(c.req.param("id"));
  if (!row) return c.json({ error: "archiefregel niet gevonden" }, 404);
  return c.text(row.body, 200, { "Content-Type": "text/plain; charset=utf-8" });
});

// ------------------------------------------------- voorkeuren en uitsluitingen

app.get("/api/prefs", async (c) => c.json({ prefs: await storeFor(c.env).listPrefs() }));

app.post("/api/prefs", async (c) => {
  const body = await c.req
    .json<{ recipeId?: string; status?: string }>()
    .catch(() => ({}) as { recipeId?: string; status?: string });
  if (!body.recipeId) return c.json({ error: "recipeId is verplicht" }, 400);
  const status =
    body.status === "fav" || body.status === "blocked" ? body.status : null;
  await storeFor(c.env).setPref(body.recipeId, status);
  return c.json({ ok: true, recipeId: body.recipeId, status });
});

app.get("/api/exclusions", async (c) => c.json({ terms: await storeFor(c.env).getExclusions() }));

app.put("/api/exclusions", async (c) => {
  const body = await c.req.json<{ terms?: unknown[] }>().catch(() => ({ terms: [] }));
  const terms = (body.terms ?? []).map((t) => String(t));
  await storeFor(c.env).putExclusions(terms);
  return c.json({ terms: await storeFor(c.env).getExclusions() });
});

// ------------------------------------------------------------------- types

interface PlanRequest {
  protein?: number;
  kcal?: number;
  carbs?: number;
  fat?: number;
  fiber?: number;
  kcalMode?: "target" | "max";
  portions?: number;
  locked?: string[];
  minScale?: number;
  maxScale?: number;
}

interface DayRequest {
  date?: string;
  /** Wint van het profiel, voor een losse dag met een afwijkend doel. */
  targets?: DailyTargets;
  slots?: unknown[];
  excludeRecipeIds?: string[];
  kcalMode?: "target" | "max";
  candidatesPerSlot?: number;
}

interface RerollRequest {
  targets?: DailyTargets;
  excludeRecipeIds?: string[];
  similarTo?: DailyTargets;
  slotTags?: string[];
  kcalMode?: "target" | "max";
  candidates?: number;
  /** Hoeveel keuzekaarten terug moeten komen; 1 (het oude gedrag) als niet gezet. */
  optionCount?: number;
}

// ----------------------------------------------------------------- helpers

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

function shiftDate(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/**
 * Rekent recepten door en bewaart de uitkomst, zodat ze in de dagplanner
 * verschijnen. Bewust foutbestendig per recept: één kapot recept mag de rest niet
 * meeslepen, want dit draait op de achtergrond en niemand ziet de fout.
 */
async function hydrate(env: Env, recipeIds: string[]): Promise<void> {
  const store = new Store(env.DB);
  const client = scrapeClient(env, store);

  for (const id of recipeIds) {
    try {
      let recipe = await store.getRecipe(id);
      if (!recipe || recipe.ingredients.length === 0) {
        recipe = await client.getRecipe(id);
        if (!recipe || recipe.ingredients.length === 0) continue;
        await store.putRecipe(recipe);
      }
      await computeNutrition(store, client, recipe);
    } catch {
      // volgende recept; dit draait buiten het antwoord om
    }
  }
}

export default {
  fetch: app.fetch,
  /**
   * Draait elk kwartier. Eén grote nachtelijke ingest liep gegarandeerd tegen
   * AH's botbescherming aan; veel kleine rondes met rust ertussen komen er wel
   * door en leveren over een dag genomen veel meer op.
   */
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      runAutoIngest(env, configFrom(env as unknown as Record<string, unknown>)).then((result) => {
        console.log("auto-ingest:", JSON.stringify(result));
      }),
    );
  },
};
