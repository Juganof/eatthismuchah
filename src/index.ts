import { Hono } from "hono";
import { AhClient } from "./ah/client";
import { Store } from "./db/queries";
import { coverageOf, resolveRecipe } from "./nutrition/resolve";
import { buildTargets, planRecipe, rankPlans, type Plan } from "./optimize/plan";
import { renderPage } from "./ui/page";

export interface Env {
  DB: D1Database;
  AH_USER_AGENT: string;
  INGEST_QUERIES: string;
  INGEST_LIMIT: string;
}

const app = new Hono<{ Bindings: Env }>();

const clientFor = (env: Env) => new AhClient(env.AH_USER_AGENT);

app.get("/", (c) => c.html(renderPage()));

/** Reports which ah.nl endpoints still work. First stop when results go empty. */
app.get("/api/probe", async (c) => c.json(await clientFor(c.env).probe()));

app.get("/api/stats", async (c) => {
  const store = new Store(c.env.DB);
  return c.json({ recipes: await store.countRecipes() });
});

/** Live Allerhande search. Results are titles only; nutrition needs /api/ingest. */
app.get("/api/search", async (c) => {
  const q = c.req.query("q");
  if (!q) return c.json({ error: "missing ?q" }, 400);
  const recipes = await clientFor(c.env).searchRecipes(q, Number(c.req.query("size") ?? 20));
  return c.json({ recipes: recipes.map(({ id, title, url, imageUrl }) => ({ id, title, url, imageUrl })) });
});

/**
 * Scrapes recipes for the given queries and works out their nutrition, filling the
 * cache that /api/generate searches. This is the slow path — it does a product
 * lookup per new ingredient — so it runs in batches and nightly via cron.
 */
app.post("/api/ingest", async (c) => {
  type IngestBody = { queries?: string[]; limit?: number };
  // An empty body is the normal case: the UI's "fetch new recipes" button posts "{}".
  const body: IngestBody = await c.req.json<IngestBody>().catch(() => ({}));
  const queries = body.queries ?? c.env.INGEST_QUERIES.split(",").map((s) => s.trim());
  const limit = body.limit ?? Number(c.env.INGEST_LIMIT || 20);
  return c.json(await ingest(c.env, queries, limit));
});

/** Nutrition and ingredient breakdown for one recipe, without any rescaling. */
app.get("/api/recipe/:id", async (c) => {
  const store = new Store(c.env.DB);
  const client = clientFor(c.env);
  const id = c.req.param("id");

  let recipe = await store.getRecipe(id);
  if (!recipe) {
    recipe = await client.getRecipe(id);
    if (!recipe) return c.json({ error: "recipe not found" }, 404);
    await store.putRecipe(recipe);
  }

  const resolved = await resolveRecipe(recipe, client, store);
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

  const store = new Store(c.env.DB);
  const client = clientFor(c.env);

  let recipe = await store.getRecipe(body.recipeId);
  if (!recipe) {
    recipe = await client.getRecipe(body.recipeId);
    if (!recipe) return c.json({ error: "recipe not found" }, 404);
    await store.putRecipe(recipe);
  }

  const resolved = await resolveRecipe(recipe, client, store);
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
  const store = new Store(c.env.DB);
  const client = clientFor(c.env);

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
    // Resolution is cache-served here: ingest already looked every product up.
    const resolved = await resolveRecipe(recipe, client, store);
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

/** Manual correction when the automatic ingredient→product match is wrong. */
app.post("/api/match", async (c) => {
  const body = await c.req.json<{ ingredient: string; webshopId: string }>();
  if (!body.ingredient || !body.webshopId) {
    return c.json({ error: "ingredient and webshopId are required" }, 400);
  }
  const store = new Store(c.env.DB);
  const product = await clientFor(c.env).getProduct(body.webshopId);
  if (!product) return c.json({ error: "product not found" }, 404);
  await store.putProduct(product);
  await store.overrideMatch(body.ingredient.toLowerCase(), product.webshopId);
  return c.json({ ok: true, product });
});

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

async function ingest(env: Env, queries: string[], limit: number) {
  const store = new Store(env.DB);
  const client = clientFor(env);
  const perQuery = Math.max(1, Math.ceil(limit / Math.max(1, queries.length)));
  let added = 0;
  const errors: string[] = [];

  for (const query of queries) {
    let found;
    try {
      found = await client.searchRecipes(query, perQuery);
    } catch (err) {
      errors.push(`search "${query}": ${err instanceof Error ? err.message : String(err)}`);
      continue;
    }

    for (const stub of found) {
      if (added >= limit) break;
      try {
        // Search results carry no ingredients, so each one needs its detail page.
        const full = stub.ingredients.length > 0 ? stub : await client.getRecipe(stub.id);
        if (!full || full.ingredients.length === 0) continue;
        await store.putRecipe(full);
        const resolved = await resolveRecipe(full, client, store);
        await store.putNutrition(full.id, resolved.total, coverageOf(resolved));
        added++;
      } catch (err) {
        errors.push(`recipe ${stub.id}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    if (added >= limit) break;
  }

  return { added, errors };
}

export default {
  fetch: app.fetch,
  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    const queries = env.INGEST_QUERIES.split(",").map((s) => s.trim());
    ctx.waitUntil(ingest(env, queries, Number(env.INGEST_LIMIT || 20)));
  },
};
