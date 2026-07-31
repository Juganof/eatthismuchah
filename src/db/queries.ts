import type { RawScrape } from "../ah/client";
import type { Nutrients, Product, RawIngredient, Recipe } from "../ah/types";
import { deriveTags, forbiddenTags } from "../nutrition/diet";
import { DEFAULT_SLOTS, type MealSlot } from "../nutrition/split";
import { DEFAULT_PROFILE, sanitiseProfile, type Profile } from "../nutrition/targets";

/** How long cached upstream data stays usable before we refetch. */
const PRODUCT_TTL_MS = 14 * 24 * 60 * 60 * 1000;
const RECIPE_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MATCH_TTL_MS = 30 * 24 * 60 * 60 * 1000;

export interface RecipeSummary {
  id: string;
  title: string;
  url: string;
  servings: number;
  imageUrl: string | null;
  nutrition: Nutrients;
  coverage: number;
  tags: string[];
}

export class Store {
  constructor(private readonly db: D1Database) {}

  // ---------------------------------------------------------------- recipes

  async getRecipe(id: string): Promise<Recipe | null> {
    const row = await this.db
      .prepare("SELECT * FROM recipes WHERE id = ? AND fetched_at > ?")
      .bind(id, Date.now() - RECIPE_TTL_MS)
      .first<{
        id: string;
        title: string;
        url: string;
        servings: number;
        image_url: string | null;
        ingredients: string;
      }>();
    if (!row) return null;
    return {
      id: row.id,
      title: row.title,
      url: row.url,
      servings: row.servings,
      imageUrl: row.image_url,
      ingredients: JSON.parse(row.ingredients) as RawIngredient[],
    };
  }

  /**
   * Bewaart een recept. Twee dingen zijn hier bewust anders dan een gewone upsert:
   * een herscrape die géén ingredienten opleverde mag een gevulde lijst nooit
   * overschrijven (dat is precies wat er gebeurt als AH zijn pagina verandert),
   * en `first_seen_at` blijft staan zodra het een keer gezet is.
   */
  async putRecipe(recipe: Recipe): Promise<void> {
    const now = Date.now();
    await this.db
      .prepare(
        `INSERT INTO recipes (id, title, url, servings, image_url, ingredients, fetched_at, first_seen_at, tags)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           title = excluded.title,
           url = excluded.url,
           servings = excluded.servings,
           image_url = COALESCE(excluded.image_url, recipes.image_url),
           ingredients = CASE
             WHEN json_array_length(excluded.ingredients) > 0 THEN excluded.ingredients
             ELSE recipes.ingredients
           END,
           tags = CASE
             WHEN json_array_length(excluded.ingredients) > 0 THEN excluded.tags
             ELSE recipes.tags
           END,
           fetched_at = excluded.fetched_at,
           first_seen_at = COALESCE(recipes.first_seen_at, excluded.first_seen_at)`,
      )
      .bind(
        recipe.id,
        recipe.title,
        recipe.url,
        recipe.servings,
        recipe.imageUrl,
        JSON.stringify(recipe.ingredients),
        now,
        now,
        JSON.stringify(deriveTags(recipe)),
      )
      .run();
  }

  /** Alle recept-ids die we kennen, ongeacht TTL. Voor onderhoudstaken. */
  async allRecipeIds(): Promise<string[]> {
    const { results } = await this.db
      .prepare("SELECT id FROM recipes")
      .all<{ id: string }>();
    return (results ?? []).map((r) => r.id);
  }

  // ------------------------------------------------------------ scrape-archief

  /**
   * Legt een ruwe scrape vast. De sleutel bevat het tijdstip, dus elke scrape
   * komt er als eigen rij bij: dit is een archief, geen cache. Een mislukte
   * insert mag de aanroeper nooit raken — archiveren gebeurt tijdens het scrapen.
   */
  async putRaw(raw: RawScrape, parsedOk = false, parseError: string | null = null): Promise<void> {
    const at = Date.now();
    try {
      await this.db
        .prepare(
          `INSERT INTO scrape_raw (id, kind, ref, url, status, body, parsed_ok, parse_error, scraped_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO NOTHING`,
        )
        .bind(
          `${raw.kind}:${raw.ref}:${at}`,
          raw.kind,
          raw.ref,
          raw.url,
          raw.status,
          raw.body,
          parsedOk ? 1 : 0,
          parseError,
          at,
        )
        .run();
    } catch {
      // opslag van het archief mag een lopende scrape niet laten mislukken
    }
  }

  /** De nieuwste ruwe payload voor een referentie, om opnieuw te parsen. */
  async latestRaw(kind: string, ref: string): Promise<{ body: string; url: string } | null> {
    return await this.db
      .prepare(
        "SELECT body, url FROM scrape_raw WHERE kind = ? AND ref = ? ORDER BY scraped_at DESC LIMIT 1",
      )
      .bind(kind, ref)
      .first<{ body: string; url: string }>();
  }

  /**
   * De nieuwste ruwe payload per referentie voor een soort. Hier hangt /api/reparse
   * aan: één poging per recept, met de meest recente scrape.
   */
  async latestRawPerRef(
    kind: string,
    limit: number,
  ): Promise<{ id: string; ref: string; body: string }[]> {
    const { results } = await this.db
      .prepare(
        `SELECT id, ref, body FROM scrape_raw
         WHERE kind = ? AND scraped_at = (
           SELECT MAX(scraped_at) FROM scrape_raw inner_raw
           WHERE inner_raw.kind = scrape_raw.kind AND inner_raw.ref = scrape_raw.ref
         )
         ORDER BY scraped_at DESC
         LIMIT ?`,
      )
      .bind(kind, limit)
      .all<{ id: string; ref: string; body: string }>();
    return results ?? [];
  }

  async markRawParsed(id: string, ok: boolean, error: string | null = null): Promise<void> {
    await this.db
      .prepare("UPDATE scrape_raw SET parsed_ok = ?, parse_error = ? WHERE id = ?")
      .bind(ok ? 1 : 0, error, id)
      .run();
  }

  async countRaw(): Promise<{ total: number; unparsed: number }> {
    const row = await this.db
      .prepare(
        "SELECT COUNT(*) AS total, SUM(CASE WHEN parsed_ok = 0 THEN 1 ELSE 0 END) AS unparsed FROM scrape_raw",
      )
      .first<{ total: number; unparsed: number | null }>();
    return { total: row?.total ?? 0, unparsed: row?.unparsed ?? 0 };
  }

  // --------------------------------------------------------------- products

  async getProduct(webshopId: string): Promise<Product | null> {
    const row = await this.db
      .prepare("SELECT * FROM products WHERE webshop_id = ? AND fetched_at > ?")
      .bind(webshopId, Date.now() - PRODUCT_TTL_MS)
      .first<{
        webshop_id: string;
        title: string;
        sales_unit_size: string | null;
        per_100g: string;
      }>();
    if (!row) return null;
    return {
      webshopId: row.webshop_id,
      title: row.title,
      salesUnitSize: row.sales_unit_size,
      per100g: JSON.parse(row.per_100g) as Nutrients,
    };
  }

  async putProduct(product: Product): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO products (webshop_id, title, sales_unit_size, per_100g, fetched_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(webshop_id) DO UPDATE SET
           title = excluded.title,
           sales_unit_size = excluded.sales_unit_size,
           per_100g = excluded.per_100g,
           fetched_at = excluded.fetched_at`,
      )
      .bind(
        product.webshopId,
        product.title,
        product.salesUnitSize,
        JSON.stringify(product.per100g),
        Date.now(),
      )
      .run();
  }

  // ---------------------------------------------------------------- matches

  /**
   * Returns the remembered decision for an ingredient name. A stored null
   * webshop_id is a real answer — "we looked, nothing matched" — so the outer
   * `undefined` means "never looked" and the inner `null` means "no match".
   */
  async getMatch(
    ingredientName: string,
  ): Promise<{ webshopId: string | null; score: number } | undefined> {
    const row = await this.db
      .prepare(
        "SELECT webshop_id, score FROM ingredient_matches WHERE ingredient_name = ? AND matched_at > ?",
      )
      .bind(ingredientName, Date.now() - MATCH_TTL_MS)
      .first<{ webshop_id: string | null; score: number }>();
    if (!row) return undefined;
    return { webshopId: row.webshop_id, score: row.score };
  }

  async putMatch(ingredientName: string, webshopId: string | null, score: number): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO ingredient_matches (ingredient_name, webshop_id, score, matched_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(ingredient_name) DO UPDATE SET
           webshop_id = excluded.webshop_id,
           score = excluded.score,
           matched_at = excluded.matched_at`,
      )
      .bind(ingredientName, webshopId, score, Date.now())
      .run();
  }

  /**
   * De onthouden producten voor een reeks ingredientnamen in één query. De
   * boodschappenlijst heeft dit nodig om productlinks te kunnen zetten; per naam
   * apart opvragen zou tientallen ronden naar D1 kosten.
   */
  async matchMap(names: string[]): Promise<Record<string, string>> {
    if (names.length === 0) return {};
    const { results } = await this.db
      .prepare(
        `SELECT ingredient_name, webshop_id FROM ingredient_matches
         WHERE webshop_id IS NOT NULL AND ingredient_name IN (${names.map(() => "?").join(", ")})`,
      )
      .bind(...names)
      .all<{ ingredient_name: string; webshop_id: string }>();

    const out: Record<string, string> = {};
    for (const row of results ?? []) out[row.ingredient_name] = row.webshop_id;
    return out;
  }

  /** Lets the UI correct a bad automatic match; corrections outlive the TTL sweep. */
  async overrideMatch(ingredientName: string, webshopId: string): Promise<void> {
    await this.putMatch(ingredientName, webshopId, 1);
  }

  // -------------------------------------------------------------- nutrition

  async putNutrition(recipeId: string, n: Nutrients, coverage: number): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO recipe_nutrition (recipe_id, kcal, protein, carbs, fat, fiber, coverage, computed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(recipe_id) DO UPDATE SET
           kcal = excluded.kcal, protein = excluded.protein, carbs = excluded.carbs,
           fat = excluded.fat, fiber = excluded.fiber, coverage = excluded.coverage,
           computed_at = excluded.computed_at`,
      )
      .bind(
        recipeId,
        n.kcal ?? 0,
        n.protein ?? 0,
        n.carbs ?? 0,
        n.fat ?? 0,
        n.fiber ?? 0,
        coverage,
        Date.now(),
      )
      .run();
  }

  /**
   * Shortlists cached recipes for a target. Ordering by protein density rather
   * than raw protein keeps 3000 kcal party dishes out of a 700 kcal dinner slot.
   */
  async shortlist(limit: number, minCoverage = 0.5): Promise<RecipeSummary[]> {
    const { results } = await this.db
      .prepare(
        `SELECT r.id, r.title, r.url, r.servings, r.image_url, r.tags,
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage
         FROM recipe_nutrition n
         JOIN recipes r ON r.id = n.recipe_id
         WHERE n.coverage >= ? AND n.kcal > 0
         ORDER BY n.protein / n.kcal DESC
         LIMIT ?`,
      )
      .bind(minCoverage, limit)
      .all<ShortlistRow>();

    return (results ?? []).map(toSummary);
  }

  /**
   * Shortlist voor één eetmoment. Filtert op wat er sowieso niet mag (dieet,
   * allergie, geblokkeerd, al gekozen vandaag) en op een ruime kcal-band rond het
   * doel — ruim, want de solver mag daarna nog schalen. Het rangschikken zelf
   * gebeurt in de planner, met het volledige plan in handen.
   */
  async shortlistForSlot(options: SlotShortlistOptions): Promise<SlotCandidate[]> {
    const {
      kcalPerPortion,
      diet = [],
      excludeRecipeIds = [],
      excludedTerms = [],
      minCoverage = 0.5,
      limit = 40,
      recentSinceDays = 10,
    } = options;

    const conditions = ["n.coverage >= ?", "n.kcal > 0", "COALESCE(p.status, '') <> 'blocked'"];
    const params: unknown[] = [minCoverage];

    // Een band rondom het doel: alles daarbuiten kan de solver niet fatsoenlijk
    // bijtrekken zonder het recept onherkenbaar te maken.
    if (kcalPerPortion > 0) {
      conditions.push("(n.kcal / MAX(r.servings, 1)) BETWEEN ? AND ?");
      params.push(kcalPerPortion * 0.35, kcalPerPortion * 2.5);
    }

    for (const tag of forbiddenTags(diet)) {
      conditions.push("COALESCE(r.tags, '[]') NOT LIKE ?");
      params.push(`%"${tag}"%`);
    }

    for (const term of excludedTerms) {
      conditions.push("LOWER(r.ingredients) NOT LIKE ?");
      params.push(`%${term.toLowerCase()}%`);
    }

    if (excludeRecipeIds.length > 0) {
      conditions.push(`r.id NOT IN (${excludeRecipeIds.map(() => "?").join(", ")})`);
      params.push(...excludeRecipeIds);
    }

    // Hoe vaak dit recept recent op een opgeslagen dag stond; de planner gebruikt
    // dat om variatie af te dwingen zonder het recept helemaal uit te sluiten.
    const recentCutoff = new Date(Date.now() - recentSinceDays * 24 * 60 * 60 * 1000)
      .toISOString()
      .slice(0, 10);

    const { results } = await this.db
      .prepare(
        `SELECT r.id, r.title, r.url, r.servings, r.image_url, r.tags,
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage,
                COALESCE(p.status, '') AS pref_status,
                (SELECT COUNT(*) FROM saved_day_meals m
                   JOIN saved_days d ON d.id = m.day_id
                  WHERE m.recipe_id = r.id AND d.date >= ?) AS recent_uses
         FROM recipe_nutrition n
         JOIN recipes r ON r.id = n.recipe_id
         LEFT JOIN recipe_prefs p ON p.recipe_id = r.id
         WHERE ${conditions.join(" AND ")}
         ORDER BY ABS((n.kcal / MAX(r.servings, 1)) - ?) ASC
         LIMIT ?`,
      )
      .bind(recentCutoff, ...params, kcalPerPortion, limit)
      .all<ShortlistRow & { pref_status: string; recent_uses: number }>();

    return (results ?? []).map((row) => ({
      ...toSummary(row),
      favourite: row.pref_status === "fav",
      recentUses: row.recent_uses ?? 0,
    }));
  }

  async countRecipes(): Promise<number> {
    const row = await this.db.prepare("SELECT COUNT(*) AS n FROM recipes").first<{ n: number }>();
    return row?.n ?? 0;
  }

  async countPlannable(): Promise<number> {
    const row = await this.db
      .prepare("SELECT COUNT(*) AS n FROM recipe_nutrition WHERE coverage >= 0.5 AND kcal > 0")
      .first<{ n: number }>();
    return row?.n ?? 0;
  }

  // ---------------------------------------------------------------- profiel

  /** Er is precies één profiel; ontbreekt het, dan geldt het standaardprofiel. */
  async getProfile(): Promise<Profile> {
    const row = await this.db
      .prepare("SELECT * FROM profile WHERE id = 'me'")
      .first<Record<string, unknown>>();
    if (!row) return { ...DEFAULT_PROFILE };
    return sanitiseProfile({
      age: row["age"] as number,
      sex: row["sex"] as Profile["sex"],
      heightCm: row["height_cm"] as number,
      weightKg: row["weight_kg"] as number,
      activityLevel: row["activity_level"] as Profile["activityLevel"],
      goal: row["goal"] as Profile["goal"],
      rateKgPerWeek: row["rate_kg_per_week"] as number,
      proteinPerKg: row["protein_per_kg"] as number,
      fatPerKg: row["fat_per_kg"] as number,
      kcalOverride: row["kcal_override"] as number,
      diet: parseJson<string[]>(row["diet"] as string | null, []),
    });
  }

  async putProfile(profile: Profile): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO profile (id, age, sex, height_cm, weight_kg, activity_level, goal,
                              rate_kg_per_week, protein_per_kg, fat_per_kg, kcal_override, diet, updated_at)
         VALUES ('me', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           age = excluded.age, sex = excluded.sex, height_cm = excluded.height_cm,
           weight_kg = excluded.weight_kg, activity_level = excluded.activity_level,
           goal = excluded.goal, rate_kg_per_week = excluded.rate_kg_per_week,
           protein_per_kg = excluded.protein_per_kg, fat_per_kg = excluded.fat_per_kg,
           kcal_override = excluded.kcal_override, diet = excluded.diet,
           updated_at = excluded.updated_at`,
      )
      .bind(
        profile.age,
        profile.sex,
        profile.heightCm,
        profile.weightKg,
        profile.activityLevel,
        profile.goal,
        profile.rateKgPerWeek,
        profile.proteinPerKg,
        profile.fatPerKg,
        profile.kcalOverride,
        JSON.stringify(profile.diet),
        Date.now(),
      )
      .run();
  }

  // ------------------------------------------------------------- eetmomenten

  /** Een lege tabel betekent "nog nooit ingesteld", dus zaaien we de standaardindeling. */
  async getSlots(): Promise<MealSlot[]> {
    const { results } = await this.db
      .prepare("SELECT * FROM meal_slots ORDER BY position ASC")
      .all<Record<string, unknown>>();
    if (!results || results.length === 0) {
      await this.putSlots(DEFAULT_SLOTS);
      return [...DEFAULT_SLOTS];
    }
    return results.map((row) => ({
      id: String(row["id"]),
      name: String(row["name"]),
      position: Number(row["position"]),
      kcalShare: Number(row["kcal_share"]),
      proteinShare: row["protein_share"] === null ? null : Number(row["protein_share"]),
      enabled: Number(row["enabled"]) === 1,
      tags: parseJson<string[]>(row["tags"] as string | null, []),
      maxKcal: row["max_kcal"] === null ? null : Number(row["max_kcal"]),
    }));
  }

  /** Vervangt de hele indeling: momenten die je weglaat verdwijnen. */
  async putSlots(slots: MealSlot[]): Promise<void> {
    await this.db.prepare("DELETE FROM meal_slots").run();
    for (const [index, slot] of slots.entries()) {
      await this.db
        .prepare(
          `INSERT INTO meal_slots (id, name, position, kcal_share, protein_share, enabled, tags, max_kcal)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(
          slot.id,
          slot.name,
          slot.position ?? index,
          slot.kcalShare,
          slot.proteinShare,
          slot.enabled ? 1 : 0,
          JSON.stringify(slot.tags ?? []),
          slot.maxKcal,
        )
        .run();
    }
  }

  // ------------------------------------------------------------ opgeslagen dagen

  async saveDay(day: SavedDayInput): Promise<string> {
    const id = day.id ?? `${day.date}-${Date.now().toString(36)}`;
    await this.db
      .prepare(
        `INSERT INTO saved_days (id, date, name, targets, totals, created_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           date = excluded.date, name = excluded.name,
           targets = excluded.targets, totals = excluded.totals`,
      )
      .bind(id, day.date, day.name ?? null, JSON.stringify(day.targets), JSON.stringify(day.totals), Date.now())
      .run();

    // Opnieuw opslaan van dezelfde dag vervangt de maaltijden; anders blijven
    // momenten staan die je net verwijderd hebt.
    await this.db.prepare("DELETE FROM saved_day_meals WHERE day_id = ?").bind(id).run();
    for (const meal of day.meals) {
      await this.db
        .prepare(
          `INSERT INTO saved_day_meals (day_id, slot_id, slot_name, position, recipe_id, portions, plan)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(id, meal.slotId, meal.slotName, meal.position, meal.recipeId, meal.portions, JSON.stringify(meal.plan))
        .run();
    }
    return id;
  }

  async getDay(id: string): Promise<SavedDay | null> {
    const row = await this.db
      .prepare("SELECT * FROM saved_days WHERE id = ?")
      .bind(id)
      .first<Record<string, unknown>>();
    if (!row) return null;
    const { results } = await this.db
      .prepare("SELECT * FROM saved_day_meals WHERE day_id = ? ORDER BY position ASC")
      .bind(id)
      .all<Record<string, unknown>>();
    return toSavedDay(row, results ?? []);
  }

  /** Alle dagen in een periode, voor het weekoverzicht. */
  async listDays(from: string, to: string): Promise<SavedDay[]> {
    const { results } = await this.db
      .prepare("SELECT * FROM saved_days WHERE date BETWEEN ? AND ? ORDER BY date ASC")
      .bind(from, to)
      .all<Record<string, unknown>>();

    const days: SavedDay[] = [];
    for (const row of results ?? []) {
      const { results: meals } = await this.db
        .prepare("SELECT * FROM saved_day_meals WHERE day_id = ? ORDER BY position ASC")
        .bind(String(row["id"]))
        .all<Record<string, unknown>>();
      days.push(toSavedDay(row, meals ?? []));
    }
    return days;
  }

  async deleteDay(id: string): Promise<void> {
    // D1 heeft foreign keys niet altijd aan staan, dus ruim de maaltijden zelf op.
    await this.db.prepare("DELETE FROM saved_day_meals WHERE day_id = ?").bind(id).run();
    await this.db.prepare("DELETE FROM saved_days WHERE id = ?").bind(id).run();
  }

  // -------------------------------------------------- voorkeuren en uitsluitingen

  async setPref(recipeId: string, status: "fav" | "blocked" | null): Promise<void> {
    if (status === null) {
      await this.db.prepare("DELETE FROM recipe_prefs WHERE recipe_id = ?").bind(recipeId).run();
      return;
    }
    await this.db
      .prepare(
        `INSERT INTO recipe_prefs (recipe_id, status, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(recipe_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at`,
      )
      .bind(recipeId, status, Date.now())
      .run();
  }

  async listPrefs(): Promise<{ recipeId: string; status: string; title: string | null }[]> {
    const { results } = await this.db
      .prepare(
        `SELECT p.recipe_id, p.status, r.title
         FROM recipe_prefs p LEFT JOIN recipes r ON r.id = p.recipe_id
         ORDER BY p.updated_at DESC`,
      )
      .all<{ recipe_id: string; status: string; title: string | null }>();
    return (results ?? []).map((r) => ({
      recipeId: r.recipe_id,
      status: r.status,
      title: r.title,
    }));
  }

  async getExclusions(): Promise<string[]> {
    const { results } = await this.db
      .prepare("SELECT term FROM excluded_ingredients ORDER BY term ASC")
      .all<{ term: string }>();
    return (results ?? []).map((r) => r.term);
  }

  async putExclusions(terms: string[]): Promise<void> {
    await this.db.prepare("DELETE FROM excluded_ingredients").run();
    for (const term of terms) {
      const clean = term.trim().toLowerCase();
      if (!clean) continue;
      await this.db
        .prepare("INSERT INTO excluded_ingredients (term, created_at) VALUES (?, ?) ON CONFLICT(term) DO NOTHING")
        .bind(clean, Date.now())
        .run();
    }
  }
}

// ------------------------------------------------------------------- types

export interface SlotShortlistOptions {
  /** Doel-kcal per portie voor dit moment; bepaalt de band en de sortering. */
  kcalPerPortion: number;
  diet?: string[];
  excludeRecipeIds?: string[];
  excludedTerms?: string[];
  minCoverage?: number;
  limit?: number;
  recentSinceDays?: number;
}

export interface SlotCandidate extends RecipeSummary {
  favourite: boolean;
  /** Hoe vaak dit recept recent op een opgeslagen dag stond. */
  recentUses: number;
}

export interface SavedDayMeal {
  slotId: string;
  slotName: string;
  position: number;
  recipeId: string;
  portions: number;
  plan: unknown;
}

export interface SavedDayInput {
  id?: string;
  date: string;
  name?: string | null;
  targets: unknown;
  totals: unknown;
  meals: SavedDayMeal[];
}

export interface SavedDay {
  id: string;
  date: string;
  name: string | null;
  targets: unknown;
  totals: unknown;
  createdAt: number;
  meals: SavedDayMeal[];
}

interface ShortlistRow {
  id: string;
  title: string;
  url: string;
  servings: number;
  image_url: string | null;
  tags: string | null;
  kcal: number;
  protein: number;
  carbs: number;
  fat: number;
  fiber: number;
  coverage: number;
}

// ----------------------------------------------------------------- helpers

function toSummary(r: ShortlistRow): RecipeSummary {
  return {
    id: r.id,
    title: r.title,
    url: r.url,
    servings: r.servings,
    imageUrl: r.image_url,
    coverage: r.coverage,
    tags: parseJson<string[]>(r.tags, []),
    nutrition: {
      kcal: r.kcal,
      protein: r.protein,
      carbs: r.carbs,
      fat: r.fat,
      fiber: r.fiber,
    },
  };
}

function toSavedDay(row: Record<string, unknown>, meals: Record<string, unknown>[]): SavedDay {
  return {
    id: String(row["id"]),
    date: String(row["date"]),
    name: (row["name"] as string | null) ?? null,
    targets: parseJson<unknown>(row["targets"] as string, {}),
    totals: parseJson<unknown>(row["totals"] as string, {}),
    createdAt: Number(row["created_at"]),
    meals: meals.map((m) => ({
      slotId: String(m["slot_id"]),
      slotName: String(m["slot_name"]),
      position: Number(m["position"]),
      recipeId: String(m["recipe_id"]),
      portions: Number(m["portions"]),
      plan: parseJson<unknown>(m["plan"] as string, null),
    })),
  };
}

/** Kolommen met JSON zijn ooit met de hand gevuld of half geschreven; val terug. */
function parseJson<T>(value: string | null, fallback: T): T {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}
