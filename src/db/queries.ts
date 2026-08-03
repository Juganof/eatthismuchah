import type { RawScrape } from "../ah/client";
import type { Nutrients, Product, RawIngredient, Recipe } from "../ah/types";
import { deriveTags, forbiddenTags } from "../nutrition/diet";
import type { ProductMatch } from "../nutrition/resolve";
import { DEFAULT_SLOTS, type MealSlot } from "../nutrition/split";
import { DEFAULT_PROFILE, sanitiseProfile, type Profile } from "../nutrition/targets";

/**
 * Recepten en producten kennen bewust géén vervaltijd meer bij het lezen. Een
 * compleet recept mag niet stilletjes onbruikbaar worden doordat het product
 * eronder "verlopen" is — dan staat er ineens 0 kcal bij iets dat gisteren nog
 * klopte. `fetched_at` blijft als informatie bestaan; opnieuw beginnen doe je
 * met de wisknop.
 */
const MATCH_TTL_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * Hoe lang een afgekeurd recept afgekeurd blijft. Niet voor eeuwig: de reden is
 * vaak dat ónze matcher een product niet vond, en een verbetering daarin zou zulke
 * recepten anders nooit meer bereiken. Eén nieuwe poging per twee weken kost twee
 * verzoeken en houdt de deur op een kier.
 */
const SKIP_TTL_MS = 14 * 24 * 60 * 60 * 1000;

export interface RecipeSummary {
  id: string;
  title: string;
  url: string;
  servings: number;
  imageUrl: string | null;
  nutrition: Nutrients;
  coverage: number;
  tags: string[];
  /** Waar het opgeslagen recepttotaal vandaan komt: AH's eigen opgave, of zelf opgeteld uit gematchte producten. */
  source: "ah" | "products";
}

export type LogLevel = "info" | "warn" | "error";

export interface LogRow {
  id: number;
  at: number;
  level: LogLevel;
  source: string;
  message: string;
  detail: string | null;
}

/** Hoe ver `Store.wipe` gaat; zie daar voor waarom dit onderscheid bestaat. */
export type WipeScope = "scrape" | "alles";

export class Store {
  constructor(private readonly db: D1Database) {}

  // ---------------------------------------------------------------- recipes

  async getRecipe(id: string): Promise<Recipe | null> {
    const row = await this.db
      .prepare("SELECT * FROM recipes WHERE id = ?")
      .bind(id)
      .first<{
        id: string;
        title: string;
        url: string;
        servings: number;
        image_url: string | null;
        ingredients: string;
        keywords: string | null;
        nutrition_per_serving: string | null;
      }>();
    if (!row) return null;
    return {
      id: row.id,
      title: row.title,
      url: row.url,
      servings: row.servings,
      imageUrl: row.image_url,
      ingredients: JSON.parse(row.ingredients) as RawIngredient[],
      keywords: parseJson<string[]>(row.keywords, []),
      // Zonder dit zou het plannen (dat alleen uit de database leest) de gaten
      // niet meer kunnen vullen die het scrapen wél gevuld had, en een recept
      // ineens veel te weinig calorieen tellen.
      nutritionPerServing: parseJson<Nutrients | null>(row.nutrition_per_serving, null),
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
        `INSERT INTO recipes (id, title, url, servings, image_url, ingredients, fetched_at, first_seen_at, tags, keywords, nutrition_per_serving)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
           keywords = CASE
             WHEN json_array_length(excluded.keywords) > 0 THEN excluded.keywords
             ELSE recipes.keywords
           END,
           -- Een herscrape zonder voedingswaarde mag een eerder gevonden blok
           -- niet wegvagen; dat is dezelfde regel als voor de ingredienten.
           nutrition_per_serving = COALESCE(excluded.nutrition_per_serving, recipes.nutrition_per_serving),
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
        JSON.stringify(recipe.keywords ?? []),
        recipe.nutritionPerServing ? JSON.stringify(recipe.nutritionPerServing) : null,
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
      .prepare("SELECT * FROM products WHERE webshop_id = ?")
      .bind(webshopId)
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

  /**
   * Titel en verpakking voor een reeks webshop-ids, net als `matchMap` voor de
   * ingredientnamen. De boodschappenlijst heeft dit nodig om "2 × 330 g" te
   * kunnen tonen; per id apart opvragen zou tientallen ronden naar D1 kosten.
   * Grote lijsten worden in stukken bevraagd (D1's parameter-limiet).
   */
  async productMap(
    ids: string[],
  ): Promise<Record<string, { title: string; salesUnitSize: string | null }>> {
    if (ids.length === 0) return {};
    const out: Record<string, { title: string; salesUnitSize: string | null }> = {};
    for (const chunk of inChunks(ids)) {
      const { results } = await this.db
        .prepare(
          `SELECT webshop_id, title, sales_unit_size FROM products
           WHERE webshop_id IN (${chunk.map(() => "?").join(", ")})`,
        )
        .bind(...chunk)
        .all<{ webshop_id: string; title: string; sales_unit_size: string | null }>();
      for (const row of results ?? []) {
        out[row.webshop_id] = { title: row.title, salesUnitSize: row.sales_unit_size ?? null };
      }
    }
    return out;
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
   * De onthouden producten voor een reeks ingredientnamen. De boodschappenlijst
   * heeft dit nodig om productlinks te kunnen zetten; per naam apart opvragen
   * zou tientallen ronden naar D1 kosten. Een week boodschappen kan honderden
   * namen bevatten — meer dan D1 aan parameters in één query toestaat — dus
   * de lijst wordt in stukken bevraagd.
   */
  async matchMap(names: string[]): Promise<Record<string, string>> {
    if (names.length === 0) return {};
    const out: Record<string, string> = {};
    for (const chunk of inChunks(names)) {
      const { results } = await this.db
        .prepare(
          `SELECT ingredient_name, webshop_id FROM ingredient_matches
           WHERE webshop_id IS NOT NULL AND ingredient_name IN (${chunk.map(() => "?").join(", ")})`,
        )
        .bind(...chunk)
        .all<{ ingredient_name: string; webshop_id: string }>();
      for (const row of results ?? []) out[row.ingredient_name] = row.webshop_id;
    }
    return out;
  }

  /**
   * De onthouden productkoppelingen mét voedingswaarden voor een reeks
   * ingredientnamen. Waar `matchMap` alleen de webshop-ids geeft, levert dit de
   * volledige `ProductMatch` die `resolveRecipe` nodig heeft om per regel met
   * gemeten waarden te rekenen in plaats van met een gewichtsverdeling. De
   * planner en de API-aanroepen gebruiken dit in plaats van per naam apart op
   * te vragen — dat zou tientallen ronden naar D1 kosten. Grote lijsten worden
   * in stukken bevraagd (D1's parameter-limiet, zie `matchMap`).
   */
  async productMatchesFor(names: string[]): Promise<Map<string, ProductMatch>> {
    if (names.length === 0) return new Map();
    const out = new Map<string, ProductMatch>();
    for (const chunk of inChunks(names)) {
      const { results } = await this.db
        .prepare(
          `SELECT m.ingredient_name, m.score, p.webshop_id, p.title, p.sales_unit_size, p.per_100g
           FROM ingredient_matches m
           JOIN products p ON p.webshop_id = m.webshop_id
           WHERE m.ingredient_name IN (${chunk.map(() => "?").join(", ")})`,
        )
        .bind(...chunk)
        .all<{
          ingredient_name: string;
          score: number;
          webshop_id: string;
          title: string;
          sales_unit_size: string | null;
          per_100g: string;
        }>();
      for (const row of results ?? []) {
        out.set(row.ingredient_name, {
          product: {
            webshopId: row.webshop_id,
            title: row.title,
            salesUnitSize: row.sales_unit_size ?? null,
            per100g: JSON.parse(row.per_100g) as Nutrients,
          },
          score: row.score,
        });
      }
    }
    return out;
  }

  /** Lets the UI correct a bad automatic match; corrections outlive the TTL sweep. */
  async overrideMatch(ingredientName: string, webshopId: string): Promise<void> {
    await this.putMatch(ingredientName, webshopId, 1);
  }

  // -------------------------------------------------------------- nutrition

  /**
   * Bewaart de voedingswaarde van een heel recept. `source` bepaalt wie wint:
   * cijfers van AH's eigen receptpagina zijn nauwkeuriger dan wat wij uit
   * gematchte producten optellen, dus een eigen berekening overschrijft ze niet.
   */
  async putNutrition(
    recipeId: string,
    n: Nutrients,
    coverage: number,
    source: "ah" | "products" = "products",
  ): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO recipe_nutrition (recipe_id, kcal, protein, carbs, fat, fiber, coverage, computed_at, source)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(recipe_id) DO UPDATE SET
           kcal = excluded.kcal, protein = excluded.protein, carbs = excluded.carbs,
           fat = excluded.fat, fiber = excluded.fiber, coverage = excluded.coverage,
           computed_at = excluded.computed_at, source = excluded.source
         WHERE excluded.source = 'ah' OR recipe_nutrition.source <> 'ah'`,
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
        source,
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
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage, n.source
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
      limit = 40,
      recentSinceDays = 10,
    } = options;

    // Geen dekkingsfilter meer: alles in de database is compleet doorgerekend,
    // anders was het er niet in gekomen.
    const conditions = ["n.kcal > 0", "COALESCE(p.status, '') <> 'blocked'"];
    const params: unknown[] = [];

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
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage, n.source,
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

  /**
   * Recepten die de planner kan gebruiken. Sinds een recept alleen compleet de
   * database in gaat, is dat elk recept met een doorgerekend totaal — maar de
   * telling blijft apart, want hij vertelt of het scrapen ook echt iets oplevert.
   */
  async countPlannable(): Promise<number> {
    const row = await this.db
      .prepare("SELECT COUNT(*) AS n FROM recipe_nutrition WHERE kcal > 0")
      .first<{ n: number }>();
    return row?.n ?? 0;
  }

  // -------------------------------------------------- afgekeurde recepten

  /**
   * Kennen we dit recept al, of hebben we het recent afgekeurd? Allebei betekent
   * "niet ophalen": een bekend recept is compleet, en een vers afgekeurd recept
   * was dat niet te krijgen. Een oude afkeuring telt niet meer mee — zie
   * `SKIP_TTL_MS`. Eén query, want dit staat in de binnenste lus van een ronde.
   */
  async isKnownRecipe(id: string): Promise<boolean> {
    const row = await this.db
      .prepare(
        `SELECT 1 AS n FROM recipes WHERE id = ?
         UNION ALL
         SELECT 1 AS n FROM skipped_recipes WHERE id = ? AND at > ?
         LIMIT 1`,
      )
      .bind(id, id, Date.now() - SKIP_TTL_MS)
      .first<{ n: number }>();
    return row !== null;
  }

  /** Legt vast waaróp een recept sneuvelde, zodat we het nooit opnieuw ophalen. */
  async skipRecipe(id: string, reason: string): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO skipped_recipes (id, reason, at) VALUES (?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET reason = excluded.reason, at = excluded.at`,
      )
      .bind(id, reason, Date.now())
      .run();
  }

  /**
   * Eenmalige opruiming bij de overstap naar "alleen complete recepten": alles
   * zonder ingredienten of zonder doorgerekende voedingswaarde eruit. Nieuwe
   * recepten kunnen niet meer in die staat terechtkomen, dus dit is een
   * migratie en geen onderhoudstaak. Favorieten en opgeslagen dagen blijven.
   */
  async dropIncompleteRecipes(): Promise<number> {
    const ids = `SELECT r.id FROM recipes r
       LEFT JOIN recipe_nutrition n ON n.recipe_id = r.id
       WHERE (json_array_length(r.ingredients) = 0 OR COALESCE(n.kcal, 0) <= 0)
         AND NOT EXISTS (SELECT 1 FROM saved_day_meals sm WHERE sm.recipe_id = r.id)
         AND NOT EXISTS (SELECT 1 FROM recipe_prefs rp WHERE rp.recipe_id = r.id AND rp.status = 'fav')`;
    const { results } = await this.db.prepare(ids).all<{ id: string }>();
    const doomed = (results ?? []).map((r) => r.id);
    if (doomed.length === 0) return 0;

    // Een grote opruiming kan honderden recepten tegelijk wissen; ook hier
    // geldt D1's parameter-limiet, dus de IN-lijsten worden in stukken gedaan.
    for (const chunk of inChunks(doomed)) {
      const holes = chunk.map(() => "?").join(", ");
      await this.db
        .prepare(`DELETE FROM recipe_nutrition WHERE recipe_id IN (${holes})`)
        .bind(...chunk)
        .run();
      await this.db.prepare(`DELETE FROM recipes WHERE id IN (${holes})`).bind(...chunk).run();
    }
    return doomed.length;
  }

  async countSkippedRecipes(): Promise<number> {
    const row = await this.db
      .prepare("SELECT COUNT(*) AS n FROM skipped_recipes")
      .first<{ n: number }>();
    return row?.n ?? 0;
  }

  // ------------------------------------------------------------- overzichten

  /**
   * Alle recepten met hun voedingswaarde, voor de overzichtspagina. Een recept
   * zonder doorgerekende voeding hoort er juist wél bij te staan — dat is precies
   * wat je wilt zien als je je afvraagt waarom iets niet in een dagmenu opduikt.
   */
  async listRecipes(options: BrowseOptions = {}): Promise<BrowseResult<RecipeRow>> {
    const { query = "", limit = 100, offset = 0 } = options;
    const where = query ? "WHERE LOWER(r.title) LIKE ?" : "";
    const params = query ? [`%${query.toLowerCase()}%`] : [];

    const total = await this.db
      .prepare(`SELECT COUNT(*) AS n FROM recipes r ${where}`)
      .bind(...params)
      .first<{ n: number }>();

    const { results } = await this.db
      .prepare(
        `SELECT r.id, r.title, r.url, r.servings, r.tags, r.fetched_at, r.first_seen_at,
                json_array_length(r.ingredients) AS ingredient_count,
                n.kcal, n.protein, n.carbs, n.fat, n.fiber, n.coverage,
                COALESCE(p.status, '') AS pref_status
         FROM recipes r
         LEFT JOIN recipe_nutrition n ON n.recipe_id = r.id
         LEFT JOIN recipe_prefs p ON p.recipe_id = r.id
         ${where}
         ORDER BY r.fetched_at DESC
         LIMIT ? OFFSET ?`,
      )
      .bind(...params, limit, offset)
      .all<Record<string, unknown>>();

    return {
      total: total?.n ?? 0,
      rows: (results ?? []).map((r) => ({
        id: String(r["id"]),
        title: String(r["title"]),
        url: String(r["url"]),
        servings: Number(r["servings"]),
        tags: parseJson<string[]>(r["tags"] as string | null, []),
        ingredientCount: Number(r["ingredient_count"] ?? 0),
        fetchedAt: Number(r["fetched_at"]),
        firstSeenAt: r["first_seen_at"] === null ? null : Number(r["first_seen_at"]),
        prefStatus: String(r["pref_status"] ?? ""),
        // Null betekent hier "nog niet doorgerekend", niet "nul calorieen".
        nutrition:
          r["kcal"] === null || r["kcal"] === undefined
            ? null
            : {
                kcal: Number(r["kcal"]),
                protein: Number(r["protein"]),
                carbs: Number(r["carbs"]),
                fat: Number(r["fat"]),
                fiber: Number(r["fiber"]),
              },
        coverage: r["coverage"] === null || r["coverage"] === undefined ? null : Number(r["coverage"]),
      })),
    };
  }

  /** Producten met hun voedingswaarde en, waar bekend, de ingredienten die erop matchen. */
  async listProducts(options: BrowseOptions = {}): Promise<BrowseResult<ProductRow>> {
    const { query = "", limit = 100, offset = 0 } = options;
    const where = query ? "WHERE LOWER(p.title) LIKE ?" : "";
    const params = query ? [`%${query.toLowerCase()}%`] : [];

    const total = await this.db
      .prepare(`SELECT COUNT(*) AS n FROM products p ${where}`)
      .bind(...params)
      .first<{ n: number }>();

    const { results } = await this.db
      .prepare(
        `SELECT p.webshop_id, p.title, p.sales_unit_size, p.per_100g, p.fetched_at,
                (SELECT COUNT(*) FROM ingredient_matches m WHERE m.webshop_id = p.webshop_id) AS match_count,
                (SELECT GROUP_CONCAT(m.ingredient_name, ', ') FROM ingredient_matches m
                  WHERE m.webshop_id = p.webshop_id) AS matched_names
         FROM products p
         ${where}
         ORDER BY match_count DESC, p.title ASC
         LIMIT ? OFFSET ?`,
      )
      .bind(...params, limit, offset)
      .all<Record<string, unknown>>();

    return {
      total: total?.n ?? 0,
      rows: (results ?? []).map((r) => ({
        webshopId: String(r["webshop_id"]),
        title: String(r["title"]),
        salesUnitSize: (r["sales_unit_size"] as string | null) ?? null,
        per100g: parseJson<Nutrients>(r["per_100g"] as string, {}),
        fetchedAt: Number(r["fetched_at"]),
        matchCount: Number(r["match_count"] ?? 0),
        matchedNames: r["matched_names"] ? String(r["matched_names"]) : "",
      })),
    };
  }

  /** Ingredientnamen en het product waaraan ze gekoppeld zijn, inclusief de missers. */
  async listMatches(options: BrowseOptions = {}): Promise<BrowseResult<MatchRow>> {
    const { query = "", limit = 200, offset = 0 } = options;
    const where = query ? "WHERE LOWER(m.ingredient_name) LIKE ?" : "";
    const params = query ? [`%${query.toLowerCase()}%`] : [];

    const total = await this.db
      .prepare(`SELECT COUNT(*) AS n FROM ingredient_matches m ${where}`)
      .bind(...params)
      .first<{ n: number }>();

    const { results } = await this.db
      .prepare(
        `SELECT m.ingredient_name, m.webshop_id, m.score, m.matched_at, p.title AS product_title
         FROM ingredient_matches m
         LEFT JOIN products p ON p.webshop_id = m.webshop_id
         ${where}
         ORDER BY (m.webshop_id IS NULL) DESC, m.score ASC
         LIMIT ? OFFSET ?`,
      )
      .bind(...params, limit, offset)
      .all<Record<string, unknown>>();

    return {
      total: total?.n ?? 0,
      rows: (results ?? []).map((r) => ({
        ingredientName: String(r["ingredient_name"]),
        webshopId: (r["webshop_id"] as string | null) ?? null,
        productTitle: (r["product_title"] as string | null) ?? null,
        score: Number(r["score"]),
        matchedAt: Number(r["matched_at"]),
      })),
    };
  }

  /** Het scrape-archief, zonder de payload zelf — die kan megabytes groot zijn. */
  async listScrapes(options: BrowseOptions = {}): Promise<BrowseResult<ScrapeRow>> {
    const { query = "", limit = 100, offset = 0 } = options;
    const where = query ? "WHERE kind = ?" : "";
    const params = query ? [query] : [];

    const total = await this.db
      .prepare(`SELECT COUNT(*) AS n FROM scrape_raw ${where}`)
      .bind(...params)
      .first<{ n: number }>();

    const { results } = await this.db
      .prepare(
        `SELECT id, kind, ref, url, status, parsed_ok, parse_error, scraped_at,
                length(body) AS body_bytes
         FROM scrape_raw ${where}
         ORDER BY scraped_at DESC
         LIMIT ? OFFSET ?`,
      )
      .bind(...params, limit, offset)
      .all<Record<string, unknown>>();

    return {
      total: total?.n ?? 0,
      rows: (results ?? []).map((r) => ({
        id: String(r["id"]),
        kind: String(r["kind"]),
        ref: String(r["ref"]),
        url: String(r["url"]),
        status: Number(r["status"]),
        parsedOk: Number(r["parsed_ok"]) === 1,
        parseError: (r["parse_error"] as string | null) ?? null,
        scrapedAt: Number(r["scraped_at"]),
        bodyBytes: Number(r["body_bytes"] ?? 0),
      })),
    };
  }

  /** De ruwe payload van één archiefregel, voor als je wilt zien wat AH stuurde. */
  async getRawById(id: string): Promise<{ id: string; kind: string; url: string; body: string } | null> {
    return await this.db
      .prepare("SELECT id, kind, url, body FROM scrape_raw WHERE id = ?")
      .bind(id)
      .first<{ id: string; kind: string; url: string; body: string }>();
  }

  // ------------------------------------------------ automatisch bijvullen

  async getState(key: string): Promise<string | null> {
    const row = await this.db
      .prepare("SELECT value FROM app_state WHERE key = ?")
      .bind(key)
      .first<{ value: string }>();
    return row?.value ?? null;
  }

  async setState(key: string, value: string): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO app_state (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
      )
      .bind(key, value, Date.now())
      .run();
  }

  /** Opent een ronde en geeft het id terug, zodat hij later afgesloten kan worden. */
  async startRun(mode: string, detail: string | null): Promise<number> {
    const row = await this.db
      .prepare(
        "INSERT INTO ingest_runs (started_at, mode, detail) VALUES (?, ?, ?) RETURNING id",
      )
      .bind(Date.now(), mode, detail)
      .first<{ id: number }>();
    return row?.id ?? 0;
  }

  async finishRun(
    id: number,
    result: { added?: number; repaired?: number; blocked?: number; errors?: string[] },
  ): Promise<void> {
    await this.db
      .prepare(
        `UPDATE ingest_runs SET finished_at = ?, added = ?, repaired = ?, blocked = ?, errors = ?
         WHERE id = ?`,
      )
      .bind(
        Date.now(),
        result.added ?? 0,
        result.repaired ?? 0,
        result.blocked ?? 0,
        result.errors && result.errors.length > 0 ? JSON.stringify(result.errors.slice(0, 5)) : null,
        id,
      )
      .run();
  }

  /** Wat er sinds een tijdstip is binnengehaald; voedt het dagbudget. */
  async runTotalsSince(since: number): Promise<{ added: number; repaired: number; blocked: number; runs: number }> {
    const row = await this.db
      .prepare(
        `SELECT COUNT(*) AS runs, COALESCE(SUM(added),0) AS added,
                COALESCE(SUM(repaired),0) AS repaired, COALESCE(SUM(blocked),0) AS blocked
         FROM ingest_runs WHERE started_at >= ?`,
      )
      .bind(since)
      .first<{ runs: number; added: number; repaired: number; blocked: number }>();
    return {
      runs: row?.runs ?? 0,
      added: row?.added ?? 0,
      repaired: row?.repaired ?? 0,
      blocked: row?.blocked ?? 0,
    };
  }

  async recentRuns(limit: number): Promise<Record<string, unknown>[]> {
    const { results } = await this.db
      .prepare("SELECT * FROM ingest_runs ORDER BY started_at DESC LIMIT ?")
      .bind(limit)
      .all<Record<string, unknown>>();
    return results ?? [];
  }

  // ------------------------------------------------------------------- logs

  /**
   * Eén regel de log in. Gooit nooit: een log die de aanroeper laat omvallen is
   * erger dan een ontbrekende regel, en dit draait midden in het scrapen. Om
   * dezelfde reden wordt er niet gewacht op de vorige regel — de aanroeper mag
   * dit zonder `await` doen.
   */
  async log(
    level: LogLevel,
    source: string,
    message: string,
    detail?: Record<string, unknown>,
  ): Promise<void> {
    try {
      await this.db
        .prepare("INSERT INTO app_logs (at, level, source, message, detail) VALUES (?, ?, ?, ?, ?)")
        .bind(
          Date.now(),
          level,
          source,
          message.slice(0, 500),
          detail ? JSON.stringify(detail).slice(0, 2000) : null,
        )
        .run();
    } catch {
      // Een log die niet weggeschreven kan worden mag niets breken.
    }
  }

  async recentLogs(limit: number, level?: LogLevel): Promise<LogRow[]> {
    const where = level ? "WHERE level = ?" : "";
    const params: unknown[] = level ? [level, limit] : [limit];
    const { results } = await this.db
      .prepare(`SELECT id, at, level, source, message, detail FROM app_logs ${where}
                ORDER BY at DESC, id DESC LIMIT ?`)
      .bind(...params)
      .all<LogRow>();
    return results ?? [];
  }

  async countLogs(): Promise<number> {
    const row = await this.db.prepare("SELECT COUNT(*) AS n FROM app_logs").first<{ n: number }>();
    return row?.n ?? 0;
  }

  /** Houdt de log begrensd: alleen de nieuwste `keep` regels blijven staan. */
  async trimLogs(keep: number): Promise<void> {
    try {
      await this.db
        .prepare(
          `DELETE FROM app_logs WHERE id NOT IN (
             SELECT id FROM app_logs ORDER BY id DESC LIMIT ?
           )`,
        )
        .bind(keep)
        .run();
    } catch {
      // zie log(): opruimen mag nooit een ronde laten mislukken
    }
  }

  async clearLogs(): Promise<void> {
    await this.db.prepare("DELETE FROM app_logs").run();
  }

  // ------------------------------------------------------------------ wissen

  /**
   * Alles weggooien en opnieuw beginnen.
   *
   * `scope` bepaalt hoe ver dat gaat, en dat onderscheid is belangrijk: de
   * scrape-data komt van ah.nl en is altijd opnieuw op te halen, maar het
   * profiel, de eetmomenten en de opgeslagen dagen zijn van jou en staan nergens
   * anders. Standaard blijft die laatste categorie dus staan.
   *
   *   'scrape' — recepten, producten, koppelingen, voedingswaarde, het
   *              scrape-archief, de rondes en de log.
   *   'alles'  — daarbovenop het profiel, de eetmomenten, opgeslagen dagen,
   *              favorieten en uitgesloten ingredienten.
   */
  async wipe(scope: WipeScope = "scrape"): Promise<Record<string, number>> {
    const scrapeTables = [
      "recipe_nutrition",
      "ingredient_matches",
      "products",
      "recipes",
      "scrape_raw",
      "skipped_recipes",
      "ingest_runs",
      "app_state",
      "app_logs",
    ];
    const userTables = [
      "saved_day_meals",
      "saved_days",
      "recipe_prefs",
      "excluded_ingredients",
      "meal_slots",
      "profile",
    ];
    // Gebruikersdata eerst: opgeslagen dagen verwijzen naar recepten, dus in de
    // andere volgorde zou een foreign key ertussen kunnen komen.
    const tables = scope === "alles" ? [...userTables, ...scrapeTables] : scrapeTables;

    const deleted: Record<string, number> = {};
    for (const table of tables) {
      const before = await this.db
        .prepare(`SELECT COUNT(*) AS n FROM ${table}`)
        .first<{ n: number }>();
      await this.db.prepare(`DELETE FROM ${table}`).run();
      if ((before?.n ?? 0) > 0) deleted[table] = before?.n ?? 0;
    }
    return deleted;
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

export interface BrowseOptions {
  /** Vrije zoekterm; bij scrapes is dit de soort in plaats van een tekstfilter. */
  query?: string;
  limit?: number;
  offset?: number;
}

export interface BrowseResult<T> {
  /** Totaal aantal rijen dat aan het filter voldoet, los van limit. */
  total: number;
  rows: T[];
}

export interface RecipeRow {
  id: string;
  title: string;
  url: string;
  servings: number;
  tags: string[];
  ingredientCount: number;
  fetchedAt: number;
  firstSeenAt: number | null;
  prefStatus: string;
  /** Null zolang het recept nog niet is doorgerekend. */
  nutrition: Nutrients | null;
  coverage: number | null;
}

export interface ProductRow {
  webshopId: string;
  title: string;
  salesUnitSize: string | null;
  per100g: Nutrients;
  fetchedAt: number;
  matchCount: number;
  matchedNames: string;
}

export interface MatchRow {
  ingredientName: string;
  webshopId: string | null;
  productTitle: string | null;
  score: number;
  matchedAt: number;
}

export interface ScrapeRow {
  id: string;
  kind: string;
  ref: string;
  url: string;
  status: number;
  parsedOk: boolean;
  parseError: string | null;
  scrapedAt: number;
  bodyBytes: number;
}

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
  source: string;
}

// ----------------------------------------------------------------- helpers

/**
 * D1 en miniflare staan maximaal 100 gebonden parameters per query toe; meer
 * levert "too many SQL variables" op. Een IN-lijst die groter is dan die
 * limiet wordt daarom in stukken bevraagd. 90 blijft ruim onder de grens en
 * houdt het aantal ronden naar D1 klein.
 */
const MAX_IN_PARAMS = 90;

function inChunks<T>(items: T[]): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += MAX_IN_PARAMS) {
    out.push(items.slice(i, i + MAX_IN_PARAMS));
  }
  return out;
}

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
    source: r.source === "ah" ? "ah" : "products",
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
