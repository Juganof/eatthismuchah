-- Cache of scraped Allerhande recipes and the AH products their ingredients map to.
-- Everything here is reconstructible from ah.nl; dropping it only costs time.

CREATE TABLE IF NOT EXISTS recipes (
  id           TEXT PRIMARY KEY,
  title        TEXT NOT NULL,
  url          TEXT NOT NULL,
  servings     INTEGER NOT NULL DEFAULT 4,
  image_url    TEXT,
  -- RawIngredient[] as scraped, before unit conversion.
  ingredients  TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  webshop_id       TEXT PRIMARY KEY,
  title            TEXT NOT NULL,
  sales_unit_size  TEXT,
  -- Nutrients per 100 g, as JSON.
  per_100g         TEXT NOT NULL,
  fetched_at       INTEGER NOT NULL
);

-- Resolved ingredient -> product decisions, so we don't re-run matching (and its
-- product searches) for an ingredient name we've already seen.
CREATE TABLE IF NOT EXISTS ingredient_matches (
  ingredient_name  TEXT PRIMARY KEY,
  webshop_id       TEXT,
  score            REAL NOT NULL,
  matched_at       INTEGER NOT NULL
);

-- Cached per-recipe nutrition totals, used to shortlist recipes before solving.
CREATE TABLE IF NOT EXISTS recipe_nutrition (
  recipe_id   TEXT PRIMARY KEY REFERENCES recipes(id) ON DELETE CASCADE,
  kcal        REAL NOT NULL DEFAULT 0,
  protein     REAL NOT NULL DEFAULT 0,
  carbs       REAL NOT NULL DEFAULT 0,
  fat         REAL NOT NULL DEFAULT 0,
  fiber       REAL NOT NULL DEFAULT 0,
  -- Fraction of ingredients that matched a product; low values mean low trust.
  coverage    REAL NOT NULL DEFAULT 0,
  computed_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recipe_nutrition_protein ON recipe_nutrition(protein);
CREATE INDEX IF NOT EXISTS idx_recipe_nutrition_kcal ON recipe_nutrition(kcal);
CREATE INDEX IF NOT EXISTS idx_recipes_fetched ON recipes(fetched_at);
