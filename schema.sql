-- Twee soorten data staan hier door elkaar, met een belangrijk verschil:
--   * De scrape-cache (recipes, products, ingredient_matches, recipe_nutrition,
--     scrape_raw) komt van ah.nl. scrape_raw is bewust GEEN cache: dat is het
--     archief, zodat een kapotte parser nooit een scrape kost.
--   * De planner-data (profile, meal_slots, saved_days, ...) is van de gebruiker
--     en nergens anders te herstellen.

CREATE TABLE IF NOT EXISTS recipes (
  id           TEXT PRIMARY KEY,
  title        TEXT NOT NULL,
  url          TEXT NOT NULL,
  servings     INTEGER NOT NULL DEFAULT 4,
  image_url    TEXT,
  -- RawIngredient[] as scraped, before unit conversion.
  ingredients  TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL,
  -- Wanneer we dit recept voor het eerst zagen; overleeft elke herscrape.
  first_seen_at INTEGER,
  -- Labels afgeleid uit titel en ingredienten: eetmoment- en dieethints, JSON.
  tags         TEXT
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

-- ---------------------------------------------------------------- scrape-archief

-- De ruwe payload van elke scrape, voor het parsen. Dit is de enige tabel die we
-- nooit weggooien: als AH zijn pagina's verandert en de parser stukgaat, kan
-- /api/reparse hiermee opnieuw beginnen zonder ah.nl nog een keer te bevragen.
CREATE TABLE IF NOT EXISTS scrape_raw (
  id           TEXT PRIMARY KEY,      -- kind:ref:scraped_at
  kind         TEXT NOT NULL,         -- 'recipe' | 'recipe_search' | 'product' | 'product_search'
  ref          TEXT NOT NULL,         -- recept-id, webshop-id of zoekterm
  url          TEXT NOT NULL,
  status       INTEGER NOT NULL,
  body         TEXT NOT NULL,         -- HTML of JSON, ongewijzigd
  parsed_ok    INTEGER NOT NULL DEFAULT 0,
  parse_error  TEXT,
  scraped_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scrape_raw_ref ON scrape_raw(kind, ref, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_scrape_raw_unparsed ON scrape_raw(parsed_ok, kind);

-- ------------------------------------------------------------------- planner

-- Eén rij (id = 'me'): de app kent geen accounts.
CREATE TABLE IF NOT EXISTS profile (
  id              TEXT PRIMARY KEY,
  age             INTEGER,
  sex             TEXT,               -- 'man' | 'vrouw'
  height_cm       REAL,
  weight_kg       REAL,
  activity_level  TEXT,               -- sedentary | light | moderate | active | very_active
  goal            TEXT,               -- cut | maintain | bulk
  -- Gewenste snelheid in kg/week; positief betekent altijd "richting het doel".
  rate_kg_per_week REAL,
  protein_per_kg  REAL,
  fat_per_kg      REAL,
  -- Gezet? Dan wint dit van de berekende TDEE.
  kcal_override   REAL,
  -- Dieetvoorkeuren als JSON-array, bv. ["vegetarisch","geen_vis"].
  diet            TEXT,
  updated_at      INTEGER NOT NULL
);

-- De eetmomenten van de dag. Aantal, naam en verdeling zijn vrij instelbaar.
CREATE TABLE IF NOT EXISTS meal_slots (
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  position       INTEGER NOT NULL,
  -- Aandeel van de dag, als fractie. Hoeft niet op 1 uit te komen: de verdeling
  -- normaliseert, zodat de gebruiker niet hoeft te rekenen.
  kcal_share     REAL NOT NULL,
  -- Null = zelfde aandeel als kcal. Los instelbaar voor bv. een eiwitrijk ontbijt.
  protein_share  REAL,
  enabled        INTEGER NOT NULL DEFAULT 1,
  -- Zoekhints voor dit moment, JSON-array, bv. ["kwark","havermout"].
  tags           TEXT,
  max_kcal       REAL
);

CREATE INDEX IF NOT EXISTS idx_meal_slots_position ON meal_slots(position);

-- Een opgeslagen dag. targets/totals worden meegeschreven zodat een dag later
-- nog te lezen is zoals hij bedoeld was, ook als het profiel intussen wijzigde.
CREATE TABLE IF NOT EXISTS saved_days (
  id         TEXT PRIMARY KEY,
  date       TEXT NOT NULL,           -- YYYY-MM-DD
  name       TEXT,
  targets    TEXT NOT NULL,
  totals     TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_days_date ON saved_days(date);

CREATE TABLE IF NOT EXISTS saved_day_meals (
  day_id     TEXT NOT NULL REFERENCES saved_days(id) ON DELETE CASCADE,
  slot_id    TEXT NOT NULL,
  slot_name  TEXT NOT NULL,
  position   INTEGER NOT NULL,
  recipe_id  TEXT NOT NULL,
  portions   REAL NOT NULL DEFAULT 1,
  -- Het volledige Plan-object. Een opgeslagen dag blijft zo reproduceerbaar,
  -- ook als het recept op ah.nl later wijzigt of uit de cache valt.
  plan       TEXT NOT NULL,
  PRIMARY KEY (day_id, slot_id)
);

CREATE INDEX IF NOT EXISTS idx_saved_day_meals_recipe ON saved_day_meals(recipe_id);

-- Favorieten en blokkades per recept.
CREATE TABLE IF NOT EXISTS recipe_prefs (
  recipe_id  TEXT PRIMARY KEY,
  status     TEXT NOT NULL,           -- 'fav' | 'blocked'
  updated_at INTEGER NOT NULL
);

-- Ingredienten die nooit in een voorstel mogen zitten (allergie, afkeer).
CREATE TABLE IF NOT EXISTS excluded_ingredients (
  term       TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);
