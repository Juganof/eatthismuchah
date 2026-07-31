-- Maakt van de recept-herschaler een dagplanner: scrape-archief, profiel,
-- eetmomenten, opgeslagen dagen, favorieten en uitsluitingen.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0001_planner.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0001_planner.sql
--
-- Idempotent: twee keer draaien is veilig. De ALTER TABLE-regels onderaan zijn dat
-- niet vanzelf (SQLite kent geen ADD COLUMN IF NOT EXISTS), dus die staan apart
-- beschreven; wrangler stopt bij de eerste fout, daarom staan ze als laatste.

CREATE TABLE IF NOT EXISTS scrape_raw (
  id           TEXT PRIMARY KEY,
  kind         TEXT NOT NULL,
  ref          TEXT NOT NULL,
  url          TEXT NOT NULL,
  status       INTEGER NOT NULL,
  body         TEXT NOT NULL,
  parsed_ok    INTEGER NOT NULL DEFAULT 0,
  parse_error  TEXT,
  scraped_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scrape_raw_ref ON scrape_raw(kind, ref, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_scrape_raw_unparsed ON scrape_raw(parsed_ok, kind);

CREATE TABLE IF NOT EXISTS profile (
  id              TEXT PRIMARY KEY,
  age             INTEGER,
  sex             TEXT,
  height_cm       REAL,
  weight_kg       REAL,
  activity_level  TEXT,
  goal            TEXT,
  rate_kg_per_week REAL,
  protein_per_kg  REAL,
  fat_per_kg      REAL,
  kcal_override   REAL,
  diet            TEXT,
  updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meal_slots (
  id             TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  position       INTEGER NOT NULL,
  kcal_share     REAL NOT NULL,
  protein_share  REAL,
  enabled        INTEGER NOT NULL DEFAULT 1,
  tags           TEXT,
  max_kcal       REAL
);

CREATE INDEX IF NOT EXISTS idx_meal_slots_position ON meal_slots(position);

CREATE TABLE IF NOT EXISTS saved_days (
  id         TEXT PRIMARY KEY,
  date       TEXT NOT NULL,
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
  plan       TEXT NOT NULL,
  PRIMARY KEY (day_id, slot_id)
);

CREATE INDEX IF NOT EXISTS idx_saved_day_meals_recipe ON saved_day_meals(recipe_id);

CREATE TABLE IF NOT EXISTS recipe_prefs (
  recipe_id  TEXT PRIMARY KEY,
  status     TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS excluded_ingredients (
  term       TEXT PRIMARY KEY,
  created_at INTEGER NOT NULL
);

-- Twee nieuwe kolommen op de bestaande recipes-tabel. Draai je deze migratie op
-- een database waar ze al staan, dan faalt precies deze regel met "duplicate
-- column name" — dat is verwacht en betekent dat je verder niets hoeft te doen.
ALTER TABLE recipes ADD COLUMN first_seen_at INTEGER;
ALTER TABLE recipes ADD COLUMN tags TEXT;
