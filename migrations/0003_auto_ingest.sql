-- De database vult zichzelf voortaan de hele dag door in kleine porties. Deze
-- twee tabellen houden bij waar de rotatie gebleven is en hoe elke ronde liep.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0003_auto_ingest.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0003_auto_ingest.sql
--
-- Idempotent: twee keer draaien is veilig.
-- Kleine sleutel-waardetabel voor de stand van zaken van de automatische ingest:
-- waar de rotatie gebleven is, en tot wanneer we ons gedeisd houden na blokkades.
CREATE TABLE IF NOT EXISTS app_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

-- Eén regel per automatische ronde. Bedoeld om te kunnen zien of het bijvullen
-- nog loopt en of AH ons afknijpt, zonder in de logs te hoeven duiken.
CREATE TABLE IF NOT EXISTS ingest_runs (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  -- 'repair' vult bestaande lege recepten aan, 'moment' haalt nieuwe op.
  mode        TEXT NOT NULL,
  detail      TEXT,
  added       INTEGER NOT NULL DEFAULT 0,
  repaired    INTEGER NOT NULL DEFAULT 0,
  -- Aantal keren dat AH ons blokkeerde; loopt dit op, dan gaat het tempo omlaag.
  blocked     INTEGER NOT NULL DEFAULT 0,
  errors      TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_started ON ingest_runs(started_at DESC);
