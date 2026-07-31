-- Een echte log van wat de app doet, in de database in plaats van in een
-- worker-log dat je nooit ziet. Bedoeld om te kunnen kopiëren en opsturen: elke
-- regel heeft een tijdstip, een niveau, waar hij vandaan komt en losse details.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0004_app_logs.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0004_app_logs.sql
--
-- Idempotent: twee keer draaien is veilig.
CREATE TABLE IF NOT EXISTS app_logs (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  at      INTEGER NOT NULL,
  -- 'info' | 'warn' | 'error'
  level   TEXT NOT NULL,
  -- Waar het vandaan komt: 'ah', 'auto', 'ingest', 'api', 'plan'.
  source  TEXT NOT NULL,
  message TEXT NOT NULL,
  -- Vrije extra context als JSON, bv. {"status":403,"url":"..."}.
  detail  TEXT
);

CREATE INDEX IF NOT EXISTS idx_app_logs_at ON app_logs(at DESC);
CREATE INDEX IF NOT EXISTS idx_app_logs_level ON app_logs(level, at DESC);
