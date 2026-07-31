-- Vanaf nu wordt een recept in één keer helemaal afgemaakt of helemaal niet
-- opgeslagen. Recepten die niet compleet te krijgen zijn (een ingredient zonder
-- bruikbaar AH-product) komen hier te staan, zodat we ze nooit opnieuw ophalen.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0005_complete_only.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0005_complete_only.sql
--
-- Idempotent: twee keer draaien is veilig.
CREATE TABLE IF NOT EXISTS skipped_recipes (
  id     TEXT PRIMARY KEY,
  -- Waarom het niet lukte, bv. 'geen product voor "middelgroot scharrelei"'.
  reason TEXT NOT NULL,
  at     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_skipped_recipes_at ON skipped_recipes(at DESC);
