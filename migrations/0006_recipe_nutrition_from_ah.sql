-- AH zet op de receptpagina zelf de voedingswaarde per portie. Die gebruiken we
-- nu om de gaten te vullen die het matchen op producten laat vallen: juist de
-- gewoonste verse producten (courgette, een scharrelei, een sjalot) hebben bij
-- AH geen voedingswaardetabel, en één zo'n regel keurde een verder prima recept
-- voorgoed af.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0006_recipe_nutrition_from_ah.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0006_recipe_nutrition_from_ah.sql
--
-- De ALTER is niet idempotent in SQLite; "duplicate column name" bij een tweede
-- run betekent simpelweg dat deze migratie al gedraaid heeft.
ALTER TABLE recipes ADD COLUMN nutrition_per_serving TEXT;

-- Alles wat onder de oude, strengere regel is afgekeurd, verdient een nieuwe
-- kans: die recepten zijn nooit meer opgehaald puur omdat er een product
-- ontbrak. Afkeuringen om een andere reden (een pagina zonder ingredientenlijst)
-- blijven staan.
DELETE FROM skipped_recipes WHERE reason LIKE 'geen product voor%';
