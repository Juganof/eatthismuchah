-- AH levert op de receptpagina zijn eigen labels (ontbijt, tussendoortje, ...) en
-- de voedingswaarde per portie mee. Die twee slaan we voortaan op: de labels
-- omdat ze betrouwbaarder zijn dan raden op de titel, de voedingswaarde omdat
-- die tientallen productzoekopdrachten per recept bespaart.
--
-- Toepassen:
--   npx wrangler d1 execute ah-macro-planner --local  --file=./migrations/0002_ah_labels.sql
--   npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0002_ah_labels.sql
--
-- SQLite kent geen ADD COLUMN IF NOT EXISTS: staan ze er al, dan faalt de regel
-- met "duplicate column name" en ben je klaar.

ALTER TABLE recipes ADD COLUMN keywords TEXT;
ALTER TABLE recipe_nutrition ADD COLUMN source TEXT NOT NULL DEFAULT 'products';
