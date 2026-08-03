// scripts/enrich-local.mjs — éénmalige lokale productverrijking.
//
// Dunne wrapper om de gedeelde logica in scripts/enrich-lib.mjs: alle
// recepten in de lokale D1-database in één keer verrijken, met samenvatting
// en duur. Wil je dat continu laten doen (elk nieuw gescraped recept vanzelf
// verrijkt), gebruik dan scripts/enrich-watch.mjs — die draait ook automatisch
// mee bij start-app.bat en start-lokaal.bat.

import "./ts-bootstrap.mjs";
import { curlContext, enrichOneRecipe, findLocalDb, openStore } from "./enrich-lib.mjs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_URL = import.meta.url;

function printUsage() {
  console.log(`Gebruik: node scripts/enrich-local.mjs [--help]

Haalt voor elk recept in de lokale D1-database de echte AH-producten op en
schrijft ze terug: voedingswaarde per 100 g (voor de planner) en de koppeling
per ingrediënt (voor de boodschappenlijst).

  --help   deze uitleg

Vanuit de Worker-runtime blokkeert Akamai /gql; dit script gebruikt daarom
curl.exe met een browser-sessie. Zie README, "Lokaal verrijken".

Wil je dit continu laten draaien, zodat elk nieuw gescraped recept vanzelf
verrijkt wordt: node scripts/enrich-watch.mjs.

Eisen:
  - curl.exe moet op de PATH staan.
  - De lokale database moet bestaan: start minimaal één keer
    \`wrangler dev\` of draai \`npm run db:init:local\`.
    De dev-server mag gewoon blijven draaien; het script wacht op locks.

Het script houdt zelf ≥1 s rust tussen verzoeken aan ah.nl en kan een recept
overslaan waarvan alle niet-vrije ingrediënten (water, zout, peper tellen
niet mee) al gekoppeld zijn.`);
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printUsage();
    return;
  }

  const repoRoot = join(dirname(fileURLToPath(SCRIPT_URL)), "..");
  const d1File = findLocalDb(repoRoot);
  if (d1File === null) {
    console.error("Geen lokale D1-database gevonden onder .wrangler/state/.");
    console.error("Start eerst `wrangler dev` (of draai `npm run db:init:local`) en probeer het daarna opnieuw.");
    process.exitCode = 1;
    return;
  }

  const store = openStore(d1File);
  const startedAt = Date.now();
  const totals = { processed: 0, skipped: 0, matched: 0, products: 0, cached: 0, errors: 0 };

  try {
    console.log(`Database: ${d1File}`);
    const curl = curlContext();
    await curl.ensureSession();

    const ids = await store.allRecipeIds();
    if (ids.length === 0) {
      console.log("Geen recepten in de database; niets te verrijken.");
      return;
    }

    for (const recipeId of ids) {
      const recipe = await store.getRecipe(recipeId);
      if (!recipe) {
        console.log(`${recipeId}: recept niet gevonden (overgeslagen)`);
        continue;
      }

      const line = await enrichOneRecipe(store, curl, recipe);
      if (line === null) {
        totals.skipped++;
        console.log(`${recipe.id}: alle niet-vrije ingrediënten al gekoppeld (overgeslagen)`);
        continue;
      }

      totals.processed++;
      totals.matched += line.gekoppeld;
      totals.products += line.nieuw;
      totals.cached += line.cached;
      totals.errors += line.fouten;
      const suffix = line.fouten > 0 ? `, ${line.fouten} fout(en)` : "";
      console.log(
        `${recipe.id}: ${line.gekoppeld}/${recipe.ingredients.length} ingrediënten gekoppeld, ` +
          `${line.nieuw} producten nieuw, ${line.cached} uit cache${suffix}`,
      );
    }
  } finally {
    const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `\nKlaar in ${duration} s: ${totals.processed} recepten verrijkt, ${totals.skipped} overgeslagen, ` +
        `${totals.matched} koppelingen, ${totals.products} producten nieuw, ` +
        `${totals.cached} uit cache, ${totals.errors} fout(en).`,
    );
    store.db.close();
  }
}

await main();
