// scripts/enrich-watch.mjs — continue productverrijking voor de lokale D1.
//
// De verrijking kan niet vanuit de Worker-runtime (Akamai blokkeert /gql);
// dit script doet het daarom lokaal en continu: elke ~20 seconden kijkt het
// of er recepten zijn waarvan nog niet-vrije ingrediënten een
// productkoppeling missen, en verrijkt er per ronde een handvol. Zo wordt
// elk recept dat de dev-server (her)scrapt vanzelf verrijkt, zonder aparte
// stap. Start hem met start-app.bat / start-lokaal.bat of zelf via
//   node scripts/enrich-watch.mjs
// De eerste ronde begint meteen; Ctrl+C stopt hem.

import "./ts-bootstrap.mjs";
import {
  curlContext,
  enrichOneRecipe,
  findLocalDb,
  markRecipeDone,
  needsEnrichment,
  openStore,
} from "./enrich-lib.mjs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_URL = import.meta.url;
const repoRoot = join(dirname(fileURLToPath(SCRIPT_URL)), "..");

// Hoelang slapen tussen twee ronden; de eerste ronde begint meteen.
const RONDE_INTERVAL_MS = 20_000;
// Bovengrens per ronde, zodat de watcher nooit eindeloos bezig is: de
// resterende recepten komen in een volgende ronde aan de beurt.
const MAX_PER_RONDE = 5;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function tijd() {
  return new Date().toLocaleTimeString("nl-NL", { hour12: false });
}

/**
 * Eén ronde: recepten zoeken die verrijkt moeten worden en er maximaal
 * MAX_PER_RONDE verrijken. Korte logregel per recept, daarna de
 * ronde-samenvatting. Levert true terug als er fouten waren, zodat de
 * aanroeper de curl-sessie kan vernieuwen voor de volgende ronde.
 */
async function rondeUitvoeren(curl, store, ronde) {
  const ids = await store.allRecipeIds();
  const kandidaten = [];
  for (const recipeId of ids) {
    const recipe = await store.getRecipe(recipeId);
    if (!recipe) continue;
    if (await needsEnrichment(store, recipe)) kandidaten.push(recipe);
  }

  if (kandidaten.length === 0) {
    console.log(
      `[${tijd()}] Ronde ${ronde}: verrijkingen: 0 (${ids.length} recepten; alle niet-vrije ingrediënten gekoppeld)`,
    );
    return false;
  }

  const batch = kandidaten.slice(0, MAX_PER_RONDE);
  const totalen = { gekoppeld: 0, nieuw: 0, cached: 0, fouten: 0 };
  for (const recipe of batch) {
    const line = await enrichOneRecipe(store, curl, recipe);
    if (line === null) continue; // tussen selectie en verwerking toch gekoppeld geraakt
    totalen.gekoppeld += line.gekoppeld;
    totalen.nieuw += line.nieuw;
    totalen.cached += line.cached;
    totalen.fouten += line.fouten;
    // Een poging zonder fouten is afgerond: markeer het recept klaar, ook als
    // er regels zonder suggestie overbleven — die blijven voor altijd open.
    // Met fouten (403, netwerk) blijft het recept juist staan voor de
    // volgende ronde.
    if (line.fouten === 0) await markRecipeDone(store, recipe.id);
    const suffix = line.fouten > 0 ? `, ${line.fouten} fout(en)` : "";
    console.log(
      `[${tijd()}] ${recipe.id}: ${line.gekoppeld}/${recipe.ingredients.length} ingrediënten gekoppeld, ` +
        `${line.nieuw} producten nieuw, ${line.cached} uit cache${suffix}`,
    );
  }

  const rest = kandidaten.length - batch.length;
  console.log(
    `[${tijd()}] Ronde ${ronde}: ${batch.length} recepten verrijkt (${totalen.gekoppeld} koppelingen, ` +
      `${totalen.nieuw} producten nieuw, ${totalen.cached} uit cache, ${totalen.fouten} fout(en))` +
      (rest > 0 ? `, ${rest} wachten op een volgende ronde` : ""),
  );

  // Een verzuurde sessie (403 op alles) heeft vers fruit nodig: één rustige
  // GET op de homepage vernieuwt de cookie-jar voor de volgende ronde.
  if (totalen.fouten > 0) {
    console.log(`[${tijd()}] Ronde ${ronde}: sessie vernieuwen na fout(en)...`);
    await curl.ensureSession();
    return true;
  }
  return false;
}

async function main() {
  console.log(`Verrijker gestart: elke ${RONDE_INTERVAL_MS / 1000} s controleren op recepten zonder productkoppelingen.`);
  console.log("Ctrl+C stopt hem. (Eénmalig alles verrijken: node scripts/enrich-local.mjs)");

  const curl = curlContext();
  let ronde = 0;
  for (;;) {
    if (ronde > 0) await sleep(RONDE_INTERVAL_MS);
    ronde++;

    const d1File = findLocalDb(repoRoot);
    if (d1File === null) {
      // Geen database (nog): de dev-server mag gewoon eerst starten. Niet
      // stoppen, want dan zou de watcher nooit opstarten vóór de dev-server.
      console.log(
        `[${tijd()}] Ronde ${ronde}: geen lokale D1-database onder .wrangler/state/ — wachten tot de ` +
          `dev-server hem aanmaakt (herkansing over ${RONDE_INTERVAL_MS / 1000} s)`,
      );
      continue;
    }

    let store;
    try {
      store = openStore(d1File);
    } catch (err) {
      console.log(
        `[${tijd()}] Ronde ${ronde}: database ${d1File} kon niet worden geopend (${err.message}) — ` +
          `herkansing over ${RONDE_INTERVAL_MS / 1000} s`,
      );
      continue;
    }

    try {
      await rondeUitvoeren(curl, store, ronde);
    } catch (err) {
      // Bijv. een database die net opnieuw aangemaakt wordt (tabellen nog
      // niet aanwezig): melden en de volgende ronde opnieuw proberen.
      console.log(`[${tijd()}] Ronde ${ronde}: ${err.message}`);
    } finally {
      store.db.close();
    }
  }
}

// Ctrl+C stopt de watcher; een eventuele curl-aanroep loopt nog af.
process.on("SIGINT", () => {
  console.log("\nVerrijker gestopt.");
  process.exit(0);
});

await main();
