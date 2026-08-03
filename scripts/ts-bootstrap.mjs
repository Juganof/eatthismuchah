// scripts/ts-bootstrap.mjs — Node-instellingen voor de script-entry's.
//
// Node kan de TS-modules uit src/ niet zonder meer laden: constructor-
// parameter-eigenschappen (src/db/queries.ts) vragen transform-ondersteuning,
// en de extensieloze relatieve imports vragen een resolve-hook. Dit module
// start daarom, als de vlag nog ontbreekt, het entry-script opnieuw op met
// --experimental-transform-types en registreert de hook zelf. Een env-marker
// voorkomt een oneindige herstartlus als de vlag op deze Node niet (meer)
// bestaat.
//
// Importeer dit als het eerste import-statement van elk script dat src/-
// modules laadt (nu: enrich-local.mjs en enrich-watch.mjs). Onder vitest niet
// importeren: daar regelt de Vite-resolver het laden van TS zelf.

import { spawnSync } from "node:child_process";
import { registerHooks } from "node:module";

if (process.features.typescript !== "transform") {
  if (process.env.AH_TS_TRANSFORMED === "1") {
    console.error("Kan de TS-modules uit src/ niet laden: --experimental-transform-types lijkt hier niet te werken.");
    process.exit(1);
  }
  const child = spawnSync(
    process.execPath,
    [
      "--experimental-transform-types",
      "--disable-warning=ExperimentalWarning",
      process.argv[1],
      ...process.argv.slice(2),
    ],
    { stdio: "inherit", env: { ...process.env, AH_TS_TRANSFORMED: "1" } },
  );
  if (child === null) {
    console.error("Kan het script niet opnieuw starten met --experimental-transform-types.");
    process.exit(1);
  }
  process.exit(child.status ?? 1);
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier.startsWith(".") && !/\.[a-zA-Z0-9]+$/.test(specifier)) {
      try {
        return nextResolve(specifier + ".ts", context);
      } catch {
        // geen .ts-buurman: laat de normale resolutie het proberen
      }
    }
    return nextResolve(specifier, context);
  },
});
