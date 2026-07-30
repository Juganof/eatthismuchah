# AH Macro Planner

Haalt Allerhande-recepten op, zoekt de voedingswaarde van elk ingrediënt op via de
AH-productcatalogus, en herberekent de hoeveelheden zodat een gerecht aan jouw
macro-doelen voldoet — bijvoorbeeld "diner met 60 g eiwit en 700 kcal".

Draait als één Cloudflare Worker met D1, dus je kunt hem vanaf je telefoon gebruiken.

## Hoe het werkt

1. **Scrapen** — `src/ah/` haalt recepten van ah.nl en producten van de AH-app-API.
2. **Omrekenen** — `src/nutrition/units.ts` zet "2 el olijfolie" om naar grammen via
   dichtheid- en stukgewicht-tabellen; alles verderop rekent in grammen, want
   AH-voedingswaarden zijn per 100 g.
3. **Matchen** — `src/nutrition/match.ts` koppelt een receptregel aan een AH-product
   en geeft een betrouwbaarheidsscore. Onder 0,4 melden we "geen match" in plaats van
   te gokken.
4. **Optimaliseren** — `src/optimize/solver.ts` kiest per ingrediënt een schaalfactor
   binnen redelijke grenzen. Het probleem is convex-kwadratisch, dus projected
   gradient descent vindt het globale optimum: geen heuristiek, geen willekeur.

Kruiden en zout worden bewust nauwelijks geschaald (de solver snapt niets van smaak),
en ingrediënten kun je vastzetten met `locked`.

## Opzetten

```bash
npm install
npx wrangler d1 create ah-macro-planner   # zet de database_id in wrangler.toml
npm run db:init                            # maakt de tabellen aan
npm run deploy
```

Daarna eenmalig vullen — dit is de trage stap, want elk nieuw ingrediënt kost een
productlookup:

```bash
curl -X POST https://<jouw-worker>.workers.dev/api/ingest \
  -H 'Content-Type: application/json' \
  -d '{"queries":["kip","zalm","pasta","vegetarisch"],"limit":40}'
```

Een cron in `wrangler.toml` vult de cache daarna elke nacht verder aan.

## Gebruik

Open de worker-URL op je telefoon: vul eiwit, calorieën en porties in, en je krijgt
recepten terug met per ingrediënt de aangepaste hoeveelheid.

### API

| Route | Doel |
| --- | --- |
| `POST /api/generate` | Doelen in, best passende herberekende recepten uit |
| `POST /api/plan` | Eén specifiek recept herberekenen (`recipeId`) |
| `GET /api/recipe/:id` | Voedingswaarde per ingrediënt, zonder herberekening |
| `GET /api/search?q=` | Live Allerhande-zoekopdracht |
| `POST /api/ingest` | Recepten scrapen en de cache vullen |
| `POST /api/match` | Een foute ingrediënt→product-koppeling corrigeren |
| `GET /api/probe` | Welke AH-endpoints het nog doen |

```bash
curl -X POST https://<jouw-worker>.workers.dev/api/generate \
  -H 'Content-Type: application/json' \
  -d '{"protein":60,"kcal":700,"kcalMode":"max","portions":2}'
```

## Belangrijk om te weten

**De AH-endpoints zijn niet officieel.** AH publiceert geen API; alles hier is wat de
eigen app en website gebruiken en kan zonder aankondiging veranderen. De code is daar
zo goed mogelijk tegen bestand — de HTML-parser zoekt naar de vorm van een recept in
de ingebouwde paginastate in plaats van naar CSS-selectors — maar als resultaten leeg
blijven, vraag dan eerst `GET /api/probe` op: die zegt welke stap stuk is.

**De endpoints zijn niet live geverifieerd.** Ze zijn geschreven op basis van hoe de
AH-app werkt, maar de omgeving waarin dit gebouwd is kon ah.nl niet bereiken. Reken
erop dat je bij de eerste deploy `src/ah/client.ts` moet bijstellen; `/api/probe` en
de tests in `test/parse.test.ts` zijn daarvoor het startpunt.

**Voedingswaarden zijn schattingen.** Stukgewichten ("1 ui = 110 g") en dichtheden
zijn tabelwaarden, productmatching is fuzzy, en niet elk ingrediënt heeft een
AH-product. Elk plan meldt daarom `coverage`: het aandeel van het receptgewicht
waarvoor we echte voedingswaarden hebben. Onder 80% waarschuwt de UI expliciet, want
dan zijn de totalen een onderschatting.

## Ontwikkelen

```bash
npm test          # 58 tests, geen netwerk nodig
npm run typecheck
npm run dev
```
