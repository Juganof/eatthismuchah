# AH Macro Planner

Stelt hele dagen samen uit Allerhande-recepten, afgestemd op jouw macro-doelen. Je
vult je profiel in (leeftijd, gewicht, lengte, activiteit, doel), bepaalt je
eetmomenten en hun onderlinge verdeling, en de planner zoekt per moment een recept
en herschaalt de hoeveelheden zodat de dag als geheel klopt. Bevalt een maaltijd
niet, dan vraag je om een ander recept met vergelijkbare macro's.

Draait als één Cloudflare Worker met D1, dus je kunt hem vanaf je telefoon gebruiken.

## Hoe het werkt

1. **Scrapen** — `src/ah/` haalt recepten van ah.nl en producten van de AH-app-API.
   Elke response wordt vóór het parsen onbewerkt weggeschreven in `scrape_raw`.
2. **Omrekenen** — `src/nutrition/units.ts` zet "2 el olijfolie" om naar grammen via
   dichtheid- en stukgewicht-tabellen; alles verderop rekent in grammen, want
   AH-voedingswaarden zijn per 100 g.
3. **Matchen** — hangt AH zelf een webshopproduct aan de receptregel (de "bestel de
   ingrediënten"-koppeling), dan is dát het product: geen zoekopdracht, geen gok. Pas
   als die koppeling ontbreekt, zoekt `src/nutrition/match.ts` er zelf een bij met een
   betrouwbaarheidsscore. Onder 0,4 melden we "geen match" in plaats van te gokken.
4. **Doelen bepalen** — `src/nutrition/targets.ts` rekent het profiel om naar een
   dagdoel (Mifflin-St Jeor → TDEE → tekort of overschot), `src/nutrition/split.ts`
   verdeelt dat over de eetmomenten.
5. **Optimaliseren** — `src/optimize/solver.ts` kiest per ingrediënt een schaalfactor
   binnen redelijke grenzen. Het probleem is convex-kwadratisch, dus projected
   gradient descent vindt het globale optimum: geen heuristiek, geen willekeur.
6. **Dag samenstellen** — `src/optimize/day.ts` doet dat per eetmoment en schuift wat
   het ene moment te veel of te weinig opleverde door naar de volgende, zodat de dag
   klopt in plaats van alleen elk moment apart.

**Plannen raakt ah.nl nooit aan.** Dat klinkt vanzelfsprekend maar was het niet: een
ingredient zonder gekoppeld product liet `resolveRecipe` alsnog zoeken, en met
tientallen kandidaten maal tien ingredienten maal de verplichte pauze ertussen liep het
genereren van een dag in de minuten. Plannen draait nu in `cacheOnly`-modus en is puur
een databaseoperatie — een dag staat er in een fractie van een seconde.

**Alleen echte cijfers per ingrediënt.** Is voor minder dan de helft van het
receptgewicht bekend wat het aan macro's levert, dan komt dat recept niet in een
dagmenu — liever geen voorstel dan een voorstel dat op een schatting drijft. Er was
hier ook een modus die het hele gerecht met één factor schaalde (×1,7 op alles) als
alleen AH's portietotaal bekend was; die is eruit. Wél blijft AH's eigen portietotaal
gebruikt om de som van de productcijfers op te ijken: de verhouding tussen
ingrediënten komt uit echte productdata, de schaal uit het cijfer van AH.

Kruiden en zout worden bewust nauwelijks geschaald (de solver snapt niets van smaak),
en ingrediënten kun je vastzetten met `locked`.

## AH's eigen gegevens gebruiken

De receptpagina bevat een `ld+json`-blok van AH zelf, en daar staat meer in dan de
ingredienten alleen:

* **`keywords`** — AH's eigen labels, inclusief de menugang (`ontbijt`, `lunch`,
  `tussendoortje`, `hoofdgerecht`). Die zijn gezaghebbend voor het eetmoment: raden op
  de titel gaat mis, want een "Sandwich met kip" is volgens AH een tussendoortje.
  Alleen als AH niets over de menugang zegt, valt de app terug op de titelheuristiek.
* **`nutrition`** — de voedingswaarde per portie, door AH zelf berekend. Die wordt
  overgenomen zonder één productzoekopdracht, en een eigen berekening overschrijft hem
  nooit (`recipe_nutrition.source`). Genoeg om te plannen is het niet: daarvoor moeten
  de ingrediënten alsnog aan producten gekoppeld zijn.
* **De productlinks per ingrediënt** — AH's eigen "bestel de ingrediënten"-koppeling,
  die in de paginastate staat (bij de huidige App Router in de `self.__next_f`-chunks,
  zie `extractFlightJson`). Staat die erin, dan wordt de voedingswaarde rechtstreeks
  van dat product gehaald: één aanroep in plaats van zoeken plus scoren, en zonder
  kans op het verkeerde product.

## Alleen schone data

Recepten waar de planner niets mee kan worden automatisch opgeruimd — elke ronde van
de automaat, en met de knop "Onbruikbare recepten opruimen" ook meteen. Weg gaat een
recept dat geen ingrediënten heeft, geen doorgerekende voedingswaarde, of geen enkel
ingrediënt met echte productcijfers erachter. Twee waarborgen: er moet een
respijtperiode voorbij zijn (standaard 24 uur, `AUTO_PURGE_GRACE_MS`) en elk
ingrediënt moet al opgezocht zijn — anders gooi je weg wat nog in de pijplijn zit.
Favorieten en recepten uit een opgeslagen dag blijven altijd staan, en het
scrape-archief wordt nooit aangeraakt: een weggegooid recept is terug te halen zonder
ah.nl opnieuw te bevragen.

Scrapen per eetmoment gaat via `POST /api/ingest` met `{"moment":"ontbijt"}`. Er wordt
breed gezocht en alles wordt bewaard, maar alleen wat AH zélf als dat moment labelt
telt mee voor de limiet — de rest komt terug in `skipped`.

## Geen scrape gaat verloren

`scrape_raw` bewaart van elke AH-response de ruwe HTML of JSON, met status en tijdstip,
vóórdat er geparsed wordt — ook als het parsen daarna mislukt. Dat is geen cache: er
wordt nooit iets overschreven of opgeruimd.

Verandert AH zijn pagina's en repareer je de parser, dan haal je met
`POST /api/reparse` de recepten uit het archief terug zonder ah.nl nog één keer te
bevragen. `GET /api/stats` laat zien hoeveel scrapes er liggen en hoeveel er nog niet
geparsed zijn.

Verder slaan `/api/search`, `/api/recipe/:id` en `/api/plan` alles op wat ze
tegenkomen, inclusief de doorgerekende voedingswaarde. Een recept dat je een keer
gezocht hebt, kan daarna in een dagmenu terechtkomen — daar is geen aparte ingest meer
voor nodig.

## Opzetten

```bash
npm install
npx wrangler d1 create ah-macro-planner   # zet de database_id in wrangler.toml
npm run db:init                            # maakt de tabellen aan
npm run deploy
```

Werk je op een database die er al stond vóór de planner, draai dan de migratie:

```bash
npx wrangler d1 execute ah-macro-planner --remote --file=./migrations/0001_planner.sql
```

De twee `ALTER TABLE`-regels onderaan die migratie falen met "duplicate column name"
als ze er al zijn; dat is verwacht en betekent dat je klaar bent.

Daarna eenmalig vullen — dit is de trage stap, want elk nieuw ingrediënt kost een
productlookup:

```bash
curl -X POST https://<jouw-worker>.workers.dev/api/ingest \
  -H 'Content-Type: application/json' \
  -d '{"queries":["kip","zalm","pasta","vegetarisch"],"limit":40}'
```

Daarna vult de database zichzelf aan — zie hieronder. Hoe meer recepten er staan,
hoe beter de dagmenu's worden.

## Automatisch bijvullen

Elke 2 minuten draait een kleine ronde, de hele dag door. Dat is geen
willekeurige keuze, maar ook geen tegenspraak met "ah.nl reageert op tempo,
niet op aantallen": het tempo zit in `AUTO_MIN_INTERVAL_MS`, de rust tussen
twee verzoeken aan ah.nl zelf, en die geldt inmiddels voor de hele Worker samen
in plaats van per aanroep (zie `src/ah/client.ts`). Hoe vaak de cron aftrapt
staat daar los van — een ronde van 8 recepten duurt op dat tempo maar een
seconde of 10, dus elke 2 minuten laat ruim voldoende lucht, en levert over de
dag veel meer op dan één grote nachtelijke ingest, die gegarandeerd tegen
403's aanloopt.

Een ronde doet één ding:

1. **Staan er lege recepten?** Die eerst. Een titel zonder ingredienten is waardeloos
   voor de planner, en aanvullen kost één pagina per recept.
2. **Anders:** nieuwe recepten voor het eetmoment dat aan de beurt is. De rotatie gaat
   langs alle vier de eetmomenten en binnen een moment langs alle zoektermen, zodat de
   database gelijkmatig alle hoeken raakt in plaats van vijftig pastarecepten.

Twee remmen zitten erop. Een **dagbudget** (`AUTO_DAILY_MAX`, standaard 250 recepten)
zodat de automaat niet eindeloos doorhamert, en een **afkoelperiode**: blokkeert AH ons
toch, dan ligt het bijvullen stil. Doorgaan alsof er niets aan de hand is maakt het
namelijk alleen erger — en blokkeert AH ons meerdere keren op rij, dan verdubbelt die
afkoelperiode elke keer (tot aan `AUTO_MAX_COOLDOWN_MS`) in plaats van steeds hetzelfde
te proberen.

Alles is in te stellen via `[vars]` in `wrangler.toml`: `AUTO_BATCH` (recepten per
ronde), `AUTO_DAILY_MAX`, `AUTO_MIN_INTERVAL_MS` (rust tussen twee verzoeken),
`AUTO_BACKOFF_MS`, `AUTO_COOLDOWN_MS` en `AUTO_MAX_COOLDOWN_MS`. Hoe vaak de cron zelf
aftrapt staat in `[triggers]` &rarr; `crons` in `wrangler.toml`.

Op het profieltabblad staat wat de automaat doet: wat er vandaag binnenkwam, hoeveel
lege recepten er nog open staan, welk eetmoment hierna aan de beurt is, en de laatste
rondes met hun blokkades. `POST /api/auto/run` draait er nu meteen een; met
`{"force":true}` negeert hij de afkoelperiode en het dagbudget.

## Gebruik

Open de worker-URL op je telefoon. Vier tabbladen:

- **Profiel** — leeftijd, geslacht, lengte, gewicht, activiteit en doel. Je ziet
  meteen je rustverbranding, dagverbruik en het dagdoel in macro's. Hier staan ook je
  dieetkeuzes en de ingrediënten die je nooit wilt zien.
- **Eetmomenten** — zoveel momenten als je wilt, met per moment een aandeel van de dag
  en zoekhints (bijv. `kwark, havermout` bij het ontbijt). De aandelen zijn relatief:
  ze worden genormaliseerd, dus je hoeft niet op 100% uit te komen.
- **Dag** — je ziet meteen alle eetmomenten met hun doel, nog leeg. Vul ze los in met
  "Genereer dit eetmoment", of laat de hele dag in één klik samenstellen. Per maaltijd
  kun je om een ander recept vragen (vergelijkbare macro's, ander gerecht), favoriet
  maken of blokkeren. Onderaan het dagtotaal tegenover je doel, en "Dag opslaan".
- **Week** — de opgeslagen dagen in een periode, en de boodschappenlijst erbij:
  ingrediënten over alle dagen bij elkaar opgeteld, met links naar het AH-product.
- **Database** — alles wat er in staat: recepten (met labels, voedingswaarde en welke
  nog niet doorgerekend zijn), producten, ingredient-naar-product-koppelingen en het
  scrape-archief. Doorzoekbaar, en de ruwe payload van elke scrape is op te vragen.

### API

| Route | Doel |
| --- | --- |
| `GET`/`PUT /api/profile` | Profiel lezen en opslaan; geeft TDEE en dagdoelen mee |
| `GET`/`PUT /api/slots` | Eetmomenten en hun verdeling |
| `GET /api/day/blank` | Leeg dagoverzicht: alle eetmomenten met hun doel |
| `POST /api/day/generate` | Hele dag samenstellen |
| `POST /api/day/slot` | Eén eetmoment invullen |
| `POST /api/day/reroll` | Ander recept voor één eetmoment |
| `POST /api/day/save` | Dag bewaren |
| `GET /api/days?from=&to=` | Opgeslagen dagen in een periode |
| `GET`/`DELETE /api/day/:id` | Eén opgeslagen dag |
| `GET /api/shopping?from=&to=` | Boodschappenlijst (of `?dayId=`) |
| `GET`/`POST /api/prefs` | Favorieten en blokkades |
| `GET`/`PUT /api/exclusions` | Uitgesloten ingrediënten |
| `POST /api/generate` | Los: doelen in, best passende herberekende recepten uit |
| `POST /api/plan` | Eén specifiek recept herberekenen (`recipeId`) |
| `GET /api/recipe/:id` | Voedingswaarde per ingrediënt, zonder herberekening |
| `GET /api/search?q=` | Live Allerhande-zoekopdracht; bewaart wat het vindt |
| `POST /api/ingest` | Recepten scrapen en de cache vullen |
| `POST /api/reparse` | Recepten terughalen uit het ruwe archief |
| `POST /api/repair` | Lege recepten alsnog ophalen (na een geblokkeerde scrape) |
| `POST /api/purge` | Onbruikbare recepten opruimen (`graceMs` optioneel) |
| `GET /api/auto/status` | Stand van het automatisch bijvullen |
| `POST /api/auto/run` | Nu een ronde draaien in plaats van wachten op de cron |
| `GET /api/browse/recipes` | Alle recepten met labels, voeding en dekking |
| `GET /api/browse/products` | Alle producten met hun koppelingen |
| `GET /api/browse/matches` | Ingredient naar product, missers eerst |
| `GET /api/browse/scrapes` | Het scrape-archief; `/api/browse/raw/:id` geeft de payload |
| `POST /api/match` | Een foute ingrediënt→product-koppeling corrigeren |
| `GET /api/probe` | Welke AH-endpoints het nog doen |
| `GET /api/stats` | Aantallen recepten, bruikbare recepten en bewaarde scrapes |

```bash
curl -X POST https://<jouw-worker>.workers.dev/api/day/generate \
  -H 'Content-Type: application/json' -d '{"date":"2026-08-01"}'
```

## Belangrijk om te weten

**De AH-endpoints zijn niet officieel.** AH publiceert geen API; alles hier is wat de
eigen app en website gebruiken en kan zonder aankondiging veranderen. De code is daar
zo goed mogelijk tegen bestand — de HTML-parser zoekt naar de vorm van een recept in
de ingebouwde paginastate in plaats van naar CSS-selectors — maar als resultaten leeg
blijven, vraag dan eerst `GET /api/probe` op: die zegt welke stap stuk is. Daarna is
`POST /api/reparse` je vangnet.

**ah.nl remt je af, en dat merk je aan lege recepten.** De site staat achter Akamai's
botbescherming, die op tempo reageert en niet op aantallen: drie receptpagina's binnen
een tiende seconde leveren 403's op, terwijl dezelfde pagina's rustig achter elkaar
gewoon binnenkomen. Een zoekopdracht geeft alleen titels, dus als de detailpagina
daarna geblokkeerd wordt, blijft een recept met 0 ingredienten achter. De scraper houdt
daarom minimaal 700 ms tussen twee verzoeken aan en probeert een 403 opnieuw met
oplopende wachttijd. Blijven er toch lege recepten staan, dan haalt
`POST /api/repair` ze alsnog op — het aantal staat in `/api/stats`.

**Voedingswaarden zijn schattingen.** Stukgewichten ("1 ui = 110 g") en dichtheden
zijn tabelwaarden, productmatching is fuzzy, en niet elk ingrediënt heeft een
AH-product. Elk plan meldt daarom `coverage`: het aandeel van het receptgewicht
waarvoor we echte voedingswaarden hebben. Onder 80% waarschuwt de UI expliciet, want
dan zijn de totalen een onderschatting.

**De dagdoelen zijn een richtlijn, geen medisch advies.** Mifflin-St Jeor is een
formule met spreiding tussen personen; het kcal-doel gaat nooit onder 1200.

**Eén gebruiker, geen login.** Er is precies één profiel (`id = 'me'`) en er zit geen
authenticatie op. Zet de worker niet op een publieke URL die je met anderen deelt.

## Ontwikkelen

```bash
npm test          # 185 tests, geen netwerk nodig
npm run typecheck
npm run dev
```

De tests draaien D1 op `node:sqlite` in het geheugen, met `schema.sql` als bron — zie
`test/helpers/d1.ts`. De echte integratietest tegen ah.nl staat uit tenzij je hem
expliciet aanzet:

```bash
LIVE_AH=1 npm test
```
