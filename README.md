# AH Macro Planner

Stelt hele dagen samen uit Allerhande-recepten, afgestemd op jouw macro-doelen. Je
vult je profiel in (leeftijd, gewicht, lengte, activiteit, doel), bepaalt je
eetmomenten en hun onderlinge verdeling, en de planner zoekt per moment een recept
en herschaalt de hoeveelheden zodat de dag als geheel klopt. Bevalt een maaltijd
niet, dan vraag je om een ander recept met vergelijkbare macro's.

Draait als één Cloudflare Worker met D1, dus je kunt hem vanaf je telefoon gebruiken.

## Hoe het werkt

1. **Scrapen** — `src/ah/` haalt recepten van ah.nl en producten van de AH-app-API.
   Elke receptpagina wordt vóór het parsen onbewerkt weggeschreven in `scrape_raw`. Een
   recept gaat alleen compleet de database in; zie "Alleen complete recepten".
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

**Alleen echte cijfers per ingrediënt.** De planner hoeft nergens meer op te wegen of
te schatten: elk recept in de database heeft voedingswaarde per ingrediënt, anders was
het er niet in gekomen. Er was hier ooit een modus die het hele gerecht met één factor
schaalde (×1,7 op alles) omdat alleen AH's portietotaal bekend was; die is eruit.

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
* **De paginastate zelf** — bij de huidige App Router staat die in de
  `self.__next_f`-chunks (zie `extractFlightJson`), en daar is het recept veel rijker
  dan in de `ld+json`: naam als `{singular, plural}`, `quantityUnit` naast de
  hoeveelheid, de kant-en-klare regel `"2 el milde olijfolie"` als terugval, en tags
  met AH's eigen indeling (`menugang: borrelhapje`, `speciale-wensen: vegetarisch`).
* **Productlinks per ingrediënt** worden gebruikt als ze er zijn — dan komt de
  voedingswaarde rechtstreeks van dat product, zonder zoeken en zonder kans op het
  verkeerde product. In de praktijk staan ze *niet* in de receptpagina: de knop "Kies
  producten" haalt ze apart op. De code pikt ze op zodra ze er wel in staan; tot die
  tijd zoekt `match.ts` het product er zelf bij.

Voor productzoekopdrachten gebruikt de app de mobiele API, en als die het anonieme
token weigert (403 op `mobile-auth`, wat in de praktijk gebeurt) valt hij terug op de
gewone webshoppagina `www.ah.nl/producten/zoeken`. Een 403 of 429 op de zoekopdracht
zelf is iets anders — dat is Akamai's tempo-blokkade, en die blijft een blokkade in
plaats van stilletjes "geen product gevonden" te worden.

## Alleen complete recepten

Een recept wordt in één keer helemaal afgemaakt of het wordt niet opgeslagen. Compleet
betekent: elk ingrediënt heeft een AH-product met echte voedingswaarde per 100 g, óf
het hoort nul te zijn (water, zout, peper — zie `isNutritionFree` in
`src/nutrition/resolve.ts`).

Lukt dat niet, dan komt het recept in `skipped_recipes` mét de reden ("geen product
voor \"middelgroot scharrelei\""). Twee weken lang wordt het niet opnieuw opgehaald;
daarna krijgt het één nieuwe kans, want de reden is vaak dat ónze matcher het product
niet vond en een verbetering daarin zou zulke recepten anders nooit meer bereiken.

**Alleen een geslaagde zoekopdracht die niets oplevert is een oordeel.** Een blokkade,
een netwerkfout of een opgeraakt verzoekbudget betekent "later nog eens proberen": dan
wordt er niets opgeslagen én niets afgekeurd. Dat onderscheid is duur geleerd — toen
een fout hier stilletjes "geen product" werd, keurde één 403 een prima recept voorgoed
af.

Vindt de matcher niets op de hele receptregel, dan volgt één herkansing op de kernnaam:
"middelgroot scharrelei" levert niets op, "scharrelei" wel (0,38 tegen 0,60 — net onder
en net boven de drempel). Blijft het bij niets, dan noteert de log wat hij zág:
hoeveel kandidaten, de eerste drie titels en de hoogste score.

Daarmee bestaan halffabricaten niet meer, en dus ook de machinerie eromheen niet: geen
lege recepten repareren, geen ingrediënten koppelen in een aparte ronde, geen
onbruikbare recepten opruimen, geen totalen die later nog bijgewerkt moeten worden. Wat
in de database staat, kan de planner gebruiken.

Scrapen voor één eetmoment gaat via `POST /api/ingest` met `{"moment":"ontbijt"}`. De
zoektermen sturen alleen wáár gezocht wordt; wat een recept ís, bepaalt AH's eigen
label — een recept dat AH als lunch labelt is ook bruikbaar als de ontbijtronde het
tegenkwam.

## Geen scrape gaat verloren

`scrape_raw` bewaart van elke receptpagina de ruwe HTML, met status en tijdstip,
vóórdat er geparsed wordt — ook als het parsen daarna mislukt. Dat is geen cache: er
wordt nooit iets overschreven of opgeruimd. Productpagina's gaan er niet in: die zijn
500–700 kB per stuk en we bewaren er toch alleen de voedingswaarde uit.

Verandert AH zijn pagina's en repareer je de parser, dan haal je met
`POST /api/reparse` de recepten uit het archief terug zonder ah.nl nog één keer te
bevragen. `GET /api/stats` laat zien hoeveel scrapes er liggen en hoeveel er nog niet
geparsed zijn.

`/api/search`, `/api/recipe/:id` en `/api/plan` slaan op wat ze tegenkomen, langs
dezelfde regel: compleet of niet. Een recept dat je een keer gezocht hebt, kan daarna
in een dagmenu terechtkomen — daar is geen aparte ingest voor nodig.

## Opzetten

```bash
npm install
npx wrangler d1 create ah-macro-planner   # zet de database_id in wrangler.toml
npm run db:init                            # maakt de tabellen aan
npm run deploy
```

Werk je op een database die er al stond, draai dan de migraties — dat is nodig na
elke update die er een toevoegt (nu tot en met `0005_complete_only.sql`, voor de
afgekeurde recepten):

```bash
npm run db:migrate          # alle migraties op de remote database
npm run db:migrate:local    # of lokaal
```

De `ALTER TABLE`-regels in `0002` falen met "duplicate column name" als de kolommen er
al zijn; dat is verwacht en betekent dat je klaar bent. Daarom staan de migraties met
`;` achter elkaar en niet met `&&`: zo loopt de rest gewoon door.

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

Elke ronde doet hetzelfde: zoeken op de term die aan de beurt is, en elk gevonden
recept helemaal afmaken. De rotatie gaat langs alle vier de eetmomenten en binnen een
moment langs alle zoektermen, zodat de database gelijkmatig alle hoeken raakt in plaats
van vijftig pastarecepten. Recepten die we al kennen of al hebben afgekeurd kosten geen
enkel verzoek — die worden overgeslagen.

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

Onder Database staat wat de automaat doet: wat er vandaag binnenkwam, hoeveel recepten
er in de database staan, hoeveel er zijn afgekeurd, welk eetmoment hierna aan de beurt
is, en de laatste rondes met hun blokkades. Met "Bijvullen uitzetten" leg je hem stil. `POST /api/auto/run` draait er nu meteen een; met
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
| `POST /api/ingest` | Een ronde scrapen; alleen complete recepten komen erin |
| `POST /api/reparse` | Recepten terughalen uit het ruwe archief |
| `GET /api/logs` | De applicatielog; `?format=text` geeft platte tekst, `?level=error` filtert |
| `POST /api/logs/clear` | Log leegmaken |
| `POST /api/wipe` | Alles wissen (`{"scope":"scrape"}` of `{"scope":"alles"}`); zet het bijvullen uit |
| `POST /api/auto/pause` | Automatisch bijvullen aan- of uitzetten |
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

## Logs en opnieuw beginnen

Onder **Database → Automatisch bijvullen → Logs** staat een echte applicatielog:
elk verzoek aan ah.nl met zijn statuscode, welke ingrediënten aan welk product
gekoppeld werden, wat er opgeruimd is, waarom een eetmoment leeg bleef, en elke fout.
Te filteren op niveau, en met **Kopieer log** heb je alles als platte tekst op je
klembord — precies de vorm om door te sturen als je wilt laten uitzoeken wat de app
deed. De log blijft begrensd op de laatste 2000 regels (`AUTO_LOG_KEEP`); ouder gaat
elke automatische ronde weg.

Wissen zet het automatisch bijvullen meteen uit, en dat is geen bijzaak: de cron
draait elke twee minuten, dus zonder die pauze staat de database een paar tellen later
weer vol en lijkt het alsof het wissen niet werkte. Aanzetten doe je met de knop
"Bijvullen aanzetten" bij Automatisch bijvullen (of `POST /api/auto/pause`
`{"paused":false}`).

**Alles wissen** staat onder Beheer & database, in twee smaken. De gewone knop gooit
de scrape-data weg (recepten, producten, koppelingen, voedingswaarde, het archief, de
rondes en de log) en laat je profiel, eetmomenten en opgeslagen dagen staan — die zijn
van jou en nergens anders vandaan te halen. De tweede knop wist ook die. Beide vragen
eerst een bevestiging en daarna het woord `WISSEN`, want er is geen weg terug.

## Belangrijk om te weten

**De AH-endpoints zijn niet officieel.** AH publiceert geen API; alles hier is wat de
eigen app en website gebruiken en kan zonder aankondiging veranderen. De code is daar
zo goed mogelijk tegen bestand — de HTML-parser zoekt naar de vorm van een recept in
de ingebouwde paginastate in plaats van naar CSS-selectors — maar als resultaten leeg
blijven, vraag dan eerst `GET /api/probe` op: die zegt welke stap stuk is. Daarna is
`POST /api/reparse` je vangnet.

Zolang er geen enkel recept plánbaar is, gaat élke automatische ronde naar het
koppelen van ingrediënten in plaats van één op de drie: meer recepten ophalen heeft
geen zin als de planner er toch niets mee kan.

**Een worker mag maar een beperkt aantal verzoeken doen per aanroep.** Op het gratis
plan zijn dat er 50, en die grens is makkelijker te raken dan hij klinkt: één recept
met vijftien ingrediënten kostte vroeger dertig productlookups, waarna Cloudflare de
hele ronde afkapte met "Too many subrequests" en élk resterend recept faalde. Daarom
telt de scraper zijn verzoeken zelf mee (`AUTO_MAX_REQUESTS`, standaard 40) en stopt
hij netjes vóór de grens — midden in een recept, zonder iets vast te leggen. Dat een
recept nu in één keer wordt afgemaakt kost dus meer verzoeken per recept, maar het
worden er snel minder: een product dat één keer opgezocht is, staat in de database en
kost daarna niets. Bij de eerste rondes komen er een paar recepten binnen, daarna
steeds meer.

**Eén kapot recept mag de automaat niet gijzelen.** Een recept dat structureel 403
geeft (verwijderd, verhuisd) kwam vroeger elke ronde weer als eerste aan de beurt en
zette daarmee de afkoelperiode aan — in de praktijk lag het bijvullen anderhalf uur
stil voor één recept. Er wordt daarom pas afgekoeld vanaf twee rondes op rij met
blokkades (`AUTO_COOLDOWN_AFTER_BLOCKS`): één 403 op één pagina zegt niets over het
tempo.

**ah.nl remt je af.** De site staat achter Akamai's botbescherming, die op tempo
reageert en niet op aantallen: drie receptpagina's binnen een tiende seconde leveren
403's op, terwijl dezelfde pagina's rustig achter elkaar gewoon binnenkomen. Een
geblokkeerde ronde levert simpelweg niets op — er blijft niets halfs achter. De scraper houdt
daarom minimaal 700 ms tussen twee verzoeken aan en probeert een 403 opnieuw met
oplopende wachttijd. Die wachttijd staat op de cron bewust hoog (8 seconden,
`AUTO_BACKOFF_MS`): uit de log bleek zo'n blokkade tientallen seconden te duren, dus
herkansen na anderhalve seconde liep gegarandeerd tegen dezelfde muur. Afkoelen gebeurt
pas na twee rondes op rij met blokkades — één 403 is ruis.

**Voedingswaarden komen van AH, maar de omrekening is niet exact.** Elk ingrediënt in
de database heeft echte productcijfers per 100 g — anders was het recept er niet in
gekomen. Wat wél een schatting blijft: stukgewichten ("1 ui = 110 g") en dichtheden
zijn tabelwaarden, en welk product bij "milde olijfolie" hoort wordt op woorden
gematcht. Een verkeerde koppeling corrigeer je met `POST /api/match`.

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
