# AH Macro Planner

Stelt hele dagen samen uit Allerhande-recepten, afgestemd op jouw macro-doelen. Je
vult je profiel in (leeftijd, gewicht, lengte, activiteit, doel), bepaalt je
eetmomenten en hun onderlinge verdeling, en de planner zoekt per moment een recept
en herschaalt de hoeveelheden zodat de dag als geheel klopt. Bevalt een maaltijd
niet, dan vraag je om een ander recept met vergelijkbare macro's.

Draait als één Cloudflare Worker met D1, dus je kunt hem vanaf je telefoon gebruiken.

## Hoe het werkt

1. **Scrapen** — `src/ah/` haalt recepten van ah.nl: één pagina per recept, met de
   ingrediënten én AH's eigen voedingswaarde per portie. Elke pagina wordt vóór het
   parsen onbewerkt weggeschreven in `scrape_raw`. Een recept gaat alleen compleet de
   database in; zie "Alleen complete recepten".
2. **Omrekenen** — `src/nutrition/units.ts` zet "2 el olijfolie" om naar grammen via
   dichtheid- en stukgewicht-tabellen. Dat gewicht is de verdeelsleutel waarmee
   `src/nutrition/resolve.ts` AH's recepttotaal over de ingrediënten verdeelt.
3. **Doelen bepalen** — `src/nutrition/targets.ts` rekent het profiel om naar een
   dagdoel (Mifflin-St Jeor → TDEE → tekort of overschot), `src/nutrition/split.ts`
   verdeelt dat over de eetmomenten.
4. **Optimaliseren** — `src/optimize/solver.ts` kiest een schaalfactor binnen redelijke
   grenzen. Het probleem is convex-kwadratisch, dus projected gradient descent vindt het
   globale optimum: geen heuristiek, geen willekeur.
5. **Dag samenstellen** — `src/optimize/day.ts` doet dat per eetmoment en schuift wat
   het ene moment te veel of te weinig opleverde door naar de volgende, zodat de dag
   klopt in plaats van alleen elk moment apart.

**Plannen raakt ah.nl nooit aan.** `resolveRecipe` is pure rekenkunde: alles wat nodig
is staat bij het recept in de database. Een dag staat er in een fractie van een seconde.

**Het gerecht schaalt als geheel.** De voedingswaarde per ingrediënt is een aandeel van
AH's recepttotaal naar gewicht, geen meting. Regels dan los van elkaar schalen zou
verzonnen precisie zijn — "minder olie" scheelt in die rekensom evenveel als "minder
courgette", en dat is niet zo. Het plan kiest daarom één factor voor het hele gerecht,
en dat is precies wél waar AH's cijfer over gaat: een halve portie is de helft van
alles. Ingrediënten kun je nog steeds vastzetten met `locked`.

## AH's eigen gegevens gebruiken

De receptpagina bevat een `ld+json`-blok van AH zelf, en daar staat meer in dan de
ingredienten alleen:

* **`keywords`** — AH's eigen labels, inclusief de menugang (`ontbijt`, `lunch`,
  `tussendoortje`, `hoofdgerecht`). Die zijn gezaghebbend voor het eetmoment: raden op
  de titel gaat mis, want een "Sandwich met kip" is volgens AH een tussendoortje.
  Alleen als AH niets over de menugang zegt, valt de app terug op de titelheuristiek.
* **`nutrition`** — de voedingswaarde per portie, door AH zelf berekend. Dit is sinds
  het loslaten van de productkoppeling de énige bron van voedingswaarde in de app, en
  wordt bij het recept bewaard (`recipes.nutrition_per_serving`) zodat het plannen er
  ook bij kan. Let op: dit blok staat alleen in de `ld+json`, terwijl de rijke
  ingrediënten alleen in de paginastate staan. De pagina bevat het recept dus twee keer
  en `collectRecipes` voegt beide samen — deed het dat niet, dan hield elk recept nul
  voedingswaarde over.
* **De paginastate zelf** — bij de huidige App Router staat die in de
  `self.__next_f`-chunks (zie `extractFlightJson`), en daar is het recept veel rijker
  dan in de `ld+json`: naam als `{singular, plural}`, `quantityUnit` naast de
  hoeveelheid, de kant-en-klare regel `"2 el milde olijfolie"` als terugval, en tags
  met AH's eigen indeling (`menugang: borrelhapje`, `speciale-wensen: vegetarisch`).
**Producten zitten er bewust niet meer in.** De app zocht vroeger bij elk ingrediënt
een AH-product om de voedingswaarde per 100 g op te halen. Dat is eruit, om drie
redenen die elkaar versterkten: het matchen op naam raadde te vaak mis ("middelgroot
scharrelei" tegen "AH Scharreleieren", "snoepkomkommer" tegen niets), veel verse
producten hebben bij AH helemaal géén voedingswaardetabel, en één misser keurde een
verder prima recept voorgoed af. Bovendien kostte het vijftien tot dertig verzoeken per
recept, waar de receptpagina er één kost — met dat verschil passen er per ronde
tientallen recepten in plaats van een handjevol.

De productzoekopdracht bestaat nog wel als handmatige actie (`GET
/api/products/search`, `POST /api/match`), voor wie een ingrediënt zelf aan een product
wil koppelen. Automatisch gebeurt er niets meer mee, en de boodschappenlijst zet alleen
nog een productlink bij ingrediënten die je zo zelf gekoppeld hebt.

## Alleen complete recepten

Een recept wordt in één keer opgeslagen of helemaal niet. Compleet betekent nog maar
twee dingen, allebei van dezelfde pagina: een ingrediëntenlijst, en AH's eigen
voedingswaarde per portie. Dat laatste staat op vrijwel elke Allerhande-pagina, dus
afkeuren is de uitzondering geworden in plaats van de regel.

Het recepttotaal is AH's cijfer maal het aantal porties. Per ingrediënt wordt dat naar
gewicht verdeeld (`resolveRecipe`), want de solver rekent per regel: 400 g kip krijgt
vier keer zoveel als 100 g, en een handvol peterselie bijna niets. Water, zout en peper
krijgen nul — die dragen echt niets bij, en meetellen zou hun aandeel van de rest
afsnoepen (`isNutritionFree`). Zo'n toegerekende regel heet `geschat`: het totaal is
van AH, de verdeling erover is van ons.

Ontbreekt de voedingswaarde, dan komt het recept in `skipped_recipes` mét de reden
("geen voedingswaarde op de receptpagina"). Twee weken lang wordt het niet
opnieuw opgehaald; daarna krijgt het één nieuwe kans, want AH rekent een recept soms
later alsnog door en een blijvende afkeuring zou dat nooit meer opmerken.

**Alleen een pagina die binnenkwam is een oordeel.** Een blokkade,
een netwerkfout of een opgeraakt verzoekbudget betekent "later nog eens proberen": dan
wordt er niets opgeslagen én niets afgekeurd. Dat onderscheid is duur geleerd — toen
een fout hier stilletjes een oordeel werd, keurde één 403 een prima recept voorgoed af.

Daarmee bestaan halffabricaten niet meer, en dus ook de machinerie eromheen niet: geen
lege recepten repareren, geen ingrediënten koppelen in een aparte ronde, geen
onbruikbare recepten opruimen, geen totalen die later nog bijgewerkt moeten worden. Wat
in de database staat, kan de planner gebruiken.

Eén ronde kost één verzoek voor de zoekpagina plus één per recept. Daar passen er dertig
in (`AUTO_BATCH`), tegen twee of drie toen elk ingrediënt nog een productzoekopdracht
kostte.

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
elke update die er een toevoegt (nu tot en met `0006_recipe_nutrition_from_ah.sql`, dat
AH's eigen voedingswaarde bij het recept bewaart en de recepten vrijgeeft die onder de
oude, strengere regel zijn afgekeurd):

```bash
npm run db:migrate          # alle migraties op de remote database
npm run db:migrate:local    # of lokaal
```

De `ALTER TABLE`-regels in `0002` en `0006` falen met "duplicate column name" als de kolommen er
al zijn; dat is verwacht en betekent dat je klaar bent. Daarom staan de migraties met
`;` achter elkaar en niet met `&&`: zo loopt de rest gewoon door.

Daarna eenmalig vullen. Eén verzoek per recept, met 700 ms ertussen:

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
  ingrediënten over alle dagen bij elkaar opgeteld, met een link naar het AH-product als
  je dat ingrediënt zelf gekoppeld hebt.
- **Database** — alles wat er in staat: recepten (met labels, voedingswaarde en welke
  nog niet doorgerekend zijn), producten, ingredient-naar-product-koppelingen en het
  scrape-archief. Doorzoekbaar, en de ruwe payload van elke scrape is op te vragen.

Een tik op de naam van een recept — op een dagkaart, een keuzekaart of in de database —
opent het receptvenster: alle ingrediënten met hun hoeveelheid en wat elke regel aan
calorieën bijdraagt, plus de voedingswaarde per portie en voor het hele gerecht. Dat het
totaal van AH komt en de verdeling per regel een schatting is, staat er ook bij.

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

**Een worker mag maar een beperkt aantal verzoeken doen per aanroep.** Op het gratis
plan zijn dat er 50, en die grens was makkelijker te raken dan hij klinkt: één recept
met vijftien ingrediënten kostte vroeger dertig productlookups, waarna Cloudflare de
hele ronde afkapte met "Too many subrequests" en élk resterend recept faalde. Nu kost
een recept één verzoek en past een hele ronde er ruim binnen. De scraper telt zijn
verzoeken nog steeds zelf mee (`AUTO_MAX_REQUESTS`, standaard 40) en stopt netjes vóór
de grens, zonder iets vast te leggen.

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

**Het recepttotaal komt van AH; de verdeling per ingrediënt is een schatting.** Wat AH
per portie opgeeft nemen we onveranderd over, en daar plant de app op. Zodra je naar één
regel kijkt is het een aandeel naar gewicht: 100 g olijfolie krijgt in die rekensom
evenveel als 100 g courgette, terwijl het in werkelijkheid een veelvoud is. Daarom
schaalt een plan het gerecht alleen als geheel, en zijn de kcal per regel niet meer dan
een indicatie. Stukgewichten ("1 ui = 110 g") en dichtheden zijn bovendien tabelwaarden.

**De dagdoelen zijn een richtlijn, geen medisch advies.** Mifflin-St Jeor is een
formule met spreiding tussen personen; het kcal-doel gaat nooit onder 1200.

**Eén gebruiker, geen login.** Er is precies één profiel (`id = 'me'`) en er zit geen
authenticatie op. Zet de worker niet op een publieke URL die je met anderen deelt.

## Ontwikkelen

```bash
npm test          # 236 tests, geen netwerk nodig
npm run typecheck
npm run dev
```

De tests draaien D1 op `node:sqlite` in het geheugen, met `schema.sql` als bron — zie
`test/helpers/d1.ts`. De echte integratietest tegen ah.nl staat uit tenzij je hem
expliciet aanzet:

```bash
LIVE_AH=1 npm test
```
