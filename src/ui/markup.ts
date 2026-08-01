/**
 * De opmaak van de vier tabbladen. Alles staat meteen in het document; de
 * clientcode vult het en verbergt wat niet actief is. Dat scheelt een
 * renderlaag en houdt de pagina bruikbaar zodra hij binnen is.
 */

/** Profiel: waaruit het dagdoel volgt. */
export function profileTab(): string {
  return `
<section class="panel" id="panel-profiel">
  <div class="card">
    <h2>Jouw gegevens</h2>
    <div class="grid">
      <div><label for="age">Leeftijd</label><input id="age" type="number" inputmode="numeric" placeholder="30"></div>
      <div>
        <label for="sex">Geslacht</label>
        <select id="sex"><option value="">kies&hellip;</option><option value="man">man</option><option value="vrouw">vrouw</option></select>
      </div>
      <div><label for="heightCm">Lengte (cm)</label><input id="heightCm" type="number" inputmode="numeric" placeholder="180"></div>
      <div><label for="weightKg">Gewicht (kg)</label><input id="weightKg" type="number" inputmode="decimal" placeholder="80"></div>
      <div>
        <label for="activityLevel">Activiteit</label>
        <select id="activityLevel">
          <option value="sedentary">zittend werk, weinig sport</option>
          <option value="light">licht actief, 1-2x sport</option>
          <option value="moderate" selected>3-5x sport per week</option>
          <option value="active">6-7x sport per week</option>
          <option value="very_active">zwaar werk of 2x per dag</option>
        </select>
      </div>
      <div>
        <label for="goal">Doel</label>
        <select id="goal">
          <option value="cut">afvallen</option>
          <option value="maintain" selected>op gewicht blijven</option>
          <option value="bulk">aankomen</option>
        </select>
      </div>
      <div><label for="rateKgPerWeek">Tempo (kg/week)</label><input id="rateKgPerWeek" type="number" inputmode="decimal" step="0.1" value="0.5"></div>
      <div><label for="kcalOverride">Kcal handmatig</label><input id="kcalOverride" type="number" inputmode="numeric" placeholder="leeg = berekend"></div>
      <div><label for="proteinPerKg">Eiwit (g per kg)</label><input id="proteinPerKg" type="number" inputmode="decimal" step="0.1" value="1.8"></div>
      <div><label for="fatPerKg">Vet (g per kg)</label><input id="fatPerKg" type="number" inputmode="decimal" step="0.1" value="0.8"></div>
    </div>

    <div style="margin-top:12px">
      <label>Dieet</label>
      <div class="macros" id="dietChoices"></div>
    </div>

    <button id="saveProfile">Profiel opslaan</button>
    <div id="profileSummary"></div>
  </div>

  <div class="card">
    <h2>Ingredi&euml;nten uitsluiten</h2>
    <p class="muted">Komma-gescheiden. Recepten met deze woorden in de ingredi&euml;nten worden nooit voorgesteld.</p>
    <input id="exclusions" type="text" placeholder="bijv. kokos, selderij">
    <button class="secondary" id="saveExclusions">Uitsluitingen opslaan</button>
  </div>
</section>`;
}

/** Eetmomenten: hoeveel, welke, en hoe de dag verdeeld wordt. */
export function slotsTab(): string {
  return `
<section class="panel" id="panel-momenten" hidden>
  <div class="card">
    <h2>Eetmomenten</h2>
    <p class="muted">Het aandeel is relatief: 2 en 2 is hetzelfde als 25% en 25%. Je hoeft dus niet op 100% uit te komen.</p>
    <div class="slot-row" style="border-bottom:1px solid var(--line);padding-bottom:4px">
      <label style="margin:0">Naam</label>
      <label style="margin:0">Aandeel</label>
      <label style="margin:0">Aan</label>
      <span></span>
    </div>
    <div id="slotRows"></div>
    <div class="actions">
      <button class="secondary small" id="addSlot">+ Eetmoment</button>
    </div>
    <button id="saveSlots">Indeling opslaan</button>
    <div id="slotSplit"></div>
  </div>
</section>`;
}

/** De dag zelf: genereren, herrollen, opslaan. */
export function dayTab(): string {
  return `
<section class="panel" id="panel-dag" hidden>
  <div class="card">
    <h2>Dag samenstellen</h2>
    <div class="grid">
      <div><label for="dayDate">Datum</label><input id="dayDate" type="date"></div>
      <div>
        <label for="dayKcalMode">Calorie&euml;n als</label>
        <select id="dayKcalMode"><option value="target">exact doel</option><option value="max">maximum</option></select>
      </div>
    </div>
    <button id="generateDay">Genereer hele dag</button>
    <p class="muted" id="dayHint">Of vul hieronder de eetmomenten los in.</p>
    <div id="dayTotals"></div>
  </div>
  <div id="dayMeals"></div>
  <div class="card" id="daySaveCard" hidden>
    <label for="dayName">Naam (optioneel)</label>
    <input id="dayName" type="text" placeholder="bijv. trainingsdag">
    <button id="saveDay">Dag opslaan</button>
  </div>
</section>`;
}

/** Week: opgeslagen dagen en de boodschappen die erbij horen. */
export function weekTab(): string {
  return `
<section class="panel" id="panel-week" hidden>
  <div class="card">
    <h2>Opgeslagen dagen</h2>
    <div class="grid">
      <div><label for="weekFrom">Van</label><input id="weekFrom" type="date"></div>
      <div><label for="weekTo">Tot en met</label><input id="weekTo" type="date"></div>
    </div>
    <div class="actions">
      <button class="secondary small" id="loadWeek">Toon periode</button>
      <button class="secondary small" id="shoppingList">Boodschappenlijst</button>
    </div>
  </div>
  <div class="week" id="weekDays"></div>
  <div id="shoppingOut"></div>
</section>`;
}

/** Database: alles wat er is, om in te kunnen kijken. */
export function browseTab(): string {
  return `
<section class="panel" id="panel-database" hidden>
  <div class="card">
    <h2>Automatisch bijvullen</h2>
    <p class="muted">De scraper draait vanzelf, in het tempo dat ah.nl toestaat.
    Elk recept wordt in één keer helemaal afgemaakt &mdash; inclusief de
    voedingswaarde van elk ingrediënt &mdash; of het komt er niet in.</p>
    <div id="autoStatus"></div>
    <div class="actions">
      <button class="secondary small" id="autoRun">Nu een ronde draaien</button>
      <button class="secondary small" id="autoPause">Bijvullen uitzetten</button>
    </div>
    <details id="autoLogsBox">
      <summary>Logs</summary>
      <p class="muted">Wat de app doet, regel voor regel: elk verzoek aan ah.nl met
      zijn statuscode, welke ingredi&euml;nten gekoppeld werden, wat er opgeruimd is
      en elke fout. Met &laquo;Kopieer&raquo; heb je de hele log als tekst op je
      klembord om door te sturen.</p>
      <div class="actions">
        <button class="secondary small" id="logCopy">Kopieer log</button>
        <button class="secondary small" id="logRefresh">Ververs</button>
        <button class="secondary small" id="logClear">Log leegmaken</button>
        <select id="logLevel" class="small">
          <option value="">alles</option>
          <option value="error">alleen fouten</option>
          <option value="warn">waarschuwingen</option>
          <option value="info">informatie</option>
        </select>
      </div>
      <div id="appLogs"></div>
      <p class="muted">Daaronder het scrape-archief: dezelfde verzoeken, maar met
      de volledige payload erbij.</p>
      <div id="autoLogs"></div>
    </details>
  </div>

  <div class="card">
    <h2>Wat zit er in de database?</h2>
    <div class="tabs" id="browseKinds" role="tablist">
      <button class="tab browse-kind" data-kind="recipes" aria-selected="true">Recepten</button>
      <button class="tab browse-kind" data-kind="products" aria-selected="false">Producten</button>
      <button class="tab browse-kind" data-kind="matches" aria-selected="false">Koppelingen</button>
      <button class="tab browse-kind" data-kind="scrapes" aria-selected="false">Scrape-archief</button>
    </div>
    <input id="browseQuery" type="search" placeholder="zoeken in dit overzicht&hellip;">
    <div class="actions">
      <button class="secondary small" id="browsePrev">&larr; vorige</button>
      <button class="secondary small" id="browseNext">volgende &rarr;</button>
      <span class="muted" id="browseCount" style="align-self:center"></span>
    </div>
  </div>
  <div id="browseOut"></div>
</section>`;
}

/**
 * Het receptvenster: één tik op een recept, waar dan ook in de app, laat hier
 * alle ingredienten zien met het AH-product dat eraan hangt en wat elke regel
 * aan voedingswaarde bijdraagt. Leeg tot `openRecipe` het vult.
 */
export function recipeDialog(): string {
  return `
<dialog id="recipeDialog" aria-label="Recept">
  <div id="recipeBody"><p class="muted">Bezig&hellip;</p></div>
  <div class="actions">
    <button class="secondary small" id="recipeClose" type="button">Sluiten</button>
  </div>
</dialog>`;
}

/** Beheer: scrapen, archief en diagnose. */
export function adminCard(): string {
  return `
  <div class="card">
    <details>
      <summary>Beheer &amp; database</summary>
      <p class="muted">Recepten ophalen voor één eetmoment. AH's eigen indeling
      bepaalt wat meetelt, dus een ontbijt is ook echt een ontbijt.</p>
      <div class="actions">
        <button class="secondary small moment" data-moment="ontbijt">Ontbijt</button>
        <button class="secondary small moment" data-moment="lunch">Lunch</button>
        <button class="secondary small moment" data-moment="snack">Tussendoortjes</button>
        <button class="secondary small moment" data-moment="diner">Diner</button>
      </div>
      <div class="actions">
        <button class="secondary small" id="ingest">Alles door elkaar ophalen</button>
        <button class="secondary small" id="reparse">Archief opnieuw parsen</button>
        <button class="secondary small" id="probe">AH-verbinding testen</button>
      </div>
      <p class="muted">Opnieuw beginnen: dit gooit alle recepten, producten,
      koppelingen en het archief weg. Je profiel, eetmomenten en
      opgeslagen dagen blijven staan &mdash; tenzij je voor &laquo;alles&raquo; kiest.</p>
      <div class="actions">
        <button class="secondary small danger" id="wipe">Alles wissen</button>
        <button class="secondary small danger" id="wipeAll">Alles wissen, ook mijn profiel</button>
      </div>
      <p class="muted" id="stats"></p>
    </details>
  </div>`;
}
