/**
 * De clientcode. Bewust gewone globale JavaScript zonder modules of buildstap,
 * in dezelfde stijl als de rest van het project.
 *
 * Let op bij het aanpassen: dit is een TypeScript-template-string, dus geen
 * backticks en geen dollar-accolades in de JavaScript hieronder — vandaar dat
 * alles met stringoptelling is geschreven.
 */
import { shoppingLinesToText } from "../plan/shopping";
import { kcalPer100g } from "./card-info";

export const script = `
const $ = (id) => document.getElementById(id);
const g = (n) => (n === undefined || n === null ? "?" : Math.round(n));
const num = (id) => { const v = parseFloat($(id).value); return Number.isFinite(v) ? v : null; };
const positive = (id) => { const v = num(id); return v !== null && v > 0 ? v : null; };

const DIETS = [
  ["vegetarisch", "vegetarisch"], ["veganistisch", "veganistisch"], ["geen_vis", "geen vis"],
  ["geen_varken", "geen varken"], ["geen_noten", "geen noten"], ["glutenvrij", "glutenvrij"],
  ["lactosevrij", "lactosevrij"]
];

/** De dag die nu op het scherm staat, plus wat je per moment hebt weggeklikt. */
let currentDay = null;
const rejected = {};

async function api(path, options) {
  const res = await fetch(path, options);
  const body = await res.json().catch(() => ({ error: "geen geldige respons" }));
  if (!res.ok) throw new Error(body.error || ("HTTP " + res.status));
  return body;
}

const postJson = (path, body) => api(path, {
  method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
});
const putJson = (path, body) => api(path, {
  method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
});

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function toast(message, isError) {
  const el = $("toast");
  el.innerHTML = '<div class="card"><p class="' + (isError ? "note" : "ok") + '">' + escapeHtml(message) + '</p></div>';
  // Meldingen verdwijnen vanzelf; een foutmelding blijft langer staan om te lezen.
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.innerHTML = ""; }, isError ? 9000 : 3500);
}

async function run(button, fn) {
  const label = button.textContent;
  button.disabled = true;
  button.textContent = "Bezig\\u2026";
  try { await fn(); }
  catch (err) { toast(err.message, true); }
  finally { button.disabled = false; button.textContent = label; }
}

/** Short unit word for a recipe line, matching how the recipe itself wrote it. */
function unitSuffix(source, unit) {
  if (source === "spoon") {
    if (unit === "eetlepel") return "el";
    if (unit === "theelepel") return "tl";
    return unit || "el";
  }
  if (source === "volume") return unit || "ml";
  if (source === "piece" && unit) return unit;
  return "";
}

function formatQty(n) {
  return String(Math.round(n * 100) / 100);
}

/**
 * Bananen blijven bananen en eetlepels blijven eetlepels: alleen ingrediënten die
 * het recept al in gram opgaf, tonen we in gram. Voor de rest is gram een intern
 * rekenmiddel voor de voedingswaarde, niet wat je gaat afmeten.
 */
function amountFor(i) {
  const changed = Math.abs(i.scale - 1) > 0.05;
  const useUnit =
    (i.gramsSource === "piece" || i.gramsSource === "spoon" || i.gramsSource === "volume")
    && i.originalQuantity !== null;

  if (!useUnit) {
    return changed
      ? '<span class="chg">' + i.grams + ' g</span> <s>' + i.originalGrams + ' g</s>'
      : i.grams + ' g';
  }

  const suffix = unitSuffix(i.gramsSource, i.unit);
  const unitStr = suffix ? " " + suffix : "";
  const scaledQty = formatQty(i.originalQuantity * i.scale) + unitStr;
  const origQty = formatQty(i.originalQuantity) + unitStr;
  return changed
    ? '<span class="chg">' + scaledQty + '</span> <s>' + origQty + '</s>'
    : scaledQty;
}

function macroChips(n) {
  return '<div class="macros">'
    + '<span class="macro">kcal <b>' + g(n.kcal) + '</b></span>'
    + '<span class="macro">eiwit <b>' + g(n.protein) + 'g</b></span>'
    + '<span class="macro">kh <b>' + g(n.carbs) + 'g</b></span>'
    + '<span class="macro">vet <b>' + g(n.fat) + 'g</b></span>'
    + '<span class="macro">vezels <b>' + g(n.fiber) + 'g</b></span>'
    + '</div>';
}

// ------------------------------------------------------------------- tabs

function selectTab(name) {
  // Scoped op #mainTabs: het database-tabblad heeft binnenin zijn eigen tabbalk
  // met dezelfde class, en die hoort geen panelen om te schakelen.
  document.querySelectorAll("#mainTabs .tab").forEach((tab) => {
    tab.setAttribute("aria-selected", String(tab.dataset.tab === name));
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.hidden = panel.id !== "panel-" + name;
  });
  if (name === "week") loadWeek();
  if (name === "database") { loadBrowse(); loadAutoStatus(); }
  // Het lege overzicht kost niets en laat meteen zien hoe de dag verdeeld is.
  if (name === "dag" && !currentDay) loadBlankDay();
}

document.querySelectorAll("#mainTabs .tab").forEach((tab) => {
  tab.onclick = () => selectTab(tab.dataset.tab);
});

// ---------------------------------------------------------------- profiel

function renderDietChoices(selected) {
  $("dietChoices").innerHTML = DIETS.map(([value, label]) =>
    '<label class="macro"><input type="checkbox" value="' + value + '"'
    + (selected.indexOf(value) >= 0 ? " checked" : "") + '> ' + label + '</label>'
  ).join("");
}

function chosenDiets() {
  return Array.from($("dietChoices").querySelectorAll("input:checked")).map((el) => el.value);
}

function renderProfileSummary(data) {
  if (!data.complete) {
    $("profileSummary").innerHTML =
      '<p class="note">Vul leeftijd, geslacht, lengte en gewicht in om je dagdoel te berekenen.</p>';
    return;
  }
  $("profileSummary").innerHTML =
    '<p class="muted" style="margin-top:12px">Rustverbranding ' + data.bmr
    + ' kcal &middot; dagverbruik ' + data.tdee + ' kcal &rarr; dagdoel:</p>'
    + macroChips(data.targets);
}

function fillProfile(data) {
  const p = data.profile;
  $("age").value = p.age === null ? "" : p.age;
  $("sex").value = p.sex || "";
  $("heightCm").value = p.heightCm === null ? "" : p.heightCm;
  $("weightKg").value = p.weightKg === null ? "" : p.weightKg;
  $("activityLevel").value = p.activityLevel;
  $("goal").value = p.goal;
  $("rateKgPerWeek").value = p.rateKgPerWeek;
  $("kcalOverride").value = p.kcalOverride === null ? "" : p.kcalOverride;
  $("proteinPerKg").value = p.proteinPerKg;
  $("fatPerKg").value = p.fatPerKg;
  renderDietChoices(p.diet || []);
  renderProfileSummary(data);
}

$("saveProfile").onclick = () => run($("saveProfile"), async () => {
  const data = await putJson("/api/profile", {
    age: num("age"), sex: $("sex").value || null,
    heightCm: num("heightCm"), weightKg: num("weightKg"),
    activityLevel: $("activityLevel").value, goal: $("goal").value,
    rateKgPerWeek: num("rateKgPerWeek"), kcalOverride: positive("kcalOverride"),
    proteinPerKg: num("proteinPerKg"), fatPerKg: num("fatPerKg"), diet: chosenDiets()
  });
  fillProfile(data);
  toast("Profiel opgeslagen.");
});

$("saveExclusions").onclick = () => run($("saveExclusions"), async () => {
  const terms = $("exclusions").value.split(",").map((t) => t.trim()).filter(Boolean);
  const data = await putJson("/api/exclusions", { terms: terms });
  $("exclusions").value = data.terms.join(", ");
  toast("Uitsluitingen opgeslagen.");
});

// ------------------------------------------------------------ eetmomenten

function slotRow(slot) {
  const row = document.createElement("div");
  row.className = "slot-row";
  row.dataset.id = slot.id || "";
  row.innerHTML =
    '<div><input class="slot-name" type="text" value="' + escapeHtml(slot.name) + '"></div>'
    + '<div><input class="slot-share" type="number" inputmode="decimal" step="0.05" min="0" value="' + slot.kcalShare + '"></div>'
    + '<div><input class="slot-enabled" type="checkbox"' + (slot.enabled ? " checked" : "") + '></div>'
    + '<button class="small del" type="button">verwijder</button>'
    // Een hard maximum wint van het berekende aandeel; leeg laten is geen limiet.
    + '<div class="slot-tags-cell"><input class="slot-max-kcal" type="number" inputmode="numeric" min="0" placeholder="max kcal, leeg = geen limiet" value="' + (slot.maxKcal || "") + '"></div>'
    + '<div class="slot-tags-cell"><input class="slot-tags" type="text" placeholder="zoekhints, bijv. kwark, havermout" value="'
    + escapeHtml((slot.tags || []).join(", ")) + '"></div>';
  row.querySelector(".del").onclick = () => row.remove();
  return row;
}

function readSlots() {
  return Array.from($("slotRows").children).map((row, index) => {
    const maxKcal = parseFloat(row.querySelector(".slot-max-kcal").value);
    return {
      id: row.dataset.id || "",
      name: row.querySelector(".slot-name").value,
      position: index,
      kcalShare: parseFloat(row.querySelector(".slot-share").value) || 0,
      enabled: row.querySelector(".slot-enabled").checked,
      // Leeg of ongeldig is geen limiet; een hard maximum wint van het aandeel.
      maxKcal: Number.isFinite(maxKcal) && maxKcal > 0 ? maxKcal : null,
      // De hints sturen waar dit moment naar zoekt: kwark in de ochtend, soep bij
      // de lunch. Leeg laten mag; dan telt alleen het macrodoel.
      tags: row.querySelector(".slot-tags").value.split(",").map((t) => t.trim()).filter(Boolean)
    };
  });
}

function renderSplit(split) {
  $("slotSplit").innerHTML = split.length
    ? split.map((entry) =>
        '<p class="muted" style="margin:10px 0 0"><strong>' + escapeHtml(entry.slot.name) + '</strong></p>'
        + macroChips(entry.targets)
      ).join("")
    : '<p class="muted" style="margin-top:10px">Vul eerst je profiel in om de verdeling in getallen te zien.</p>';
}

function renderSlots(data) {
  $("slotRows").innerHTML = "";
  data.slots.forEach((slot) => $("slotRows").appendChild(slotRow(slot)));
  renderSplit(data.split);
}

$("addSlot").onclick = () => {
  $("slotRows").appendChild(slotRow({ id: "", name: "Nieuw moment", kcalShare: 0.1, enabled: true }));
};

$("saveSlots").onclick = () => run($("saveSlots"), async () => {
  const data = await putJson("/api/slots", { slots: readSlots() });
  renderSlots(data);
  toast("Indeling opgeslagen.");
});

// ------------------------------------------------------------------- dag

function mealHead(meal) {
  return '<div class="meal-head"><span class="slot">' + escapeHtml(meal.slotName) + '</span>'
    + '<span class="muted">doel ' + g(meal.targets.kcal) + ' kcal &middot; '
    + g(meal.targets.protein) + 'g eiwit</span></div>';
}

/** Eén ingredientregel: naam links, hoeveelheid rechts. */
function ingredientRow(i) {
  const flag = i.unmatched ? ' <span class="muted">(geen voedingswaarde)</span>' : '';
  return '<div class="row"><span>' + escapeHtml(i.name) + flag + '</span>'
    + '<span class="amt">' + amountFor(i) + '</span></div>';
}

/** Een receptnaam waar je op kunt tikken voor het receptvenster. */
function recipeLink(recipeId, title) {
  return '<button class="linklike recipe-open" type="button" data-recipe="'
    + escapeHtml(recipeId) + '">' + escapeHtml(title) + '</button>';
}

function planCard(meal) {
  // Er staan keuzekaarten klaar: die krijgen voorrang op de vaste kaart, zolang
  // er nog niet definitief gekozen is.
  if (meal.options) return optionsCard(meal);

  const p = meal.plan;
  // Nog niets ingevuld: toon wél het moment en zijn doel, met een knop om juist
  // dit moment te vullen. Zo zie je hoe je dag eruitziet voordat er iets staat.
  if (!p) {
    return '<div class="card" data-slot="' + escapeHtml(meal.slotId) + '">'
      + mealHead(meal)
      + (meal.note ? '<p class="note">' + escapeHtml(meal.note) + '</p>' : '<p class="muted">Nog niet ingevuld.</p>')
      + '<div class="actions"><button class="secondary small fill" type="button">Genereer dit eetmoment</button></div>'
      + '</div>';
  }

  const rows = p.ingredients.map(ingredientRow).join("");

  const scalingNote = p.coverage < 0.8
    ? '<p class="note">Let op: voor ' + Math.round((1 - p.coverage) * 100)
      + '% van het gewicht (de gemarkeerde regels) is geen voedingswaarde gevonden, dus is de '
      + 'bijdrage van die ingredi\\u00ebnten een onderschatting.</p>'
    : "";

  // Porties per maaltijd: de doelen blijven per portie, het plan (grams en
  // totalen) is al × porties. Wijzigen roept de slot-route opnieuw aan.
  const portions = meal.portions || p.portions || 1;
  const portionsStep =
    '<div class="portions"><button class="secondary small portions-minus" type="button" title="E\\u00e9\\u00e9n portie minder">&#8722;</button>'
    + '<span class="muted">' + portions + ' portie' + (portions === 1 ? "" : "s") + '</span>'
    + '<button class="secondary small portions-plus" type="button" title="E\\u00e9\\u00e9n portie meer">&#43;</button></div>';

  return '<div class="card" data-slot="' + escapeHtml(meal.slotId) + '">'
    + mealHead(meal)
    + momentBadge(p.moment)
    + portionsStep
    + recipeLink(p.recipeId, p.title)
    + macroChips(p.perPortion)
    + rows
    + productList(p)
    + scalingNote
    + '<div class="actions">'
    + '<button class="secondary small reroll" type="button">&#128260; Ander recept</button>'
    + '<button class="secondary small fav" type="button">&#9829; Favoriet</button>'
    + '<button class="secondary small block" type="button">&#9940; Nooit meer</button>'
    + '<a class="muted" style="align-self:center" href="' + p.url + '" target="_blank" rel="noopener">Bereiding op ah.nl</a>'
    + '</div></div>';
}

/** Korte samenvatting van hoeveel er aan een plan is gesleuteld. */
function scaleSummary(plan) {
  const changed = plan.ingredients.filter((i) => Math.abs(i.scale - 1) > 0.05).length;
  return changed === 0
    ? "ongewijzigd"
    : changed + " van " + plan.ingredients.length + " ingredi\\u00ebnten aangepast";
}

/**
 * Het eetmoment van dit recept als opvallende badge boven op de kaart. De
 * server leidt het af uit AH's eigen keywords; "snack" is voor lezers een
 * "Tussendoortje". Alleen tonen als het moment bekend is.
 */
function momentBadge(moment) {
  if (!moment) return "";
  const label = moment === "snack" ? "Tussendoortje"
    : moment.charAt(0).toUpperCase() + moment.slice(1);
  return '<span class="moment-badge">' + label + '</span>';
}

/**
 * Compacte productlijst onder de kaartinfo: per regel het AH-product met zijn
 * kcal per 100 g, afgeleid uit wat de regel als geheel bijdraagt. Hooguit vijf
 * regels; de rest wordt samengevat, anders wordt de kaart een boodschappenlijst.
 * Zonder producten blijft de kaart zoals hij is.
 */
function productList(plan) {
  const lines = [];
  for (const i of plan.ingredients) {
    if (!i.productTitle) continue;
    lines.push([i.productTitle, kcalPer100g(i.grams, i.nutrients.kcal)]);
  }
  if (lines.length === 0) return "";
  const shown = lines.slice(0, 5);
  const extra = lines.length - shown.length;
  let html = '<div class="products">';
  for (const line of shown) {
    html += '<span>' + escapeHtml(line[0])
      + (line[1] !== null ? ' &mdash; ' + line[1] + ' kcal/100 g' : '')
      + '</span>';
  }
  if (extra > 0) html += '<span class="muted">+' + extra + ' meer</span>';
  return html + '</div>';
}

/**
 * Eén keuzekaart: een tik erop kiest hem meteen, dus dat is een <button>. De
 * knop om het recept eerst te bekijken staat er bewust náást en niet in: een
 * knop in een knop bestaat niet, en per ongeluk kiezen terwijl je alleen wilde
 * kijken is precies wat je niet wilt.
 */
function optionCard(option, index) {
  const p = option.plan;
  const bucketLabel = option.bucket === "origineel" ? "zoals het recept" : "aangepast aan je doel";
  return '<div class="option-wrap">'
    + '<button class="option" type="button" data-index="' + index + '">'
    + momentBadge(p.moment)
    + '<strong>' + escapeHtml(p.title) + '</strong>'
    + '<div class="macros"><span class="macro">' + bucketLabel + '</span></div>'
    + macroChips(p.perPortion)
    + productList(p)
    + '<p class="muted" style="margin:4px 0 0">' + escapeHtml(scaleSummary(p)) + '</p>'
    + '</button>'
    + '<div class="actions"><button class="secondary small recipe-open" type="button" data-recipe="'
    + escapeHtml(p.recipeId) + '">Bekijk ingredi\\u00ebnten</button></div>'
    + '</div>';
}

/** Kaart met een handvol opties in plaats van één vast plan. */
function optionsCard(meal) {
  return '<div class="card" data-slot="' + escapeHtml(meal.slotId) + '">'
    + mealHead(meal)
    + '<p class="muted">Kies er een:</p>'
    + '<div class="options">' + meal.options.map(optionCard).join("") + '</div>'
    + '<div class="actions">'
    + '<button class="secondary small more" type="button">Meer opties</button>'
    + '<button class="secondary small cancelOptions" type="button">Annuleren</button>'
    + '</div></div>';
}

function renderTotals(day) {
  const parts = ["kcal", "protein", "carbs", "fat", "fiber"].map((key) => {
    const target = day.targets[key] || 0;
    const got = day.totals[key] || 0;
    // Binnen 10% van het doel is "goed genoeg"; daarbuiten wil je het zien.
    const close = target > 0 && Math.abs(got - target) / target <= 0.1;
    return '<span class="macro ' + (close ? "good" : "over") + '">' + key
      + ' <b>' + g(got) + ' / ' + g(target) + '</b></span>';
  });
  $("dayTotals").innerHTML = '<p class="muted" style="margin-top:12px">Dagtotaal tegenover je doel:</p>'
    + '<div class="macros">' + parts.join("") + '</div>';
}

function renderDay(day) {
  currentDay = day;
  renderTotals(day);
  $("dayMeals").innerHTML = day.meals.map(planCard).join("");
  // Opslaan kan pas als er iets te bewaren valt.
  $("daySaveCard").hidden = !day.meals.some((m) => m.plan);
  bindMealButtons();
}

/** Het lege overzicht: alle eetmomenten met hun doel, nog zonder recepten. */
async function loadBlankDay() {
  try {
    const day = await api("/api/day/blank?date=" + encodeURIComponent($("dayDate").value || ""));
    renderDay(day);
  } catch (err) {
    $("dayMeals").innerHTML = '<div class="card"><p class="note">' + escapeHtml(err.message) + '</p></div>';
  }
}

function mealFor(slotId) {
  return currentDay.meals.filter((m) => m.slotId === slotId)[0];
}

function bindMealButtons() {
  document.querySelectorAll("#dayMeals .card").forEach((card) => {
    const slotId = card.dataset.slot;
    if (!slotId) return;
    const meal = mealFor(slotId);
    if (!meal) return;

    // Er staan keuzekaarten: die aan elkaar knopen, niks anders op deze kaart.
    if (meal.options) {
      card.querySelectorAll(".option").forEach((button) => {
        button.onclick = () => chooseOption(meal, Number(button.dataset.index));
      });
      card.querySelector(".more").onclick = (event) => run(event.target, () => moreOptions(meal));
      card.querySelector(".cancelOptions").onclick = () => {
        meal.options = null;
        renderDay(currentDay);
      };
      return;
    }

    // Leeg moment: één knop, om juist dit moment in te vullen.
    if (!meal.plan) {
      const fill = card.querySelector(".fill");
      if (fill) fill.onclick = (event) => run(event.target, () => fillMeal(meal));
      return;
    }

    card.querySelector(".reroll").onclick = (event) => run(event.target, () => replaceMeal(meal));

    // Porties per maaltijd: + en - roepen de slot-route opnieuw aan met het
    // nieuwe aantal, zodat het plan (grams en totalen) meeschaalt.
    const minus = card.querySelector(".portions-minus");
    if (minus) minus.onclick = (event) => run(event.target, () => setPortions(meal, portionsOf(meal) - 1));
    const plus = card.querySelector(".portions-plus");
    if (plus) plus.onclick = (event) => run(event.target, () => setPortions(meal, portionsOf(meal) + 1));

    card.querySelector(".fav").onclick = (event) => run(event.target, async () => {
      await postJson("/api/prefs", { recipeId: meal.plan.recipeId, status: "fav" });
      toast("Favoriet: " + meal.plan.title);
    });

    // Blokkeren zonder vervanging zou het geblokkeerde gerecht op het scherm
    // laten staan, dus zoek er meteen iets anders bij. Toast eerst: replaceMeal
    // toont zo meteen de keuzekaarten, en dan moet de blokkade-melding al gezien zijn.
    card.querySelector(".block").onclick = (event) => run(event.target, async () => {
      const title = meal.plan.title;
      await postJson("/api/prefs", { recipeId: meal.plan.recipeId, status: "blocked" });
      toast("Geblokkeerd: " + title + ". Komt niet meer terug.");
      await replaceMeal(meal);
    });
  });
}

/** Toont een set keuzekaarten voor dit moment; kiezen commit pas bij een tik. */
function showOptions(meal, options) {
  // Elke getoonde optie telt als "al gezien", zodat "meer opties" nooit herhaalt.
  rejected[meal.slotId] = (rejected[meal.slotId] || []).concat(options.map((o) => o.plan.recipeId));
  meal.options = options;
  renderDay(currentDay);
}

/** Eén tik = gekozen: meteen definitief, zoals het bestaande commit-patroon. */
function chooseOption(meal, index) {
  meal.plan = meal.options[index].plan;
  meal.options = null;
  meal.note = undefined;
  recomputeTotals();
  renderDay(currentDay);
}

/** Nieuwe set opties, met alles wat al getoond is uitgesloten. */
async function moreOptions(meal) {
  const data = await postJson("/api/day/reroll", {
    targets: meal.targets,
    excludeRecipeIds: (rejected[meal.slotId] || []).concat(chosenToday(meal.slotId)),
    similarTo: meal.plan ? meal.plan.perPortion : undefined,
    slotTags: meal.slotTags || [],
    kcalMode: $("dayKcalMode").value,
    optionCount: 4,
    portions: portionsOf(meal)
  });
  showOptions(meal, data.options);
}

/** Vult één leeg eetmoment in, zonder de rest van de dag aan te raken. */
async function fillMeal(meal) {
  const data = await postJson("/api/day/slot", {
    targets: meal.targets,
    excludeRecipeIds: chosenToday(meal.slotId),
    slotTags: meal.slotTags || [],
    kcalMode: $("dayKcalMode").value,
    optionCount: 4,
    portions: portionsOf(meal)
  });
  showOptions(meal, data.options);
}

/** De recepten die vandaag al ergens anders staan; die wil je niet dubbel. */
function chosenToday(exceptSlotId) {
  return currentDay.meals
    .filter((m) => m.slotId !== exceptSlotId && m.plan)
    .map((m) => m.plan.recipeId);
}

/** Zoekt andere recepten voor dit moment en toont ze als keuzekaarten. */
async function replaceMeal(meal) {
  rejected[meal.slotId] = (rejected[meal.slotId] || []).concat([meal.plan.recipeId]);
  const data = await postJson("/api/day/reroll", {
    targets: meal.targets,
    // De andere maaltijden van vandaag horen er ook niet nog een keer bij.
    excludeRecipeIds: rejected[meal.slotId].concat(chosenToday(meal.slotId)),
    similarTo: meal.plan.perPortion,
    slotTags: meal.slotTags || [],
    kcalMode: $("dayKcalMode").value,
    optionCount: 4,
    portions: portionsOf(meal)
  });
  showOptions(meal, data.options);
}

/** Na een herrolling klopt het dagtotaal niet meer; opnieuw optellen. */
function recomputeTotals() {
  const totals = { kcal: 0, protein: 0, carbs: 0, fat: 0, fiber: 0 };
  currentDay.meals.forEach((meal) => {
    if (!meal.plan) return;
    // De plan-totalen zijn al × porties; perPortion zou het dagtotaal laten
    // krimpen naar "voor 1 persoon".
    Object.keys(totals).forEach((key) => { totals[key] += meal.plan.totals[key] || 0; });
  });
  currentDay.totals = totals;
}

/** Het aantal porties dat dit moment nu heeft; staat op de maaltijd of het plan. */
function portionsOf(meal) {
  return meal.portions || (meal.plan && meal.plan.portions) || 1;
}

/**
 * Nieuw aantal porties voor dit moment. De doelen blijven per portie; het plan
 * wordt opnieuw opgehaald, zodat grams en totalen meeschalen.
 */
async function setPortions(meal, n) {
  n = Math.min(Math.max(Math.round(n) || 1, 1), 10);
  if (n === portionsOf(meal)) return;
  const data = await postJson("/api/day/slot", {
    targets: meal.targets,
    excludeRecipeIds: chosenToday(meal.slotId),
    slotTags: meal.slotTags || [],
    kcalMode: $("dayKcalMode").value,
    optionCount: 1,
    portions: n
  });
  meal.plan = data.plan;
  meal.portions = n;
  meal.options = null;
  recomputeTotals();
  renderDay(currentDay);
}

$("generateDay").onclick = () => run($("generateDay"), async () => {
  Object.keys(rejected).forEach((key) => delete rejected[key]);
  const day = await postJson("/api/day/generate", {
    date: $("dayDate").value || undefined,
    kcalMode: $("dayKcalMode").value
  });
  renderDay(day);
});

// Van datum wisselen betekent een andere dag: begin weer leeg.
$("dayDate").onchange = () => loadBlankDay();

$("saveDay").onclick = () => run($("saveDay"), async () => {
  if (!currentDay) throw new Error("genereer eerst een dag");
  await postJson("/api/day/save", { day: currentDay, name: $("dayName").value || null });
  toast("Dag opgeslagen.");
});

// ------------------------------------------------------------------ week

function dayCard(day) {
  const meals = day.meals.map((m) =>
    '<li>' + escapeHtml(m.slotName) + ': ' + escapeHtml((m.plan && m.plan.title) || m.recipeId) + '</li>'
  ).join("");
  return '<div class="day-card">'
    + '<h3>' + escapeHtml(day.date) + (day.name ? ' &middot; ' + escapeHtml(day.name) : "") + '</h3>'
    + '<p class="muted" style="margin:0 0 6px">' + g(day.totals && day.totals.kcal) + ' kcal &middot; '
    + g(day.totals && day.totals.protein) + 'g eiwit</p>'
    + '<ul>' + meals + '</ul>'
    + '<div class="actions"><button class="secondary small" type="button" data-del="' + escapeHtml(day.id) + '">verwijder</button></div>'
    + '</div>';
}

async function loadWeek() {
  try {
    const query = "?from=" + encodeURIComponent($("weekFrom").value) + "&to=" + encodeURIComponent($("weekTo").value);
    const data = await api("/api/days" + query);
    // Dezelfde dagen voeden de boodschappen-dagkeuze: één optie per dag.
    fillShopDaySelect(data.days);
    $("weekDays").innerHTML = data.days.length
      ? data.days.map(dayCard).join("")
      : '<p class="muted">Nog geen dagen opgeslagen in deze periode.</p>';
    $("weekDays").querySelectorAll("[data-del]").forEach((button) => {
      button.onclick = () => run(button, async () => {
        await api("/api/day/" + encodeURIComponent(button.dataset.del), { method: "DELETE" });
        await loadWeek();
      });
    });
  } catch (err) {
    toast(err.message, true);
  }
}

$("loadWeek").onclick = () => run($("loadWeek"), loadWeek);

// -------------------------------------------------------- boodschappenlijst

// De platte-tekstversie komt rechtstreeks uit src/plan/shopping.ts: de server
// vult de bron van die pure functie hier in, zodat er maar één implementatie
// bestaat (en die door de testsuite gedekt wordt).
const shoppingLinesToText = ${shoppingLinesToText.toString()};

// De kcal-per-100-g-afleiding voor de productregels op de kaart: zelfde
// patroon, één implementatie uit src/ui/card-info.ts, gedekt door de tests.
const kcalPer100g = ${kcalPer100g.toString()};

/** De laatst opgehaalde lijst; de kopieerknop kopieert deze. */
let shoppingLines = [];
/** Afgevinkte regels, op naam; alleen deze sessie, herladen begint opnieuw. */
const checkedShopping = new Set();

/** Kopiëren naar het klembord. navigator.clipboard werkt alleen na een echte
 * klik op een beveiligde verbinding; anders een verborgen textarea met
 * execCommand, want de app draait ook via file://. */
async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    const box = document.createElement("textarea");
    box.value = text;
    box.style.cssText = "position:fixed;left:-9999px;top:0";
    document.body.appendChild(box);
    box.select();
    let ok = false;
    try { ok = document.execCommand("copy"); } catch (e2) { ok = false; }
    box.remove();
    return ok;
  }
}

/** Toont de lijst voor deze query; de weekknop en de dagkeuze roepen dit aan. */
async function loadShopping(query) {
  const data = await api("/api/shopping" + query);
  shoppingLines = data.lines;
  if (!data.lines.length) {
    $("shoppingOut").innerHTML = '<div class="card"><p class="muted">Geen boodschappen: sla eerst een dag op.</p></div>';
    return;
  }
  const rows = data.lines.map((line) => {
    const name = line.productUrl
      ? '<a href="' + line.productUrl + '" target="_blank" rel="noopener">' + escapeHtml(line.name) + '</a>'
      : escapeHtml(line.name);
    const flag = line.unmatched ? ' <span class="muted">(controleer zelf)</span>' : '';
    // Wat je pakt: verpakkingen als die bekend zijn, anders stuks, anders gram.
    const amt = line.packagesLabel
      ? line.packagesLabel
      : line.pieces != null
        ? g(line.pieces) + " stuks"
        : g(line.grams) + " g";
    // Afvinken draait op de geëscapete naam als sleutel: dataset geeft de
    // attribuutwaarde letterlijk terug, dus de Set en de checkbox matchen.
    const key = escapeHtml(line.name);
    const checked = checkedShopping.has(key) ? " checked" : "";
    return '<div class="row shop-row' + (checked ? " checked" : "") + '">'
      + '<label class="shop-checkbox"><input class="shop-check" type="checkbox" data-key="' + key + '"' + checked + '></label>'
      + '<span>' + name + flag + '</span><span class="amt">' + escapeHtml(amt) + '</span></div>';
  }).join("");
  $("shoppingOut").innerHTML = '<div class="card"><h2>Boodschappen (' + data.days + ' dagen)</h2>' + rows + '</div>';
}

$("shoppingList").onclick = () => run($("shoppingList"), async () => {
  // De dagkeuze hoort er al te staan (het weektabblad laadt de dagen), maar
  // als iemand hier zonder dat aankomt, halen we de dagen eerst op.
  if ($("shopDay").options.length <= 1) await loadWeek();
  const query = "?from=" + encodeURIComponent($("weekFrom").value) + "&to=" + encodeURIComponent($("weekTo").value);
  await loadShopping(query);
});

/** De dagkeuze bijwerken: één optie per opgeslagen dag in deze periode. */
function fillShopDaySelect(days) {
  const current = $("shopDay").value;
  $("shopDay").innerHTML = '<option value="">Hele week</option>'
    + days.map((day) =>
        '<option value="' + escapeHtml(day.id) + '">' + escapeHtml(day.date)
        + (day.name ? ' &middot; ' + escapeHtml(day.name) : '') + '</option>'
      ).join("");
  // Staat de gekozen dag er nog in, blijf die tonen; anders terug naar week.
  $("shopDay").value = days.some((day) => day.id === current) ? current : "";
}

$("shopDay").onchange = async () => {
  try {
    // De dagkeuze laadt één dag via ?dayId=; "Hele week" herstelt de weekrange.
    const dayId = $("shopDay").value;
    const query = dayId
      ? "?dayId=" + encodeURIComponent(dayId)
      : "?from=" + encodeURIComponent($("weekFrom").value) + "&to=" + encodeURIComponent($("weekTo").value);
    await loadShopping(query);
  } catch (err) {
    toast(err.message, true);
  }
};

// Eén gedelegeerde listener op #shoppingOut, net als bij #browseOut: de rijen
// worden steeds via innerHTML vervangen, maar de container blijft staan.
$("shoppingOut").addEventListener("change", (event) => {
  const box = event.target.closest(".shop-check");
  if (!box) return;
  const row = box.closest(".shop-row");
  if (box.checked) checkedShopping.add(box.dataset.key);
  else checkedShopping.delete(box.dataset.key);
  row.classList.toggle("checked", box.checked);
});

$("shop-copy").onclick = () => run($("shop-copy"), async () => {
  if (!shoppingLines.length) throw new Error("Genereer eerst de lijst.");
  const text = shoppingLinesToText(shoppingLines);
  if (!(await copyText(text))) throw new Error("Kopiëren mocht niet automatisch; probeer de lijst handmatig te selecteren.");
  toast("Lijst gekopieerd (" + shoppingLines.length + " regels).");
});

// ------------------------------------------------------- receptvenster

/**
 * Alles wat er over één recept te zeggen valt: de ingredienten met het
 * AH-product dat eraan hangt, wat elke regel bijdraagt, en de voedingswaarde per
 * portie én voor het hele gerecht.
 *
 * De knoppen die dit openen staan overal (dagkaart, keuzekaart, database), dus
 * dit luistert op documentniveau in plaats van per kaart opnieuw gebonden te
 * worden — dan blijft het ook werken na een hertekening.
 */
document.addEventListener("click", (event) => {
  const button = event.target.closest ? event.target.closest("[data-recipe]") : null;
  if (!button) return;
  event.preventDefault();
  openRecipe(button.dataset.recipe);
});

$("recipeClose").onclick = () => $("recipeDialog").close();

async function openRecipe(id) {
  const dialog = $("recipeDialog");
  $("recipeBody").innerHTML = '<p class="muted">Bezig\\u2026</p>';
  if (!dialog.open) dialog.showModal();
  try {
    $("recipeBody").innerHTML = recipeDetail(await api("/api/recipe/" + encodeURIComponent(id)));
  } catch (err) {
    $("recipeBody").innerHTML = '<p class="note">' + escapeHtml(err.message) + '</p>';
  }
}

/** De hoeveelheid zoals het recept hem opschreef, plus de grammen erachter. */
function detailAmount(i) {
  const suffix = unitSuffix(i.gramsSource, i.unit);
  const own = i.quantity !== null && suffix ? formatQty(i.quantity) + " " + suffix : null;
  // Stond het recept al in grammen, dan is het herhalen ervan alleen ruis.
  if (!own || i.gramsSource === "explicit") return g(i.grams) + " g";
  return own + ' <span class="muted">(' + g(i.grams) + ' g)</span>';
}

function recipeDetail(data) {
  const r = data.recipe;
  const rows = data.ingredients.map((i) => {
    const flag = i.nutrientSource === "geschat"
      ? ' <span class="warnish">(geschat)</span>'
      : '';
    const kcal = i.nutrients && i.nutrients.kcal !== undefined
      ? g(i.nutrients.kcal) + " kcal"
      : '<span class="warnish">geen voedingswaarde</span>';
    // Het echte product erachter: klikbare link naar ah.nl, de verpakking en de
    // voedingswaarde per 100 g rechtstreeks uit het productlabel.
    const product = i.productUrl
      ? '<br><a href="' + i.productUrl + '" target="_blank" rel="noopener">'
        + escapeHtml(i.product) + '</a>'
        + (i.productSize ? ' <span class="muted">' + escapeHtml(i.productSize) + '</span>' : '')
        + (i.per100g && i.per100g.kcal !== undefined
          ? ' <span class="muted">(' + g(i.per100g.kcal) + ' kcal / 100 g)</span>'
          : '')
      : '';
    return '<div class="row"><span>' + escapeHtml(i.name) + flag + '</span>'
      + '<span class="amt">' + detailAmount(i) + '<br>' + kcal + product + '</span></div>';
  }).join("");

  // Waar de cijfers vandaan komen hoort erbij: het totaal is van AH, de
  // verdeling over de regels is naar gewicht toegerekend en dus een schatting.
  const bron = data.nutritionSource === "ah"
    ? '<p class="muted">Voedingswaarde volgens AH. Per portie klopt het totaal; de kcal per '
      + 'ingredi\u00ebnt is het aandeel naar gewicht, dus een schatting.</p>'
    : '<p class="muted">AH geeft bij dit recept geen voedingswaarde.</p>';

  return '<h2>' + escapeHtml(r.title) + '</h2>'
    + '<p class="muted">' + plural(r.servings || data.servings, "portie", "porties") + ' &middot; '
    + plural(data.ingredients.length, "ingredient", "ingredienten") + '</p>'
    + '<p class="muted" style="margin-top:10px">Per portie:</p>'
    + macroChips(data.perPortion)
    + '<p class="muted">Hele recept:</p>'
    + macroChips(data.total)
    + rows
    + bron
    + '<div class="actions"><a href="' + r.url + '" target="_blank" rel="noopener">Bereiding op ah.nl</a></div>';
}

// ---------------------------------------------------------- database

const browse = { kind: "recipes", offset: 0, limit: 50, total: 0 };

const when = (ms) => (ms ? new Date(ms).toLocaleDateString("nl-NL") : "-");
// Onder een kilobyte afronden zou elke kleine payload "0 kB" maken.
const kib = (bytes) => (bytes < 1024 ? bytes + " B" : Math.round(bytes / 1024) + " kB");
const plural = (n, one, many) => n + " " + (n === 1 ? one : many);

function browseRecipe(r) {
  // Alles in de database is compleet doorgerekend, dus er is geen "nog niet"-geval.
  const macros = r.nutrition
    ? g(r.nutrition.kcal) + ' kcal &middot; ' + g(r.nutrition.protein) + 'g eiwit'
    : '';
  const flag = r.prefStatus === "fav" ? ' &#9829;' : (r.prefStatus === "blocked" ? ' &#9940;' : '');
  const tags = r.tags.length ? r.tags.map((t) => '<span class="macro">' + escapeHtml(t) + '</span>').join("") : '<span class="macro">geen labels</span>';
  return '<div class="card">'
    + recipeLink(r.id, r.title) + flag
    + '<p class="muted" style="margin:4px 0">' + macros
    + ' &middot; <a href="' + r.url + '" target="_blank" rel="noopener">op ah.nl</a></p>'
    + '<div class="macros">' + tags + '</div>'
    + '<p class="muted" style="margin:4px 0">' + plural(r.ingredientCount, "ingredient", "ingredienten")
    + ' &middot; ' + plural(r.servings, "portie", "porties")
    + ' &middot; opgehaald ' + when(r.fetchedAt) + '</p>'
    + '</div>';
}

function browseProduct(p) {
  const n = p.per100g || {};
  const known = Object.keys(n).length > 0;
  return '<div class="card">'
    + '<strong><a href="https://www.ah.nl/producten/product/wi' + encodeURIComponent(p.webshopId)
    + '" target="_blank" rel="noopener">' + escapeHtml(p.title) + '</a></strong>'
    + '<p class="muted" style="margin:4px 0">' + escapeHtml(p.salesUnitSize || "onbekende verpakking") + '</p>'
    + (known ? macroChips(n) : '<p class="note">geen voedingswaarde bekend</p>')
    + '<p class="muted" style="margin:4px 0">gekoppeld aan ' + plural(p.matchCount, "ingredient", "ingredienten")
    + (p.matchedNames ? ': ' + escapeHtml(p.matchedNames) : '') + '</p>'
    + '</div>';
}

function browseMatch(m) {
  const target = m.webshopId
    ? '<a href="https://www.ah.nl/producten/product/wi' + encodeURIComponent(m.webshopId)
      + '" target="_blank" rel="noopener">' + escapeHtml(m.productTitle || m.webshopId) + '</a>'
    : '<span class="warnish">geen product gevonden</span>';
  const name = encodeURIComponent(m.ingredientName);
  return '<div class="row"><span>' + escapeHtml(m.ingredientName) + ' &rarr; ' + target + '</span>'
    + '<span class="amt">' + (m.webshopId ? Math.round(m.score * 100) + '% ' : '')
    + '<button class="small secondary match-correct" type="button" data-ingredient="' + name + '">corrigeren</button>'
    + '</span></div>'
    + '<div class="match-fix" data-ingredient="' + name + '" hidden>'
    + '<div class="actions">'
    + '<input type="search" class="match-fix-q" placeholder="zoek een product&hellip;">'
    + '<button class="small secondary match-fix-search" type="button">Zoek</button>'
    + '</div>'
    + '<div class="match-fix-results"></div>'
    + '</div>';
}

function browseScrape(s) {
  const state = s.parsedOk
    ? '<span class="ok">geparsed</span>'
    : '<span class="warnish">' + escapeHtml(s.parseError || "nog niet geparsed") + '</span>';
  return '<div class="row"><span>'
    + '<a href="/api/browse/raw/' + encodeURIComponent(s.id) + '" target="_blank" rel="noopener">'
    + escapeHtml(s.kind) + ' &middot; ' + escapeHtml(s.ref) + '</a><br>'
    + '<span class="muted">HTTP ' + s.status + ' &middot; ' + kib(s.bodyBytes) + ' &middot; ' + when(s.scrapedAt)
    + ' &middot; ' + state + '</span></span></div>';
}

async function loadBrowse() {
  try {
    const params = "?q=" + encodeURIComponent($("browseQuery").value)
      + "&limit=" + browse.limit + "&offset=" + browse.offset;
    const data = await api("/api/browse/" + browse.kind + params);
    browse.total = data.total;

    const from = data.total === 0 ? 0 : browse.offset + 1;
    $("browseCount").textContent = from + "-" + (browse.offset + data.rows.length) + " van " + data.total;
    $("browsePrev").disabled = browse.offset === 0;
    $("browseNext").disabled = browse.offset + browse.limit >= data.total;

    if (data.rows.length === 0) {
      $("browseOut").innerHTML = '<div class="card"><p class="muted">Niets gevonden.</p></div>';
      return;
    }
    // Recepten en producten krijgen een kaart per stuk; de andere twee zijn
    // lijsten waarvan je er veel tegelijk wilt kunnen scannen.
    if (browse.kind === "recipes") $("browseOut").innerHTML = data.rows.map(browseRecipe).join("");
    else if (browse.kind === "products") $("browseOut").innerHTML = data.rows.map(browseProduct).join("");
    else if (browse.kind === "matches") $("browseOut").innerHTML = '<div class="card">' + data.rows.map(browseMatch).join("") + '</div>';
    else $("browseOut").innerHTML = '<div class="card">' + data.rows.map(browseScrape).join("") + '</div>';
  } catch (err) {
    toast(err.message, true);
  }
}

document.querySelectorAll(".browse-kind").forEach((button) => {
  button.onclick = () => {
    browse.kind = button.dataset.kind;
    browse.offset = 0;
    document.querySelectorAll(".browse-kind").forEach((b) => {
      b.setAttribute("aria-selected", String(b === button));
    });
    loadBrowse();
  };
});

/** Eén product-kandidaat, aantikbaar om als koppeling te kiezen. */
function matchPickRow(product, ingredientAttr) {
  const size = product.salesUnitSize
    ? ' <span class="muted">(' + escapeHtml(product.salesUnitSize) + ')</span>'
    : '';
  return '<div class="row"><span>' + escapeHtml(product.title) + size + '</span>'
    + '<button class="small secondary match-pick" type="button" data-ingredient="' + ingredientAttr
    + '" data-webshop="' + escapeHtml(String(product.webshopId)) + '">kies</button></div>';
}

// Eén gedelegeerde listener op #browseOut: de rijen erin worden steeds
// vervangen via innerHTML, maar #browseOut zelf niet, dus hoeft dit niet na
// elke herlaadbeurt opnieuw aangehaakt te worden.
$("browseOut").addEventListener("click", (event) => {
  const correct = event.target.closest(".match-correct");
  if (correct) {
    const panel = document.querySelector('.match-fix[data-ingredient="' + correct.dataset.ingredient + '"]');
    const opening = panel.hidden;
    document.querySelectorAll(".match-fix").forEach((p) => { p.hidden = true; });
    panel.hidden = !opening;
    return;
  }

  const search = event.target.closest(".match-fix-search");
  if (search) {
    run(search, async () => {
      const panel = search.closest(".match-fix");
      const q = panel.querySelector(".match-fix-q").value.trim();
      const results = panel.querySelector(".match-fix-results");
      if (!q) { results.innerHTML = ""; return; }
      const data = await api("/api/products/search?q=" + encodeURIComponent(q));
      results.innerHTML = data.products.length
        ? data.products.map((p) => matchPickRow(p, panel.dataset.ingredient)).join("")
        : '<p class="muted">Niets gevonden.</p>';
    });
    return;
  }

  const pick = event.target.closest(".match-pick");
  if (pick) {
    run(pick, async () => {
      const ingredient = decodeURIComponent(pick.dataset.ingredient);
      await postJson("/api/match", { ingredient: ingredient, webshopId: pick.dataset.webshop });
      toast("Gekoppeld: " + ingredient);
      loadBrowse();
    });
  }
});

let browseTimer = null;
$("browseQuery").oninput = () => {
  // Wachten tot je uitgetypt bent; anders vuurt elke toetsaanslag een query af.
  clearTimeout(browseTimer);
  browseTimer = setTimeout(() => { browse.offset = 0; loadBrowse(); }, 300);
};

$("browsePrev").onclick = () => {
  browse.offset = Math.max(0, browse.offset - browse.limit);
  loadBrowse();
};
$("browseNext").onclick = () => {
  if (browse.offset + browse.limit < browse.total) {
    browse.offset += browse.limit;
    loadBrowse();
  }
};

// --------------------------------------------------------------- beheer

$("ingest").onclick = () => run($("ingest"), async () => {
  const r = await postJson("/api/ingest", {});
  toast(r.added + " recepten toegevoegd" + (r.errors.length ? ", " + r.errors.length + " fouten" : "."));
  loadStats();
});

$("probe").onclick = () => run($("probe"), async () => {
  const r = await api("/api/probe");
  $("stats").textContent = Object.keys(r).map((k) => k + ": " + r[k]).join(" | ");
});

// Scrapen gaat bewust langzaam (AH blokkeert bursts), dus dit duurt even.
document.querySelectorAll(".moment").forEach((button) => {
  button.onclick = () => run(button, async () => {
    const r = await postJson("/api/ingest", { moment: button.dataset.moment, limit: 20 });
    const geskipt = r.skipped ? ", " + r.skipped + " vielen af (ander eetmoment)" : "";
    toast(r.added + " recepten voor " + button.dataset.moment + geskipt + ".");
    loadStats();
  });
});

$("reparse").onclick = () => run($("reparse"), async () => {
  const r = await postJson("/api/reparse", {});
  toast(r.recovered + " van " + r.examined + " recepten hersteld uit het archief.");
  loadStats();
});

function renderAutoStatus(s) {
  const v = s.vandaag || {};
  const binnen = (v.added || 0) + (v.repaired || 0);
  // De schakelaar volgt de stand: staat het bijvullen uit, dan is de knop de
  // manier om het weer aan te zetten.
  $("autoPause").textContent = s.gepauzeerd ? "Bijvullen aanzetten" : "Bijvullen uitzetten";
  const chips = '<div class="macros">'
    + '<span class="macro">vandaag <b>' + binnen + ' / ' + s.dagbudget + '</b></span>'
    + '<span class="macro">rondes <b>' + (v.runs || 0) + '</b></span>'
    + '<span class="macro">recepten <b>' + (s.recepten || 0) + '</b></span>'
    + '<span class="macro">afgekeurd <b>' + (s.afgekeurd || 0) + '</b></span>'
    + '<span class="macro">hierna <b>' + escapeHtml(s.volgende || "-") + '</b></span>'
    + '</div>';

  // Afkoelen is geen storing maar precies de bedoeling; alleen even melden.
  // Bij meerdere blokkades op rij loopt de afkoelperiode op, dus dat melden we
  // erbij — anders lijkt het alsof hij na elke 30 minuten weer vastloopt.
  const streak = s.blokkadesOpEenRij || 0;
  const streakNote = streak > 1
    ? ' AH blokkeerde ons ' + streak + 'x op rij, dus de afkoelperiode loopt nu op.'
    : "";
  const uit = s.gepauzeerd
    ? '<p class="note">Automatisch bijvullen staat uit. De database blijft zoals hij nu is.</p>'
    : "";
  const koelt = s.afkoelenTot
    ? '<p class="note">AH blokkeerde ons even. Het bijvullen ligt stil tot '
      + new Date(s.afkoelenTot).toLocaleTimeString("nl-NL", { hour: "2-digit", minute: "2-digit" })
      + '.' + streakNote + '</p>'
    : "";

  const laatste = (s.rondes || []).slice(0, 5).map((r) => {
    const wat = r.added + " opgeslagen"
      + (r.repaired ? ", " + r.repaired + " afgekeurd" : "");
    const geblokkeerd = r.blocked ? ' <span class="warnish">(' + r.blocked + 'x geblokkeerd)</span>' : "";
    const tijd = new Date(r.started_at).toLocaleTimeString("nl-NL", { hour: "2-digit", minute: "2-digit" });
    return '<div class="row"><span>' + tijd + ' &middot; ' + escapeHtml(r.detail || r.mode) + '</span>'
      + '<span class="amt">' + wat + geblokkeerd + '</span></div>';
  }).join("");

  $("autoStatus").innerHTML = chips + uit + koelt
    + (laatste ? '<p class="muted" style="margin:10px 0 0">Laatste rondes:</p>' + laatste : "");
}

function loadAutoStatus() {
  api("/api/auto/status").then(renderAutoStatus).catch(() => {});
}

function loadAutoLogs() {
  api("/api/browse/scrapes?limit=20").then((data) => {
    $("autoLogs").innerHTML = data.rows.length
      ? data.rows.map(browseScrape).join("")
      : '<p class="muted">Nog geen verzoeken geweest.</p>';
  }).catch(() => {});
}

$("autoLogsBox").addEventListener("toggle", () => {
  if ($("autoLogsBox").open) { loadAutoLogs(); loadAppLogs(); }
});

// ---------------------------------------------------------------- app-log

/** Eén logregel: tijd, herkomst, bericht en de losse details erachter. */
function logLine(r) {
  const at = new Date(r.at).toLocaleTimeString("nl-NL", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const detail = r.detail ? ' <span class="det">' + escapeHtml(r.detail) + '</span>' : "";
  return '<div class="logline ' + escapeHtml(r.level) + '">'
    + '<span class="at">' + at + '</span>'
    + '<span class="src">' + escapeHtml(r.source) + '</span>'
    + '<span class="msg">' + escapeHtml(r.message) + detail + '</span>'
    + '</div>';
}

function loadAppLogs() {
  const level = $("logLevel").value;
  return api("/api/logs?limit=200" + (level ? "&level=" + level : "")).then((data) => {
    $("appLogs").innerHTML = data.rows.length
      ? data.rows.map(logLine).join("")
      : '<p class="muted">Nog niets gelogd.</p>';
  }).catch(() => {});
}

$("logLevel").onchange = loadAppLogs;
$("logRefresh").onclick = () => run($("logRefresh"), loadAppLogs);

$("logClear").onclick = () => run($("logClear"), async () => {
  await postJson("/api/logs/clear", {});
  toast("Log leeggemaakt.");
  loadAppLogs();
});

/**
 * De hele log als platte tekst op het klembord. navigator.clipboard bestaat
 * alleen op een beveiligde verbinding en na een echte klik; lukt het niet, dan
 * valt dit terug op een selecteerbaar tekstvak zodat kopiëren altijd kán.
 */
$("logCopy").onclick = () => run($("logCopy"), async () => {
  const res = await fetch("/api/logs?limit=1000&format=text");
  const text = await res.text();
  try {
    await navigator.clipboard.writeText(text);
    toast("Log gekopieerd (" + text.split("\\n").length + " regels).");
  } catch (err) {
    const box = document.createElement("textarea");
    box.value = text;
    box.style.cssText = "width:100%;height:180px;margin-top:10px";
    $("appLogs").prepend(box);
    box.focus();
    box.select();
    toast("Kopiëren mocht niet automatisch; de tekst staat geselecteerd klaar.", true);
  }
});

// ------------------------------------------------------------- alles wissen

/** Wissen is onomkeerbaar, dus twee keer vragen: een bevestiging en het woord. */
async function wipeWithConfirm(button, scope) {
  const wat = scope === "alles"
    ? "ALLES, inclusief je profiel, eetmomenten en opgeslagen dagen"
    : "alle gescrapete recepten, producten, koppelingen en het scrape-archief (je profiel en opgeslagen dagen blijven staan)";
  if (!confirm("Dit verwijdert " + wat + ". Doorgaan?")) return;
  if (prompt('Typ WISSEN om te bevestigen.') !== "WISSEN") { toast("Geannuleerd."); return; }

  await run(button, async () => {
    const r = await postJson("/api/wipe", { scope });
    // Het bijvullen gaat automatisch uit: de cron draait elke twee minuten, dus
    // anders staat de database binnen een paar tellen weer vol en lijkt het
    // alsof het wissen niet werkte.
    toast(r.total + " rijen verwijderd. Automatisch bijvullen staat nu uit.");
    loadStats();
    loadAutoStatus();
    loadAppLogs();
    loadBrowse();
  });
}

$("wipe").onclick = () => wipeWithConfirm($("wipe"), "scrape");
$("wipeAll").onclick = () => wipeWithConfirm($("wipeAll"), "alles");


$("autoPause").onclick = () => run($("autoPause"), async () => {
  const status = await api("/api/auto/status");
  const r = await postJson("/api/auto/pause", { paused: !status.gepauzeerd });
  toast(r.paused ? "Automatisch bijvullen staat uit." : "Automatisch bijvullen loopt weer.");
  loadAutoStatus();
});

$("autoRun").onclick = () => run($("autoRun"), async () => {
  // Handmatig op de knop drukken is een bewuste keuze om nu te kijken wat AH
  // doet, dus die mag de afkoelperiode en het dagbudget overslaan.
  const r = await postJson("/api/auto/run", { force: true });
  if (!r.ran) { toast("Overgeslagen: " + r.reason + "."); loadAutoStatus(); return; }
  toast(r.added + " recepten opgeslagen, " + (r.rejected || 0) + " afgekeurd (" + r.detail + ").");
  loadAutoStatus();
  loadStats();
  if ($("autoLogsBox").open) { loadAutoLogs(); loadAppLogs(); }
});

function loadStats() {
  api("/api/stats").then((s) => {
    const afgekeurd = s.afgekeurd ? " &middot; " + s.afgekeurd + " afgekeurd" : "";
    $("stats").innerHTML = s.recipes + " recepten" + afgekeurd
      + " &middot; " + s.scrapes + " scrapes bewaard";
  }).catch(() => {});
}

// ----------------------------------------------------------------- start

function isoDate(offsetDays) {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

$("dayDate").value = isoDate(0);
$("weekTo").value = isoDate(0);
$("weekFrom").value = isoDate(-6);

api("/api/profile").then(fillProfile).catch(() => {});
api("/api/slots").then(renderSlots).catch(() => {});
api("/api/exclusions").then((d) => { $("exclusions").value = d.terms.join(", "); }).catch(() => {});
loadStats();
`;
