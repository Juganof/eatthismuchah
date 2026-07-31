/**
 * De clientcode. Bewust gewone globale JavaScript zonder modules of buildstap,
 * in dezelfde stijl als de rest van het project.
 *
 * Let op bij het aanpassen: dit is een TypeScript-template-string, dus geen
 * backticks en geen dollar-accolades in de JavaScript hieronder — vandaar dat
 * alles met stringoptelling is geschreven.
 */
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
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.setAttribute("aria-selected", String(tab.dataset.tab === name));
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.hidden = panel.id !== "panel-" + name;
  });
  if (name === "week") loadWeek();
}

document.querySelectorAll(".tab").forEach((tab) => {
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
    + '<div class="slot-tags-cell"><input class="slot-tags" type="text" placeholder="zoekhints, bijv. kwark, havermout" value="'
    + escapeHtml((slot.tags || []).join(", ")) + '"></div>';
  row.querySelector(".del").onclick = () => row.remove();
  return row;
}

function readSlots() {
  return Array.from($("slotRows").children).map((row, index) => ({
    id: row.dataset.id || "",
    name: row.querySelector(".slot-name").value,
    position: index,
    kcalShare: parseFloat(row.querySelector(".slot-share").value) || 0,
    enabled: row.querySelector(".slot-enabled").checked,
    // De hints sturen waar dit moment naar zoekt: kwark in de ochtend, soep bij
    // de lunch. Leeg laten mag; dan telt alleen het macrodoel.
    tags: row.querySelector(".slot-tags").value.split(",").map((t) => t.trim()).filter(Boolean)
  }));
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

function planCard(meal) {
  const p = meal.plan;
  if (!p) {
    return '<div class="card"><div class="meal-head"><span class="slot">' + escapeHtml(meal.slotName) + '</span></div>'
      + '<p class="note">' + escapeHtml(meal.note || "geen recept gevonden") + '</p></div>';
  }

  const rows = p.ingredients.map((i) => {
    // Alleen hoeveelheden benoemen die de solver echt heeft verschoven.
    const changed = Math.abs(i.scale - 1) > 0.05;
    const amount = changed
      ? '<span class="chg">' + i.grams + ' g</span> <s>' + i.originalGrams + ' g</s>'
      : i.grams + ' g';
    const flag = i.unmatched ? ' <span class="muted">(geen voedingswaarde)</span>' : '';
    return '<div class="row"><span>' + escapeHtml(i.name) + flag + '</span><span class="amt">' + amount + '</span></div>';
  }).join("");

  const lowCoverage = p.coverage < 0.8
    ? '<p class="note">Let op: voor ' + Math.round((1 - p.coverage) * 100)
      + '% van het gewicht is geen voedingswaarde gevonden, de totalen zijn dus een onderschatting.</p>'
    : "";

  return '<div class="card" data-slot="' + escapeHtml(meal.slotId) + '">'
    + '<div class="meal-head"><span class="slot">' + escapeHtml(meal.slotName) + '</span>'
    + '<span class="muted">doel ' + g(meal.targets.kcal) + ' kcal &middot; ' + g(meal.targets.protein) + 'g eiwit</span></div>'
    + '<strong>' + escapeHtml(p.title) + '</strong>'
    + macroChips(p.perPortion)
    + rows
    + lowCoverage
    + '<div class="actions">'
    + '<button class="secondary small reroll" type="button">&#128260; Ander recept</button>'
    + '<button class="secondary small fav" type="button">&#9829; Favoriet</button>'
    + '<button class="secondary small block" type="button">&#9940; Nooit meer</button>'
    + '<a class="muted" style="align-self:center" href="' + p.url + '" target="_blank" rel="noopener">Bereiding op ah.nl</a>'
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
  $("daySaveCard").hidden = false;
  bindMealButtons();
}

function mealFor(slotId) {
  return currentDay.meals.filter((m) => m.slotId === slotId)[0];
}

function bindMealButtons() {
  document.querySelectorAll("#dayMeals .card").forEach((card) => {
    const slotId = card.dataset.slot;
    if (!slotId) return;
    const meal = mealFor(slotId);
    if (!meal || !meal.plan) return;

    card.querySelector(".reroll").onclick = (event) => run(event.target, () => replaceMeal(meal));

    card.querySelector(".fav").onclick = (event) => run(event.target, async () => {
      await postJson("/api/prefs", { recipeId: meal.plan.recipeId, status: "fav" });
      toast("Favoriet: " + meal.plan.title);
    });

    // Blokkeren zonder vervanging zou het geblokkeerde gerecht op het scherm
    // laten staan, dus zoek er meteen iets anders bij.
    card.querySelector(".block").onclick = (event) => run(event.target, async () => {
      const title = meal.plan.title;
      await postJson("/api/prefs", { recipeId: meal.plan.recipeId, status: "blocked" });
      await replaceMeal(meal);
      toast("Geblokkeerd: " + title + ". Komt niet meer terug.");
    });
  });
}

/** Zoekt een ander recept voor dit moment en zet het op het scherm. */
async function replaceMeal(meal) {
  rejected[meal.slotId] = (rejected[meal.slotId] || []).concat([meal.plan.recipeId]);
  // De andere maaltijden van vandaag horen er ook niet nog een keer bij.
  const alsoToday = currentDay.meals
    .filter((m) => m.slotId !== meal.slotId && m.plan)
    .map((m) => m.plan.recipeId);
  const data = await postJson("/api/day/reroll", {
    targets: meal.targets,
    excludeRecipeIds: rejected[meal.slotId].concat(alsoToday),
    similarTo: meal.plan.perPortion,
    slotTags: meal.slotTags || [],
    kcalMode: $("dayKcalMode").value
  });
  meal.plan = data.plan;
  recomputeTotals();
  renderDay(currentDay);
}

/** Na een herrolling klopt het dagtotaal niet meer; opnieuw optellen. */
function recomputeTotals() {
  const totals = { kcal: 0, protein: 0, carbs: 0, fat: 0, fiber: 0 };
  currentDay.meals.forEach((meal) => {
    if (!meal.plan) return;
    Object.keys(totals).forEach((key) => { totals[key] += meal.plan.perPortion[key] || 0; });
  });
  currentDay.totals = totals;
}

$("generateDay").onclick = () => run($("generateDay"), async () => {
  Object.keys(rejected).forEach((key) => delete rejected[key]);
  const day = await postJson("/api/day/generate", {
    date: $("dayDate").value || undefined,
    kcalMode: $("dayKcalMode").value
  });
  renderDay(day);
});

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

$("shoppingList").onclick = () => run($("shoppingList"), async () => {
  const query = "?from=" + encodeURIComponent($("weekFrom").value) + "&to=" + encodeURIComponent($("weekTo").value);
  const data = await api("/api/shopping" + query);
  if (!data.lines.length) {
    $("shoppingOut").innerHTML = '<div class="card"><p class="muted">Geen boodschappen: sla eerst een dag op.</p></div>';
    return;
  }
  const rows = data.lines.map((line) => {
    const name = line.productUrl
      ? '<a href="' + line.productUrl + '" target="_blank" rel="noopener">' + escapeHtml(line.name) + '</a>'
      : escapeHtml(line.name);
    const flag = line.unmatched ? ' <span class="muted">(controleer zelf)</span>' : '';
    return '<div class="row"><span>' + name + flag + '</span><span class="amt">' + line.grams + ' g</span></div>';
  }).join("");
  $("shoppingOut").innerHTML = '<div class="card"><h2>Boodschappen (' + data.days + ' dagen)</h2>' + rows + '</div>';
});

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

$("reparse").onclick = () => run($("reparse"), async () => {
  const r = await postJson("/api/reparse", {});
  toast(r.recovered + " van " + r.examined + " recepten hersteld uit het archief.");
  loadStats();
});

function loadStats() {
  api("/api/stats").then((s) => {
    $("stats").textContent = s.recipes + " recepten (" + s.plannable + " bruikbaar) &middot; "
      + s.scrapes + " scrapes bewaard, " + s.unparsed + " nog niet geparsed";
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
