/**
 * The whole front-end: one self-contained page, sized for a phone browser.
 * No build step and no external requests, so it loads instantly over mobile data.
 */
export function renderPage(): string {
  return `<!doctype html>
<html lang="nl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b1120">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>AH Macro Planner</title>
<style>
  :root {
    --bg: #f6f7f9; --card: #fff; --text: #10151f; --muted: #5b6472;
    --line: #e3e7ed; --accent: #0071ce; --ok: #15803d; --warn: #b45309;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0b1120; --card: #151c2c; --text: #e8ecf4; --muted: #94a0b4;
      --line: #263149; --accent: #4da3ff; --ok: #4ade80; --warn: #fbbf24;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 16px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    padding: max(12px, env(safe-area-inset-top)) 12px max(24px, env(safe-area-inset-bottom));
    -webkit-text-size-adjust: 100%;
  }
  main { max-width: 640px; margin: 0 auto; }
  h1 { font-size: 1.35rem; margin: 4px 0 2px; }
  .sub { color: var(--muted); font-size: .85rem; margin: 0 0 16px; }
  .card {
    background: var(--card); border: 1px solid var(--line); border-radius: 14px;
    padding: 14px; margin-bottom: 12px;
  }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  label { display: block; font-size: .78rem; color: var(--muted); margin-bottom: 4px; }
  input, select {
    width: 100%; padding: 11px 12px; border: 1px solid var(--line); border-radius: 10px;
    background: var(--bg); color: var(--text); font-size: 16px; /* 16px stops iOS zoom */
  }
  button {
    width: 100%; padding: 13px; border: 0; border-radius: 10px; background: var(--accent);
    color: #fff; font-size: 1rem; font-weight: 600; margin-top: 12px;
  }
  button:disabled { opacity: .55; }
  button.secondary { background: transparent; color: var(--accent); border: 1px solid var(--line); }
  .row { display: flex; justify-content: space-between; gap: 8px; padding: 7px 0; border-bottom: 1px solid var(--line); }
  .row:last-child { border-bottom: 0; }
  .row .amt { white-space: nowrap; font-variant-numeric: tabular-nums; color: var(--muted); }
  .chg { color: var(--accent); font-weight: 600; }
  .macros { display: flex; flex-wrap: wrap; gap: 6px; margin: 10px 0; }
  .macro {
    background: var(--bg); border: 1px solid var(--line); border-radius: 999px;
    padding: 4px 10px; font-size: .8rem; font-variant-numeric: tabular-nums;
  }
  .macro b { font-weight: 700; }
  .note { font-size: .78rem; color: var(--warn); margin-top: 8px; }
  .muted { color: var(--muted); font-size: .8rem; }
  a { color: var(--accent); }
  details summary { cursor: pointer; font-size: .85rem; color: var(--muted); }
</style>
</head>
<body>
<main>
  <h1>AH Macro Planner</h1>
  <p class="sub">Allerhande-recepten, herberekend naar jouw macro's.</p>

  <div class="card">
    <div class="grid">
      <div><label for="protein">Eiwit (g)</label><input id="protein" type="number" inputmode="numeric" value="60"></div>
      <div><label for="kcal">Calorie&euml;n</label><input id="kcal" type="number" inputmode="numeric" value="700"></div>
      <div><label for="carbs">Koolhydraten (g)</label><input id="carbs" type="number" inputmode="numeric" placeholder="optioneel"></div>
      <div><label for="fat">Vet (g)</label><input id="fat" type="number" inputmode="numeric" placeholder="optioneel"></div>
      <div><label for="portions">Porties</label><input id="portions" type="number" inputmode="numeric" value="1" min="1"></div>
      <div>
        <label for="kcalMode">Calorie&euml;n als</label>
        <select id="kcalMode"><option value="target">exact doel</option><option value="max">maximum</option></select>
      </div>
    </div>
    <button id="go">Recepten genereren</button>
    <details style="margin-top:12px">
      <summary>Beheer</summary>
      <button class="secondary" id="ingest">Nieuwe recepten ophalen</button>
      <button class="secondary" id="probe">AH-verbinding testen</button>
      <p class="muted" id="stats"></p>
    </details>
  </div>

  <div id="out"></div>
</main>
<script>
const $ = (id) => document.getElementById(id);
const numOrUndef = (id) => { const v = parseFloat($(id).value); return Number.isFinite(v) && v > 0 ? v : undefined; };
const g = (n) => n === undefined || n === null ? "?" : Math.round(n);

async function api(path, options) {
  const res = await fetch(path, options);
  const body = await res.json().catch(() => ({ error: "geen geldige respons" }));
  if (!res.ok) throw new Error(body.error || ("HTTP " + res.status));
  return body;
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

function renderPlan(p) {
  const rows = p.ingredients.map((i) => {
    // Only call out amounts the solver actually moved.
    const changed = Math.abs(i.scale - 1) > 0.05;
    const amount = changed
      ? '<span class="chg">' + i.grams + ' g</span> <s>' + i.originalGrams + ' g</s>'
      : i.grams + ' g';
    const flag = i.unmatched ? ' <span class="muted">(geen voedingswaarde)</span>' : '';
    return '<div class="row"><span>' + escapeHtml(i.name) + flag + '</span><span class="amt">' + amount + '</span></div>';
  }).join("");

  const lowCoverage = p.coverage < 0.8
    ? '<p class="note">Let op: voor ' + Math.round((1 - p.coverage) * 100) + '% van het gewicht is geen voedingswaarde gevonden, de totalen zijn dus een onderschatting.</p>'
    : "";

  return '<div class="card">'
    + '<strong>' + escapeHtml(p.title) + '</strong>'
    + '<p class="muted">per portie &middot; ' + p.portions + ' portie(s) totaal</p>'
    + macroChips(p.perPortion)
    + rows
    + lowCoverage
    + '<p class="muted" style="margin-top:10px"><a href="' + p.url + '" target="_blank" rel="noopener">Bereidingswijze op ah.nl</a></p>'
    + '</div>';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function run(button, fn) {
  const label = button.textContent;
  button.disabled = true;
  button.textContent = "Bezig\\u2026";
  try { await fn(); }
  catch (err) { $("out").innerHTML = '<div class="card"><p class="note">' + escapeHtml(err.message) + '</p></div>'; }
  finally { button.disabled = false; button.textContent = label; }
}

$("go").onclick = () => run($("go"), async () => {
  const body = {
    protein: numOrUndef("protein"), kcal: numOrUndef("kcal"),
    carbs: numOrUndef("carbs"), fat: numOrUndef("fat"),
    portions: numOrUndef("portions") || 1, kcalMode: $("kcalMode").value,
  };
  const data = await api("/api/generate", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  $("out").innerHTML = data.plans.length
    ? data.plans.map(renderPlan).join("")
    : '<div class="card"><p class="muted">Geen recepten gevonden.</p></div>';
});

$("ingest").onclick = () => run($("ingest"), async () => {
  const r = await api("/api/ingest", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
  $("stats").textContent = r.added + " recepten toegevoegd" + (r.errors.length ? ", " + r.errors.length + " fouten" : "");
});

$("probe").onclick = () => run($("probe"), async () => {
  const r = await api("/api/probe");
  $("stats").textContent = Object.entries(r).map(([k, v]) => k + ": " + v).join(" | ");
});

api("/api/stats").then((s) => { $("stats").textContent = s.recipes + " recepten in de cache"; }).catch(() => {});
</script>
</body>
</html>`;
}
