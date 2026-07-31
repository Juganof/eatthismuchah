import { adminCard, dayTab, profileTab, slotsTab, weekTab } from "./markup";
import { script } from "./script";
import { styles } from "./styles";

/**
 * De hele front-end: één zelfstandige pagina, gemaakt voor een telefoonbrowser.
 * Geen buildstap en geen externe requests, zodat hij ook op mobiele data direct
 * laadt. Opmaak, stijl en gedrag staan in aparte bestanden hiernaast; dit
 * bestand plakt ze aan elkaar.
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
<!-- Inline favicon: scheelt een request die anders altijd 404 geeft. -->
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><text y='26' font-size='26'>%F0%9F%A5%97</text></svg>">
<style>${styles}</style>
</head>
<body>
<main>
  <h1>AH Macro Planner</h1>
  <p class="sub">Allerhande-recepten, herberekend naar jouw macro's.</p>

  <div class="tabs" role="tablist">
    <button class="tab" role="tab" data-tab="profiel" aria-selected="true">Profiel</button>
    <button class="tab" role="tab" data-tab="momenten" aria-selected="false">Eetmomenten</button>
    <button class="tab" role="tab" data-tab="dag" aria-selected="false">Dag</button>
    <button class="tab" role="tab" data-tab="week" aria-selected="false">Week</button>
  </div>

  <div id="toast"></div>

${profileTab()}
${slotsTab()}
${dayTab()}
${weekTab()}
${adminCard()}
</main>
<script>${script}</script>
</body>
</html>`;
}
