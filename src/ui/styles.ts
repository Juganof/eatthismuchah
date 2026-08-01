/**
 * Alle stijl van de app. Inline in de pagina, want er is geen buildstap en geen
 * tweede request: op mobiele data is één document dat meteen klopt het snelst.
 */
export const styles = `
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
  main { max-width: 680px; margin: 0 auto; }
  h1 { font-size: 1.35rem; margin: 4px 0 2px; }
  h2 { font-size: 1rem; margin: 0 0 10px; }
  .sub { color: var(--muted); font-size: .85rem; margin: 0 0 14px; }

  /* Tabbalk: horizontaal scrollbaar zodat vier tabs ook op een smalle telefoon passen. */
  .tabs {
    display: flex; gap: 6px; overflow-x: auto; margin-bottom: 12px;
    scrollbar-width: none; -webkit-overflow-scrolling: touch;
  }
  .tabs::-webkit-scrollbar { display: none; }
  .tab {
    /* width en margin overschrijven de algemene button-regel hieronder, anders
       vult één tab de hele balk en verdwijnen de andere drie buiten beeld. */
    flex: 0 0 auto; width: auto; margin: 0;
    padding: 9px 14px; border-radius: 999px; border: 1px solid var(--line);
    background: var(--card); color: var(--muted); font-size: .88rem; font-weight: 600;
    cursor: pointer; white-space: nowrap;
  }
  .tab[aria-selected="true"] { background: var(--accent); border-color: var(--accent); color: #fff; }
  .panel[hidden] { display: none; }

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
  input[type="checkbox"] { width: auto; }
  button {
    width: 100%; padding: 13px; border: 0; border-radius: 10px; background: var(--accent);
    color: #fff; font-size: 1rem; font-weight: 600; margin-top: 12px; cursor: pointer;
  }
  button:disabled { opacity: .55; }
  button.secondary { background: transparent; color: var(--accent); border: 1px solid var(--line); }
  button.small { width: auto; padding: 7px 12px; font-size: .82rem; margin: 0; }
  /* Wissen is onomkeerbaar, dus die knoppen zien er ook zo uit. */
  button.danger { color: var(--warn); border-color: var(--warn); }
  select.small { width: auto; padding: 6px 10px; font-size: .82rem; margin: 0; }
  .actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }

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
  .macro.over b { color: var(--warn); }
  .macro.good b { color: var(--ok); }

  /* Keuzekaarten: elke optie is een <button>, dus dezelfde override als .tab
     hierboven — anders maakt de algemene button-regel er iets vols en blauws van. */
  .options { display: grid; gap: 8px; margin-top: 10px; }
  .option {
    width: 100%; margin: 0; text-align: left; padding: 10px 12px;
    background: var(--bg); color: var(--text);
    border: 1px solid var(--line); border-radius: 12px; font-weight: 400; cursor: pointer;
  }
  .option:hover, .option:focus-visible { border-color: var(--accent); }
  /* De knop om het recept eerst te bekijken hoort bij de kaart erboven, dus geen
     extra ruimte ertussen. */
  .option-wrap .actions { margin-top: 6px; }
  .option strong { display: block; }
  .option .macros { margin: 6px 0 4px; }

  .note { font-size: .78rem; color: var(--warn); margin-top: 8px; }
  /* Voor waarschuwende tekst midden in een regel, waar .note te veel marge geeft. */
  .warnish { color: var(--warn); }

  /* De applicatielog: veel korte regels, dus compact en met vaste breedte voor
     het tijdstip zodat je verticaal kunt scannen. */
  .logline {
    display: flex; gap: 8px; padding: 3px 0; font-size: .76rem; line-height: 1.35;
    border-bottom: 1px solid var(--line); font-variant-numeric: tabular-nums;
  }
  .logline:last-child { border-bottom: 0; }
  .logline .at { color: var(--muted); white-space: nowrap; }
  .logline .src { color: var(--muted); min-width: 52px; }
  .logline.error .msg { color: var(--warn); font-weight: 600; }
  .logline.warn .msg { color: var(--warn); }
  .logline .det { color: var(--muted); }
  #appLogs { max-height: 340px; overflow-y: auto; }
  #browseQuery { margin-top: 4px; }
  #browseOut .row a { text-decoration: none; }
  .muted { color: var(--muted); font-size: .8rem; }
  .ok { color: var(--ok); }
  a { color: var(--accent); }
  details summary { cursor: pointer; font-size: .85rem; color: var(--muted); }

  /* Eetmomenten: naam, aandeel en aan/uit op één regel. */
  .slot-row {
    display: grid; grid-template-columns: 1fr 82px 62px auto; gap: 8px;
    align-items: end; padding: 8px 0; border-bottom: 1px solid var(--line);
  }
  .slot-row:last-of-type { border-bottom: 0; }
  .slot-row .del { background: transparent; color: var(--warn); border: 1px solid var(--line); }
  /* De hints krijgen de volle breedte onder de rest van de rij. */
  .slot-tags-cell { grid-column: 1 / -1; }
  .slot-tags-cell input { font-size: .85rem; padding: 8px 10px; }

  .meal-head { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
  .meal-head .slot { font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }

  /* Het product onder een ingredientregel: ondergeschikt aan de regel zelf,
     maar wel aanklikbaar — dit is wat je in de winkel of in je mandje legt. */
  .prod { font-size: .78rem; text-decoration: none; }
  .prod-none { font-size: .78rem; color: var(--muted); }
  .row .name { display: block; }

  /* Een receptnaam die je kunt aantikken. Ziet eruit als tekst, gedraagt zich
     als een knop, zodat toetsenbord en schermlezer het ook snappen. */
  .linklike {
    width: auto; margin: 0; padding: 0; background: none; border: 0;
    color: inherit; font: inherit; font-weight: 700; text-align: left;
    cursor: pointer; text-decoration: underline; text-decoration-style: dotted;
    text-underline-offset: 3px;
  }
  .linklike:hover, .linklike:focus-visible { color: var(--accent); }

  /* Het receptvenster. Op een telefoon vult het bijna het scherm; scrollen doet
     de inhoud, niet de pagina eronder. */
  dialog {
    width: min(620px, 94vw); max-height: 88vh; overflow-y: auto;
    background: var(--card); color: var(--text);
    border: 1px solid var(--line); border-radius: 14px; padding: 16px;
  }
  dialog::backdrop { background: rgba(0, 0, 0, .5); }
  dialog h2 { margin-bottom: 4px; }

  /* Weekoverzicht: kolommen op een groot scherm, gestapeld op een telefoon. */
  .week { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 10px; }
  .day-card { background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 10px; }
  .day-card h3 { font-size: .85rem; margin: 0 0 6px; }
  .day-card ul { margin: 0; padding-left: 16px; font-size: .78rem; color: var(--muted); }
`;
