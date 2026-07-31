/**
 * Helpers for getting structured data back out of ah.nl pages. AH renders with
 * Next.js, so the page state we need is sitting in a script tag rather than in
 * the markup — parsing that is both easier and far more stable than CSS selectors.
 */

/** Script tags known to carry the page's serialised state. */
const STATE_SCRIPTS = [
  /<script[^>]*id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i,
  /<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/i,
  /window\.__INITIAL_STATE__\s*=\s*(\{[\s\S]*?\});?\s*<\/script>/i,
];

/**
 * Returns every JSON blob embedded in the page. Callers search the result rather
 * than indexing into it, so an extra or missing blob is harmless.
 */
export function extractEmbeddedJson(html: string): unknown[] {
  const out: unknown[] = [...extractFlightJson(html)];
  for (const re of STATE_SCRIPTS) {
    // Match every occurrence, not just the first — ld+json appears many times.
    const global = new RegExp(re.source, re.flags.includes("g") ? re.flags : re.flags + "g");
    for (const match of html.matchAll(global)) {
      const raw = match[1];
      if (!raw) continue;
      try {
        out.push(JSON.parse(raw.trim()));
      } catch {
        // A blob we can't parse is simply not a source of recipes.
      }
    }
  }
  return out;
}

/**
 * De App Router van Next.js zet de paginastate niet in één script-tag maar in een
 * reeks `self.__next_f.push([1, "..."])`-aanroepen die samen één stream vormen.
 * Daar zit precies het rijke recept-object in — inclusief de productkoppeling per
 * ingredient — dat in de ld+json niet voorkomt. We plakken de stukjes aan elkaar
 * en vissen er elk JSON-object uit dat parseert; wat niet parseert is simpelweg
 * geen bron van recepten.
 */
export function extractFlightJson(html: string): unknown[] {
  let stream = "";
  for (const match of html.matchAll(
    /self\.__next_f\.push\(\s*\[\s*\d+\s*,\s*("(?:[^"\\]|\\.)*")/g,
  )) {
    try {
      stream += JSON.parse(match[1]!) as string;
    } catch {
      // Een chunk die geen geldige string-literal is, slaan we over.
    }
  }
  if (stream === "") return [];

  const out: unknown[] = [];
  // Elke flight-regel begint met "<id>:" gevolgd door een optionele typeletter en
  // dan de payload. Alleen daar beginnen we te lezen, zodat een accolade midden
  // in een tekst geen scan van tienduizenden tekens uitlokt.
  for (const start of rowStarts(stream)) {
    const json = balancedSlice(stream, start);
    if (json === null) continue;
    try {
      out.push(JSON.parse(json));
    } catch {
      // onvolledige of afgekapte regel
    }
  }
  return out;
}

function rowStarts(stream: string): number[] {
  const starts: number[] = [];
  for (const match of stream.matchAll(/(?:^|\n)[0-9a-f]+:[A-Za-z]?(?=[[{])/g)) {
    starts.push(match.index! + match[0].length);
  }
  return starts;
}

/** Leest vanaf `start` één gebalanceerd JSON-object of -array, of null. */
function balancedSlice(text: string, start: number): string | null {
  const open = text[start];
  if (open !== "{" && open !== "[") return null;
  const close = open === "{" ? "}" : "]";
  let depth = 0;
  let inString = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i]!;
    if (inString) {
      if (ch === "\\") i++;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === open) depth++;
    else if (ch === close) {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
}

/** Depth-first search for the first value satisfying `pred`. */
export function deepFind(
  root: unknown,
  pred: (v: unknown) => boolean,
  maxDepth = 12,
): unknown | null {
  const stack: { v: unknown; d: number }[] = [{ v: root, d: 0 }];
  while (stack.length > 0) {
    const next = stack.pop();
    if (!next) break;
    const { v, d } = next;
    if (v === null || v === undefined || d > maxDepth) continue;
    if (pred(v)) return v;
    if (Array.isArray(v)) {
      for (const item of v) stack.push({ v: item, d: d + 1 });
    } else if (typeof v === "object") {
      for (const item of Object.values(v as Record<string, unknown>)) {
        stack.push({ v: item, d: d + 1 });
      }
    }
  }
  return null;
}
