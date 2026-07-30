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
  const out: unknown[] = [];
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
