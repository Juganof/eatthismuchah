/**
 * Het tempo waarmee we ah.nl benaderen, gedeeld over alle clients.
 *
 * Elke aanroep (een knop in de UI, de cron, een andere API-route) bouwt zijn
 * eigen client, en die kunnen ook nog eens gelijktijdig lopen. Pacing per
 * instance zou dan niets voorstellen — twee knoppen kort na elkaar ingedrukt
 * spreken elk hun eigen klok, die allebei op nul begint, en samen alsnog een
 * tempo-blokkade veroorzaken. Deze klok wordt daarom door elke client gedeeld
 * die in hetzelfde worker-isolate leeft.
 */

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let lastRequestAt = 0;

/** Rust tussen twee verzoeken aan ah.nl, zodat Akamai ons niet op tempo blokkeert. */
export async function sharedPace(minIntervalMs: number): Promise<void> {
  const wait = minIntervalMs - (Date.now() - lastRequestAt);
  if (wait > 0) await sleep(wait);
  lastRequestAt = Date.now();
}

/** Alleen voor tests: zet de klok terug zodat er niets tussen tests lekt. */
export function resetPace(): void {
  lastRequestAt = 0;
}
