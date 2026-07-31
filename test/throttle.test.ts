import { afterEach, describe, expect, it, vi } from "vitest";
import { AhClient, type RawScrape } from "../src/ah/client";

/**
 * ah.nl staat achter Akamai's botbescherming. Uit het scrape-archief van de
 * live worker bleek dat die op tempo reageert, niet op aantallen: drie
 * receptpagina's binnen 150 ms leverden 403's op, terwijl exact dezelfde
 * pagina's een halve minuut later gewoon 200 gaven. Vandaar deze tests.
 */

const RECIPE_PAGE = `<html><script type="application/ld+json">${JSON.stringify({
  "@type": "Recipe",
  name: "Testrecept",
  url: "https://www.ah.nl/allerhande/recept/R-R1/testrecept",
  recipeYield: "4",
  recipeIngredient: ["100 g kipfilet"],
})}</script></html>`;

const blocked = () => new Response("Access Denied", { status: 403 });
const ok = () => new Response(RECIPE_PAGE, { status: 200 });

afterEach(() => vi.unstubAllGlobals());

/** Zonder pauzes, anders duurt de suite seconden in plaats van milliseconden. */
const fastClient = (onRaw?: (raw: RawScrape) => void) =>
  new AhClient("test", onRaw, { minIntervalMs: 0, backoffMs: 0, maxRetries: 2 });

describe("retrying a blocked request", () => {
  it("tries again after a 403 and succeeds", async () => {
    const fetchMock = vi.fn().mockImplementationOnce(blocked).mockImplementationOnce(ok);
    vi.stubGlobal("fetch", fetchMock);

    const recipe = await fastClient().getRecipe("R-R1");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(recipe?.ingredients).toHaveLength(1);
  });

  it("gives up after the retry budget and reports the status", async () => {
    const fetchMock = vi.fn().mockImplementation(blocked);
    vi.stubGlobal("fetch", fetchMock);

    await expect(fastClient().getRecipe("R-R1")).rejects.toThrow("403");
    // Eén poging plus twee herkansingen.
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("does not retry a 404, because that page is simply not there", async () => {
    const fetchMock = vi.fn().mockImplementation(() => new Response("nope", { status: 404 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(fastClient().getRecipe("R-R1")).rejects.toThrow("404");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("archives every attempt, including the blocked ones", async () => {
    const fetchMock = vi.fn().mockImplementationOnce(blocked).mockImplementationOnce(ok);
    vi.stubGlobal("fetch", fetchMock);

    const archived: RawScrape[] = [];
    await fastClient((raw) => archived.push(raw)).getRecipe("R-R1");

    // Juist de geblokkeerde poging is het bewijsmateriaal: zonder die rij in het
    // archief was nooit duidelijk geworden dat het om tempo ging.
    expect(archived.map((a) => a.status)).toEqual([403, 200]);
    expect(archived[0]!.kind).toBe("recipe");
    expect(archived[0]!.ref).toBe("R-R1");
  });
});

describe("pacing", () => {
  it("leaves a gap between two requests", async () => {
    vi.stubGlobal("fetch", vi.fn().mockImplementation(ok));
    const client = new AhClient("test", undefined, { minIntervalMs: 120 });

    const started = Date.now();
    await client.getRecipe("R-R1");
    await client.getRecipe("R-R2");

    // Twee verzoeken, dus minstens één wachttijd ertussen.
    expect(Date.now() - started).toBeGreaterThanOrEqual(100);
  });

  it("also paces across separate client instances, not just within one", async () => {
    // Elke knop in de UI en elke cron-tick bouwt zijn eigen AhClient. Zonder een
    // gedeelde klok begint de pacing van elke instance weer bij nul, en spreken
    // twee kort na elkaar ingedrukte knoppen elk hun eigen tempo — samen toch
    // een burst richting ah.nl, ook al hield elke aanroep zich keurig aan zijn
    // eigen interval.
    vi.stubGlobal("fetch", vi.fn().mockImplementation(ok));

    const started = Date.now();
    await new AhClient("test", undefined, { minIntervalMs: 120 }).getRecipe("R-R1");
    await new AhClient("test", undefined, { minIntervalMs: 120 }).getRecipe("R-R2");

    expect(Date.now() - started).toBeGreaterThanOrEqual(100);
  });
});
