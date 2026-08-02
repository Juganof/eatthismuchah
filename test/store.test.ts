import { afterEach, describe, expect, it } from "vitest";
import type { Recipe } from "../src/ah/types";
import { Store } from "../src/db/queries";
import { DEFAULT_SLOTS } from "../src/nutrition/split";
import { DEFAULT_PROFILE } from "../src/nutrition/targets";
import { createTestDb, type TestDb } from "./helpers/d1";
import { FOODS, seedRecipes } from "./helpers/seed";

let db: TestDb | null = null;
afterEach(() => {
  db?.close();
  db = null;
});

function freshStore(): Store {
  db = createTestDb();
  return new Store(db);
}

const recipe = (over: Partial<Recipe> = {}): Recipe => ({
  id: "R-R1",
  title: "Kip met rijst",
  url: "https://www.ah.nl/allerhande/recept/R-R1",
  servings: 4,
  imageUrl: "https://example.test/kip.jpg",
  ingredients: [{ name: "kipfilet", quantity: 400, unit: "g" }],
  ...over,
});

describe("recipe storage", () => {
  it("stores and reads a recipe back", async () => {
    const store = freshStore();
    await store.putRecipe(recipe());
    const stored = await store.getRecipe("R-R1");

    expect(stored?.title).toBe("Kip met rijst");
    expect(stored?.ingredients).toEqual([{ name: "kipfilet", quantity: 400, unit: "g" }]);
  });

  it("never lets a failed rescrape wipe an ingredient list", async () => {
    const store = freshStore();
    await store.putRecipe(recipe());
    // Dit is wat er gebeurt als AH zijn pagina omgooit: titel nog wel, rest niet.
    await store.putRecipe(recipe({ ingredients: [] }));

    const stored = await store.getRecipe("R-R1");
    expect(stored?.ingredients).toHaveLength(1);
  });

  it("keeps the first sighting and the image across rescrapes", async () => {
    const store = freshStore();
    await store.putRecipe(recipe());
    const first = await db!
      .prepare("SELECT first_seen_at FROM recipes WHERE id = 'R-R1'")
      .first<{ first_seen_at: number }>();

    await new Promise((resolve) => setTimeout(resolve, 5));
    await store.putRecipe(recipe({ imageUrl: null, title: "Kip met rijst en broccoli" }));

    const after = await db!
      .prepare("SELECT first_seen_at, image_url, title FROM recipes WHERE id = 'R-R1'")
      .first<{ first_seen_at: number; image_url: string; title: string }>();

    expect(after?.first_seen_at).toBe(first?.first_seen_at);
    expect(after?.image_url).toBe("https://example.test/kip.jpg");
    expect(after?.title).toBe("Kip met rijst en broccoli");
  });

  it("does not read free-range eggs as chicken meat", async () => {
    const store = freshStore();
    // AH schrijft scharreleieren als "kip buiteneieren". Zonder correctie leest
    // de tagger daar vlees in en krijgt een vegetarier dit recept nooit te zien.
    await store.putRecipe(
      recipe({
        id: "R-R2",
        title: "Panzanella met gegrilde groente en ei",
        ingredients: [{ name: "kip buiteneieren", quantity: 6, unit: "stuk" }],
      }),
    );
    const row = await db!
      .prepare("SELECT tags FROM recipes WHERE id = 'R-R2'")
      .first<{ tags: string }>();

    const tags = JSON.parse(row!.tags) as string[];
    expect(tags).toContain("ei");
    expect(tags).not.toContain("vlees");
  });

  it("bewaart AH's eigen voedingswaarde, want het plannen leest alleen uit de database", async () => {
    const store = freshStore();
    await store.putRecipe(recipe({ nutritionPerServing: { kcal: 520, protein: 41 } }));
    expect((await store.getRecipe("R-R1"))?.nutritionPerServing).toEqual({ kcal: 520, protein: 41 });
  });

  it("laat een herscrape zonder voedingswaarde de eerder gevonden cijfers staan", async () => {
    const store = freshStore();
    await store.putRecipe(recipe({ nutritionPerServing: { kcal: 520 } }));
    await store.putRecipe(recipe({ nutritionPerServing: null }));
    expect((await store.getRecipe("R-R1"))?.nutritionPerServing).toEqual({ kcal: 520 });
  });

  it("derives tags so the diet filter has something to work with", async () => {
    const store = freshStore();
    await store.putRecipe(recipe());
    const row = await db!
      .prepare("SELECT tags FROM recipes WHERE id = 'R-R1'")
      .first<{ tags: string }>();

    expect(JSON.parse(row!.tags)).toContain("vlees");
  });
});

describe("scrape archive", () => {
  const raw = {
    kind: "recipe" as const,
    ref: "R-R1",
    url: "https://www.ah.nl/allerhande/recept/R-R1",
    status: 200,
    body: "<html>hallo</html>",
  };

  it("keeps every scrape rather than overwriting the previous one", async () => {
    const store = freshStore();
    await store.putRaw(raw);
    await new Promise((resolve) => setTimeout(resolve, 5));
    await store.putRaw({ ...raw, body: "<html>nieuwer</html>" });

    const counts = await store.countRaw();
    expect(counts.total).toBe(2);
    expect(counts.unparsed).toBe(2);
  });

  it("hands back the most recent payload for a reference", async () => {
    const store = freshStore();
    await store.putRaw(raw);
    await new Promise((resolve) => setTimeout(resolve, 5));
    await store.putRaw({ ...raw, body: "<html>nieuwer</html>" });

    expect((await store.latestRaw("recipe", "R-R1"))?.body).toBe("<html>nieuwer</html>");
  });

  it("returns one row per reference for reparsing, newest first", async () => {
    const store = freshStore();
    await store.putRaw(raw);
    await new Promise((resolve) => setTimeout(resolve, 5));
    await store.putRaw({ ...raw, body: "<html>nieuwer</html>" });
    await store.putRaw({ ...raw, ref: "R-R2" });

    const rows = await store.latestRawPerRef("recipe", 10);
    expect(rows).toHaveLength(2);
    expect(rows.map((r) => r.ref).sort()).toEqual(["R-R1", "R-R2"]);
    expect(rows.find((r) => r.ref === "R-R1")?.body).toBe("<html>nieuwer</html>");
  });

  it("records that a payload parsed, so reparse can skip it", async () => {
    const store = freshStore();
    await store.putRaw(raw);
    const [row] = await store.latestRawPerRef("recipe", 1);
    await store.markRawParsed(row!.id, true);

    expect((await store.countRaw()).unparsed).toBe(0);
  });
});

describe("profile", () => {
  it("hands back the default profile before anything is stored", async () => {
    expect(await freshStore().getProfile()).toEqual(DEFAULT_PROFILE);
  });

  it("survives a round trip", async () => {
    const store = freshStore();
    const profile = {
      ...DEFAULT_PROFILE,
      age: 34,
      sex: "man" as const,
      heightCm: 183,
      weightKg: 82.5,
      goal: "cut" as const,
      diet: ["geen_vis"],
    };
    await store.putProfile(profile);
    expect(await store.getProfile()).toEqual(profile);
  });

  it("overwrites rather than adding a second profile", async () => {
    const store = freshStore();
    await store.putProfile({ ...DEFAULT_PROFILE, weightKg: 80 });
    await store.putProfile({ ...DEFAULT_PROFILE, weightKg: 78 });

    const row = await db!.prepare("SELECT COUNT(*) AS n FROM profile").first<{ n: number }>();
    expect(row?.n).toBe(1);
    expect((await store.getProfile()).weightKg).toBe(78);
  });
});

describe("meal slots", () => {
  it("seeds the default division on first use", async () => {
    const store = freshStore();
    expect(await store.getSlots()).toEqual(DEFAULT_SLOTS);
    // Zaaien mag maar één keer gebeuren, anders komen ze dubbel terug.
    expect(await store.getSlots()).toHaveLength(DEFAULT_SLOTS.length);
  });

  it("replaces the whole division, so a removed slot stays gone", async () => {
    const store = freshStore();
    await store.getSlots();
    await store.putSlots([DEFAULT_SLOTS[0]!, DEFAULT_SLOTS[3]!]);

    const slots = await store.getSlots();
    expect(slots.map((s) => s.id)).toEqual(["ontbijt", "diner"]);
  });
});

describe("saved days", () => {
  const day = {
    date: "2026-08-01",
    targets: { kcal: 2000 },
    totals: { kcal: 1980 },
    meals: [
      { slotId: "ontbijt", slotName: "Ontbijt", position: 0, recipeId: "R-R1", portions: 1, plan: { recipeId: "R-R1" } },
      { slotId: "diner", slotName: "Diner", position: 1, recipeId: "R-R3", portions: 2, plan: { recipeId: "R-R3" } },
    ],
  };

  it("stores a day with its meals and reads it back", async () => {
    const store = freshStore();
    const id = await store.saveDay(day);
    const stored = await store.getDay(id);

    expect(stored?.date).toBe("2026-08-01");
    expect(stored?.meals).toHaveLength(2);
    expect(stored?.meals[1]!.portions).toBe(2);
    expect(stored?.totals).toEqual({ kcal: 1980 });
  });

  it("replaces the meals when the same day is saved again", async () => {
    const store = freshStore();
    const id = await store.saveDay(day);
    await store.saveDay({ ...day, id, meals: [day.meals[0]!] });

    expect((await store.getDay(id))?.meals).toHaveLength(1);
  });

  it("lists days inside a window only", async () => {
    const store = freshStore();
    await store.saveDay(day);
    await store.saveDay({ ...day, date: "2026-08-05" });
    await store.saveDay({ ...day, date: "2026-09-01" });

    const days = await store.listDays("2026-08-01", "2026-08-07");
    expect(days.map((d) => d.date)).toEqual(["2026-08-01", "2026-08-05"]);
  });

  it("takes the meals with it when a day is deleted", async () => {
    const store = freshStore();
    const id = await store.saveDay(day);
    await store.deleteDay(id);

    expect(await store.getDay(id)).toBeNull();
    const row = await db!
      .prepare("SELECT COUNT(*) AS n FROM saved_day_meals")
      .first<{ n: number }>();
    expect(row?.n).toBe(0);
  });
});

describe("preferences and exclusions", () => {
  it("stores, changes and clears a preference", async () => {
    const store = freshStore();
    await store.setPref("R-R1", "fav");
    expect((await store.listPrefs())[0]).toMatchObject({ recipeId: "R-R1", status: "fav" });

    await store.setPref("R-R1", "blocked");
    expect((await store.listPrefs())[0]!.status).toBe("blocked");

    await store.setPref("R-R1", null);
    expect(await store.listPrefs()).toEqual([]);
  });

  it("normalises and de-duplicates exclusion terms", async () => {
    const store = freshStore();
    await store.putExclusions([" Banaan ", "banaan", "", "Kokos"]);
    expect(await store.getExclusions()).toEqual(["banaan", "kokos"]);
  });
});

describe("shortlistForSlot", () => {
  async function seeded(): Promise<Store> {
    const store = freshStore();
    await seedRecipes(store, [
      {
        id: "R-R1",
        title: "Kwark met banaan",
        ingredients: [{ name: "kwark", grams: 250, per100g: FOODS.kwark! }],
      },
      {
        id: "R-R3",
        title: "Kip met rijst",
        ingredients: [
          { name: "kipfilet", grams: 200, per100g: FOODS.kipfilet! },
          { name: "rijst", grams: 90, per100g: FOODS.rijst! },
        ],
      },
    ]);
    return store;
  }

  it("drops blocked recipes entirely", async () => {
    const store = await seeded();
    await store.setPref("R-R3", "blocked");

    const ids = (await store.shortlistForSlot({ kcalPerPortion: 450 })).map((c) => c.id);
    expect(ids).not.toContain("R-R3");
  });

  it("marks favourites so the planner can prefer them", async () => {
    const store = await seeded();
    await store.setPref("R-R1", "fav");

    const found = (await store.shortlistForSlot({ kcalPerPortion: 200 })).find(
      (c) => c.id === "R-R1",
    );
    expect(found?.favourite).toBe(true);
  });

  it("counts recent uses so a repeat can be discouraged", async () => {
    const store = await seeded();
    await store.saveDay({
      date: new Date().toISOString().slice(0, 10),
      targets: {},
      totals: {},
      meals: [
        { slotId: "diner", slotName: "Diner", position: 0, recipeId: "R-R3", portions: 1, plan: {} },
      ],
    });

    const found = (await store.shortlistForSlot({ kcalPerPortion: 450 })).find(
      (c) => c.id === "R-R3",
    );
    expect(found?.recentUses).toBe(1);
  });

  it("keeps out recipes far outside the calorie band", async () => {
    const store = await seeded();
    // R-R3 zit rond de 530 kcal; op een doel van 50 kcal hoort die er niet bij.
    const ids = (await store.shortlistForSlot({ kcalPerPortion: 50 })).map((c) => c.id);
    expect(ids).not.toContain("R-R3");
  });
});

describe("matchMap", () => {
  it("returns the remembered product per ingredient in one go", async () => {
    const store = freshStore();
    await store.putMatch("kipfilet", "wi-123", 0.9);
    await store.putMatch("water", null, 0);

    expect(await store.matchMap(["kipfilet", "water"])).toEqual({ kipfilet: "wi-123" });
    expect(await store.matchMap([])).toEqual({});
  });
});

describe("productMap", () => {
  it("haalt titel en verpakking van meerdere producten in één query op", async () => {
    const store = freshStore();
    await store.putProduct({ webshopId: "wi-1", title: "Kikkererwten", salesUnitSize: "330 g", per100g: {} });
    await store.putProduct({ webshopId: "wi-2", title: "Melk", salesUnitSize: "1 l", per100g: {} });

    expect(await store.productMap(["wi-1", "wi-2"])).toEqual({
      "wi-1": { title: "Kikkererwten", salesUnitSize: "330 g" },
      "wi-2": { title: "Melk", salesUnitSize: "1 l" },
    });
    expect(await store.productMap([])).toEqual({});
  });
});

describe("applicatielog", () => {
  it("bewaart regels met niveau, herkomst en details, nieuwste eerst", async () => {
    const store = freshStore();
    await store.log("info", "ah", "recipe R-R1 -> 200", { bytes: 12 });
    await store.log("error", "ingest", "recept R-R1 mislukt", { fout: "GET -> 403" });

    const rows = await store.recentLogs(10);
    expect(rows).toHaveLength(2);
    expect(rows[0]!.message).toBe("recept R-R1 mislukt");
    expect(JSON.parse(rows[0]!.detail!)).toEqual({ fout: "GET -> 403" });
    expect(await store.recentLogs(10, "info")).toHaveLength(1);
    expect(await store.countLogs()).toBe(2);
  });

  it("laat de aanroeper nooit omvallen als loggen misgaat", async () => {
    const store = freshStore();
    await db!.prepare("DROP TABLE app_logs").run();
    // Geen throw: een kapotte log mag een lopende scrape niet stoppen.
    await expect(store.log("info", "ah", "iets")).resolves.toBeUndefined();
  });

  it("houdt alleen de nieuwste regels over bij het inkorten", async () => {
    const store = freshStore();
    for (let i = 0; i < 5; i++) await store.log("info", "test", "regel " + i);

    await store.trimLogs(2);

    const rows = await store.recentLogs(10);
    expect(rows.map((r) => r.message)).toEqual(["regel 4", "regel 3"]);
  });
});

describe("wipe", () => {
  it("gooit de scrape-data weg maar laat profiel en opgeslagen dagen staan", async () => {
    const store = freshStore();
    await seedRecipes(store, [
      { id: "R-R1", title: "Kwark", ingredients: [{ name: "kwark", grams: 200, per100g: FOODS.kwark! }] },
    ]);
    await store.putProfile({ ...DEFAULT_PROFILE, weightKg: 82 });

    const deleted = await store.wipe();

    expect(deleted["recipes"]).toBe(1);
    expect(await store.countRecipes()).toBe(0);
    expect(await store.getProduct("wi-kwark")).toBeNull();
    // Het profiel is van de gebruiker en staat nergens anders: dat blijft.
    expect((await store.getProfile()).weightKg).toBe(82);
  });

  it("gooit met scope 'alles' ook het profiel weg", async () => {
    const store = freshStore();
    await store.putProfile({ ...DEFAULT_PROFILE, weightKg: 82 });

    await store.wipe("alles");

    expect((await store.getProfile()).weightKg).toBe(DEFAULT_PROFILE.weightKg);
  });
});
