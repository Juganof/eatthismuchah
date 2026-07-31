import { afterAll, describe, expect, it } from "vitest";
import worker, { type Env } from "../src/index";
import { createTestDb } from "./helpers/d1";

const db = createTestDb();
afterAll(() => db.close());

const env: Env = {
  DB: db,
  AH_USER_AGENT: "test",
  INGEST_QUERIES: "kip",
  INGEST_LIMIT: "1",
};

const ctx = { waitUntil: () => {}, passThroughOnException: () => {} } as unknown as ExecutionContext;

const fetchWorker = (path: string, init?: RequestInit) =>
  worker.fetch(new Request(`https://worker.test${path}`, init), env, ctx);

describe("Worker routes", () => {
  it("renders the planner UI", async () => {
    const response = await fetchWorker("/");
    const html = await response.text();
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/html");
    expect(html).toContain("AH Macro Planner");
    // De vier tabbladen en de endpoints waar ze op leunen zitten in één document,
    // dus als er eentje uit de bundel valt, valt deze test om.
    for (const tab of ["panel-profiel", "panel-momenten", "panel-dag", "panel-week"]) {
      expect(html, `tab ${tab} ontbreekt`).toContain(tab);
    }
    for (const route of [
      "/api/profile",
      "/api/slots",
      "/api/day/generate",
      "/api/day/slot",
      "/api/day/reroll",
      "/api/shopping",
      "/api/enrich",
      "/api/products/search",
    ]) {
      expect(html, `${route} wordt niet aangeroepen`).toContain(route);
    }
  });

  it("reports empty database statistics", async () => {
    const response = await fetchWorker("/api/stats");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      recipes: 0,
      plannable: 0,
      zonderIngredienten: 0,
      scrapes: 0,
      unparsed: 0,
    });
  });

  it("validates search, plan and match requests", async () => {
    const search = await fetchWorker("/api/search");
    expect(search.status).toBe(400);

    const plan = await fetchWorker("/api/plan", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(plan.status).toBe(400);

    const match = await fetchWorker("/api/match", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(match.status).toBe(400);
  });

  it("answers a database failure with readable JSON, not a bare 500", async () => {
    // Dit is wat er live gebeurde toen de migratie nog niet gedraaid was: D1
    // gooide op een ontbrekende kolom, Hono antwoordde met platte tekst, en de
    // UI kon daar alleen "geen geldige respons" van maken.
    const brokenDb = {
      prepare: () => {
        throw new Error("D1_ERROR: no such column: first_seen_at");
      },
    } as unknown as D1Database;

    const response = await worker.fetch(
      new Request("https://worker.test/api/stats"),
      { ...env, DB: brokenDb },
      ctx,
    );

    expect(response.status).toBe(500);
    expect(response.headers.get("content-type")).toContain("application/json");
    const body = (await response.json()) as { error: string };
    expect(body.error).toContain("no such column");
    // en de melding wijst naar de oplossing
    expect(body.error).toContain("db:migrate");
  });

  it("explains that ingest is required before generation", async () => {
    const response = await fetchWorker("/api/generate", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ protein: 60, kcal: 700 }),
    });
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({ plans: [] });
  });
});
