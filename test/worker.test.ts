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
    expect(html).toContain("/api/generate");
  });

  it("reports empty database statistics", async () => {
    const response = await fetchWorker("/api/stats");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ recipes: 0 });
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
