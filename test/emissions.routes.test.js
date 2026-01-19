const test = require("node:test");
const assert = require("node:assert/strict");
const fastify = require("fastify");
const { ObjectId } = require("mongodb");

const emissionsRoutes = require("../src/routes/emissions.routes");

// Minimal chainable mock for Mongo collection cursor
function makeEmissionsColMock(returnDocs, capture) {
  return {
    find(filter, opts) {
      capture.filter = filter;
      capture.opts = opts;

      return {
        sort(sortSpec) {
          capture.sort = sortSpec;
          return this;
        },
        limit(n) {
          capture.limit = n;
          return this;
        },
        async toArray() {
          return returnDocs;
        },
      };
    },
  };
}

test("GET /emissions - requires siteId and equipmentId (Fastify schema validation)", async () => {
  const app = fastify({ logger: false });

  const capture = {};
  const emissionsCol = makeEmissionsColMock([], capture);

  await app.register(emissionsRoutes, { emissionsCol });

  const res = await app.inject({
    method: "GET",
    url: "/emissions",
  });

  assert.equal(res.statusCode, 400);
  await app.close();
});

test("GET /emissions - invalid from returns 400", async () => {
  const app = fastify({ logger: false });

  const capture = {};
  const emissionsCol = makeEmissionsColMock([], capture);

  await app.register(emissionsRoutes, { emissionsCol });

  const res = await app.inject({
    method: "GET",
    url: "/emissions?siteId=s1&equipmentId=e1&from=not-a-date",
  });

  assert.equal(res.statusCode, 400);
  assert.deepEqual(res.json(), { error: "Invalid 'from' timestamp" });
  await app.close();
});

test("GET /emissions - cursorTs without cursorId returns 400", async () => {
  const app = fastify({ logger: false });

  const capture = {};
  const emissionsCol = makeEmissionsColMock([], capture);

  await app.register(emissionsRoutes, { emissionsCol });

  const res = await app.inject({
    method: "GET",
    url: "/emissions?siteId=s1&equipmentId=e1&cursorTs=2026-01-01T00:00:00.000Z",
  });

  assert.equal(res.statusCode, 400);
  assert.deepEqual(res.json(), { error: "Both cursorTs and cursorId are required together" });
  await app.close();
});

test("GET /emissions - invalid cursorId returns 400", async () => {
  const app = fastify({ logger: false });

  const capture = {};
  const emissionsCol = makeEmissionsColMock([], capture);

  await app.register(emissionsRoutes, { emissionsCol });

  const res = await app.inject({
    method: "GET",
    url: "/emissions?siteId=s1&equipmentId=e1&cursorTs=2026-01-01T00:00:00.000Z&cursorId=not-an-objectid",
  });

  assert.equal(res.statusCode, 400);
  assert.deepEqual(res.json(), { error: "Invalid 'cursorId' ObjectId" });
  await app.close();
});

test("GET /emissions - happy path builds filter/sort/limit and returns nextCursor", async () => {
  const app = fastify({ logger: false });

  const limit = 2;
  const ts1 = new Date("2026-01-02T00:00:00.000Z");
  const ts2 = new Date("2026-01-01T00:00:00.000Z");

  const docs = [
    { _id: new ObjectId(), timestamp: ts1, siteId: "s1", equipmentId: "e1", confidence: 0.9 },
    { _id: new ObjectId(), timestamp: ts2, siteId: "s1", equipmentId: "e1", confidence: 0.95 },
    // extra doc => hasMore should be true
    {
      _id: new ObjectId(),
      timestamp: new Date("2025-12-31T00:00:00.000Z"),
      siteId: "s1",
      equipmentId: "e1",
      confidence: 0.99,
    },
  ];

  const capture = {};
  const emissionsCol = makeEmissionsColMock(docs, capture);

  await app.register(emissionsRoutes, { emissionsCol });

  const res = await app.inject({
    method: "GET",
    url: `/emissions?siteId=s1&equipmentId=e1&limit=${limit}&confidenceMin=0.75`,
  });

  assert.equal(res.statusCode, 200);

  // verify Mongo query shape
  assert.deepEqual(capture.sort, { timestamp: -1, _id: -1 });
  assert.equal(capture.limit, limit + 1);

  // verify response
  const body = res.json();
  assert.equal(body.count, limit);
  assert.equal(body.items.length, limit);
  assert.ok(body.nextCursor);

  // nextCursor should match last returned item
  const last = body.items[body.items.length - 1];
  assert.equal(body.nextCursor.cursorTs, new Date(last.timestamp).toISOString());
  assert.equal(body.nextCursor.cursorId, String(last._id));
  await app.close();
});
