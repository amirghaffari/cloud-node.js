const test = require("node:test");
const assert = require("node:assert/strict");

const { safeJsonParse, mapRow, ensureIndexes } = require("../scripts/seed-emissions");

test("safeJsonParse returns parsed value or []", () => {
  assert.deepEqual(safeJsonParse("[1,2,3]"), [1, 2, 3]);
  assert.deepEqual(safeJsonParse("not-json"), []);
});

test("mapRow converts types correctly", () => {
  const row = {
    id: "uuid-1",
    timestamp: "2026-01-01T00:00:00.000Z",
    siteId: "site-1",
    siteName: "Site Name",
    equipmentId: "equip-1",
    type: "CH4",
    mass: "12.34",
    unit: "kg",
    scanDuration: "10",
    confidence: "0.9",
    numDetections: "2",
    detections: '[{"x":1,"y":2,"width":3,"height":4}]',
  };

  const doc = mapRow(row);

  assert.equal(doc.id, "uuid-1");
  assert.ok(doc.timestamp instanceof Date);
  assert.equal(doc.mass, 12.34);
  assert.equal(doc.scanDuration, 10);
  assert.equal(doc.confidence, 0.9);
  assert.equal(doc.numDetections, 2);
  assert.deepEqual(doc.detections, [{ x: 1, y: 2, width: 3, height: 4 }]);
});

test("ensureIndexes creates expected indexes", async () => {
  const calls = [];
  const col = {
    async createIndexes(indexes) {
      calls.push(indexes);
    },
  };

  await ensureIndexes(col);

  assert.equal(calls.length, 1);
  const indexes = calls[0];

  // This matches your current ensureIndexes definition
  assert.ok(indexes.find((i) => i.name === "by_id" && i.unique === true));
  assert.ok(indexes.find((i) => i.name === "by_scope_time"));
  assert.ok(indexes.find((i) => i.name === "by_site_time"));
});
