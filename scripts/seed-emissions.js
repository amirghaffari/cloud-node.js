/**
 * MongoDB seeding script for emissions.csv.gz
 *
 * Uses:
 * - Stream (no full-file buffering)
 * - Batch inserts (fast)
 * - Backpressure-aware (no runaway memory)
 * - Create indexes for query endpoint
 *
 * Usage:
 *   1) set DB_NAME=kuva_interview
 *   2) set MONGODB_URI="mongodb://admin:password@localhost:27017/?authSource=admin"
 *
 * Optional env:
 *   COLLECTION=emissions
 *   INPUT=fixtures/emissions.csv.gz
 *   BATCH_SIZE=5000
 */

const fs = require("node:fs");
const path = require("node:path");
const zlib = require("node:zlib");
const { pipeline } = require("node:stream/promises");

const { MongoClient } = require("mongodb");
const { parse } = require("csv-parse");

const MONGODB_URI =
  process.env.MONGODB_URI || "mongodb://admin:password@localhost:27017/?authSource=admin";
const DB_NAME = process.env.DB_NAME || "kuva_interview";
const COLLECTION = process.env.COLLECTION || "emissions";
const INPUT = process.env.INPUT || path.join(process.cwd(), "fixtures", "emissions.csv.gz"); // adjust if the file lives elsewhere
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 5000);

function toNumber(v) {
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error(`Invalid number: ${v}`);
  return n;
}

function toInt(v) {
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) throw new Error(`Invalid int: ${v}`);
  return n;
}

function toDate(v) {
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) throw new Error(`Invalid timestamp: ${v}`);
  return d;
}

function safeJsonParse(v) {
  try {
    return JSON.parse(v);
  } catch {
    return [];
  }
}

function mapRow(row) {
  // CSV columns from emissions-dataset.md:
  return {
    id: row.id,
    timestamp: toDate(row.timestamp),
    siteId: row.siteId,
    siteName: row.siteName,
    equipmentId: row.equipmentId,
    type: row.type,
    mass: toNumber(row.mass),
    unit: row.unit,
    scanDuration: toInt(row.scanDuration),
    confidence: toNumber(row.confidence),
    numDetections: toInt(row.numDetections),
    detections: safeJsonParse(row.detections),
  };
}

async function ensureIndexes(col) {
  await col.createIndexes([
    { key: { id: 1 }, name: "by_id", unique: true }, //prevents duplicates on reruns
    { key: { siteId: 1, equipmentId: 1, timestamp: -1, _id: -1 }, name: "by_scope_time" },
    { key: { siteId: 1, timestamp: -1, _id: -1 }, name: "by_site_time" },
  ]);
}

async function main() {
  const client = new MongoClient(MONGODB_URI, {
    maxPoolSize: 10,
  });

  console.log(`Connecting to MongoDB: ${MONGODB_URI}`);
  await client.connect();
  const db = client.db(DB_NAME);
  const col = db.collection(COLLECTION);

  console.log(`Using DB=${DB_NAME}, collection=${COLLECTION}`);
  await ensureIndexes(col);

  // Optional: clear existing data (comment out if you want append-only)
  // await col.deleteMany({});

  let batch = [];
  let inserted = 0;
  let skipped = 0;
  let start = Date.now();

  const csvParser = parse({
    columns: true, // maps rows to object by header
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
  });

  async function flushBatch() {
    if (batch.length === 0) return;

    const docs = batch;
    batch = [];

    try {
      // insertMany is fastest for pure inserts.
      // ordered:false continues on dup key errors (if rerun)
      const res = await col.insertMany(docs, { ordered: false });
      inserted += res.insertedCount;
    } catch (err) {
      // Handle duplicate keys or partial successes.
      // Mongo bulk errors carry "writeErrors" and may still insert some docs.
      if (err && err.writeErrors) {
        const dupes = err.writeErrors.filter((e) => e.code === 11000).length;
        skipped += dupes;

        // Some inserts may still have succeeded:
        const successful =
          (err.result && err.result.nInserted) || (err.result && err.result.insertedCount) || 0;
        inserted += successful;
      } else {
        throw err;
      }
    }

    const elapsedSec = (Date.now() - start) / 1000;
    const rate = Math.round((inserted + skipped) / Math.max(elapsedSec, 1));
    console.log(`Progress: inserted=${inserted} skipped=${skipped} rate≈${rate}/s`);
  }

  // Backpressure-aware consumption:
  // pause the parser while flushing to keep memory steady.
  csvParser.on("data", (row) => {
    try {
      batch.push(mapRow(row));
    } catch (e) {
      // If a bad row ever happens, skip and continue
      skipped += 1;
      return;
    }

    if (batch.length >= BATCH_SIZE) {
      csvParser.pause();
      flushBatch()
        .then(() => csvParser.resume())
        .catch((e) => csvParser.destroy(e));
    }
  });

  await pipeline(fs.createReadStream(INPUT), zlib.createGunzip(), csvParser);

  // Flush remaining docs
  await flushBatch();

  const totalSec = (Date.now() - start) / 1000;
  console.log(`DONE. inserted=${inserted} skipped=${skipped} elapsed=${totalSec.toFixed(1)}s`);

  await client.close();
}

if (require.main === module) {
  main().catch((err) => {
    console.error("Seeding failed:", err);
    process.exit(1);
  });
}

module.exports = {
  toNumber,
  toInt,
  toDate,
  safeJsonParse,
  mapRow,
  ensureIndexes,
};
