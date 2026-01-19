const { ObjectId } = require("mongodb");

async function emissionsRoutes(server, options) {
  const { emissionsCol } = options;

  server.get(
    "/emissions",
    {
      schema: {
        querystring: {
          type: "object",
          required: ["siteId", "equipmentId"],
          additionalProperties: false,
          properties: {
            siteId: { type: "string", minLength: 1 },
            equipmentId: { type: "string", minLength: 1 },
            from: { type: "string" },
            to: { type: "string" },
            confidenceMin: { type: "number", minimum: 0, maximum: 1, default: 0.75 },
            limit: { type: "integer", minimum: 1, maximum: 1000, default: 100 },
            includeDetections: { type: "boolean", default: false },

            // Pagination cursor
            cursorTs: { type: "string" }, // timestamp of last item from previous page
            cursorId: { type: "string" }, // _id of last item from previous page
          },
        },
      },
    },
    async (req, reply) => {
      try {
        const {
          siteId,
          equipmentId,
          from,
          to,
          confidenceMin,
          limit,
          includeDetections,
          cursorTs,
          cursorId,
        } = req.query;

        // Validate from/to
        const timeFilter = {};
        if (from) {
          const d = new Date(from);
          if (isNaN(d)) return reply.code(400).send({ error: "Invalid 'from' timestamp" });
          timeFilter.$gte = d;
        }
        if (to) {
          const d = new Date(to);
          if (isNaN(d)) return reply.code(400).send({ error: "Invalid 'to' timestamp" });
          timeFilter.$lte = d;
        }
        if (timeFilter.$gte && timeFilter.$lte && timeFilter.$gte > timeFilter.$lte) {
          return reply.code(400).send({ error: "'from' must be <= 'to'" });
        }

        // Base filter
        const filter = {
          siteId,
          equipmentId,
          confidence: { $gte: confidenceMin },
        };
        if (Object.keys(timeFilter).length) filter.timestamp = timeFilter;

        // Cursor pagination filter (fetch "older" than last item)
        if (cursorTs || cursorId) {
          if (!cursorTs || !cursorId) {
            return reply
              .code(400)
              .send({ error: "Both cursorTs and cursorId are required together" });
          }

          const ts = new Date(cursorTs);
          if (isNaN(ts)) {
            return reply.code(400).send({ error: "Invalid 'cursorTs' timestamp" });
          }

          let oid;
          try {
            oid = new ObjectId(cursorId);
          } catch {
            return reply.code(400).send({ error: "Invalid 'cursorId' ObjectId" });
          }

          // sort by timestamp desc, _id desc:
          // next page should be items where (timestamp < ts) OR (timestamp == ts AND _id < oid)
          filter.$or = [{ timestamp: { $lt: ts } }, { timestamp: ts, _id: { $lt: oid } }];
        }

        const projection = includeDetections
          ? { _id: 1, siteId: 1, equipmentId: 1, timestamp: 1, confidence: 1, detections: 1 }
          : { _id: 1, siteId: 1, equipmentId: 1, timestamp: 1, confidence: 1 };

        // Fetch limit + 1 so we can tell if there is another page
        const docs = await emissionsCol
          .find(filter, { projection })
          .sort({ timestamp: -1, _id: -1 })
          .limit(limit + 1)
          .toArray();

        const hasMore = docs.length > limit;
        const items = hasMore ? docs.slice(0, limit) : docs;

        // Build nextCursor from the last returned item
        let nextCursor = null;
        if (hasMore && items.length) {
          const last = items[items.length - 1];
          nextCursor = {
            cursorTs: last.timestamp.toISOString(),
            cursorId: String(last._id),
          };
        }

        return reply.send({
          count: items.length,
          siteId,
          equipmentId,
          nextCursor, // null if no more
          items,
        });
      } catch (err) {
        req.log.error(err);
        return reply.code(500).send({ error: "Internal server error" });
      }
    },
  );
}

module.exports = emissionsRoutes;
