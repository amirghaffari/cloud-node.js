const fastify = require("fastify");
const { MongoClient } = require("mongodb");

const server = fastify({ logger: true });

let mongoClient;
let emissionsCol;

const emissionsRoutes = require("./routes/emissions.routes");

server.get("/health", async () => ({ status: "ok" }));

async function initMongo() {
  const MONGODB_URI =
    process.env.MONGODB_URI || "mongodb://admin:password@localhost:27017/?authSource=admin";
  mongoClient = new MongoClient(MONGODB_URI, { maxPoolSize: 10 });
  await mongoClient.connect();

  const db = mongoClient.db(process.env.DB_NAME || "kuva_interview");
  emissionsCol = db.collection(process.env.COLLECTION || "emissions");

  server.log.info("MongoDB connected");
}

const start = async () => {
  try {
    const port = process.env.PORT || 3000;
    const host = process.env.HOST || "0.0.0.0";

    // Initialize DB
    await initMongo();
    // Register routes once DB is ready
    await server.register(emissionsRoutes, {
      emissionsCol,
    });
    server.log.info("\n" + server.printRoutes());
    await server.listen({ port, host });

    server.log.info(`Server listening on ${host}:${port}`);
  } catch (err) {
    server.log.error(err);

    process.exit(1);
  }
};

const gracefulShutdown = async (signal) => {
  server.log.info(`Received ${signal}. Starting graceful shutdown...`);

  try {
    await server.close();

    server.log.info("Server closed successfully");
    if (mongoClient) {
      await mongoClient.close();
      server.log.info("MongoDB connection closed");
    }
    process.exit(0);
  } catch (err) {
    server.log.error("Error during shutdown:", err);

    process.exit(1);
  }
};

module.exports = { server, start, gracefulShutdown };
