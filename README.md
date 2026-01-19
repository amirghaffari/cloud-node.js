This coding challenge is designed to assess your ability to work with data processing and Node.js back-end development. Your implementation will be reviewed and discussed during the technical interview, where we'll explore your decisions, approach to performance, and problem-solving process.

## Requirements

- Node.js 24
- PNPM 10
- Docker

## Getting Started

Install dependencies:

```
pnpm install
```

Run MongoDB:
```
docker-compose up
```

Run the server:

```
pnpm start
```

## Tasks

* **Write a script to seed the database with the emissions in `fixtures/emissions.csv.gz`.** The script should read the gzipped file and save the data to a MongoDB collection efficiently.
* **Create a `GET` endpoint to query the seeded emissions data by siteId and equipmentId.** The endpoint should allow for querying based on a time range, and to filter by a confidence threshold.

Use your best judgement to implement a performant endpoint, and include validations and error handling.

### Additional Notes

* `equipment` belongs to `sites`. Sites typically have more than 1 equipment.
---
## Design & Implementation Notes

### Data Import (Seeding)

The database seeding script is designed to handle large datasets efficiently and safely:

- **Streaming** is used to read the gzipped CSV file, avoiding loading the full file into memory.
- **Batch inserts** (`insertMany`) significantly improve write throughput.
- **Backpressure handling** (pause/resume on the CSV parser) ensures memory usage remains stable even for large inputs.
- **Defensive parsing** is applied for numeric, date, and JSON fields to safely handle malformed or unexpected data without failing the entire import.

This approach allows the import process to scale to large datasets while remaining robust.

### Indexing

Indexes are created to support the API’s query patterns:

- A **unique index on `id`** to prevent duplicates on re-runs.
- **Compound indexes on `(siteId, equipmentId, timestamp, _id)`** (and site-level variants) to optimize filtering and sorting.

### API Endpoint

The `GET /emissions` endpoint:

- Requires **`siteId`** and **`equipmentId`**
- Supports filtering by:
  - **Time range** using `from` / `to`
  - **Confidence threshold** using `confidenceMin`
- Validates timestamps and enforces a bounded **`limit`** parameter

**Example requests**

First page:
http://127.0.0.1:3000/emissions?siteId=4fb96f75-6828-5673-c4gd-3d074g77bgb7&equipmentId=e5f6a7b8-9012-34ef-0123-567890abcdef&limit=150


Next page (cursor-based pagination):
http://127.0.0.1:3000/emissions?siteId=4fb96f75-6828-5673-c4gd-3d074g77bgb7&equipmentId=e5f6a7b8-9012-34ef-0123-567890abcdef&limit=150&cursorId=696c40682de1d0bbca8aa236&cursorTs=2025-12-31T19:00:49.184Z

### Pagination

Cursor-based pagination is used, ordered by `(timestamp, _id)`, providing consistent and performant pagination without offset scans.

### Testing

Unit tests cover both the API behavior and the seeding helpers, using Node.js’s built-in test runner with no external dependencies.

## How to Run & Test the Project

### Start MongoDB
Run MongoDB using Docker Compose:

```
docker-compose up
```

This starts a MongoDB instance with authentication enabled using the credentials defined in docker-compose.yml.

### Seed the Database from the gzipped CSV file
``` 
pnpm seed
```

### Run the Application Server
``` 
pnpm start
```
The server will be available at http://127.0.0.1:3000

### Run Unit Tests

Execute the unit tests using Node.js’s built-in test runner:
``` 
pnpm run test
 ```
 This runs tests for both the API endpoints and the data seeding helpers without requiring a database connection.
 
 ### Configuration

The application supports the following environment variables:

- **`DB_NAME`**  
  Database name (default: `kuva_interview`)

- **`MONGODB_URI`**  
  MongoDB connection string.  
  By default, it uses the same authentication credentials defined in `docker-compose.yml`, but this can be overridden via this variable.

- **`COLLECTION`**  
  MongoDB collection name (default: `emissions`)

- **`INPUT`**  
  Path to the input gzipped CSV file  
  (default: `fixtures/emissions.csv.gz`)

- **`BATCH_SIZE`**  
  Number of records inserted per batch during seeding  
  (default: `5000`)

 
 