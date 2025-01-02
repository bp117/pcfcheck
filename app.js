/*******************************************************
 * app.js
 *
 * Node.js version of the PCF/TAS task simulation.
 *******************************************************/

const express = require('express');
const { Pool } = require('pg');

// 1) Environment Variables
//    - CF_INSTANCE_INDEX: Provided by TAS/PCF. Defaults to 0 if not found.
const INSTANCE_INDEX = parseInt(process.env.CF_INSTANCE_INDEX || '0', 10);

// 2) Hard-coded fallback VCAP_SERVICES if none is provided
//    (Used if you're not binding a real Postgres service.)
const FALLBACK_VCAP = {
  "user-provided": [
    {
      "name": "my-fallback-postgres-service",
      "credentials": {
        "host": "localhost",
        "port": 5432,
        "name": "postgres",
        "username": "postgres",
        "password": "password"
      }
    }
  ]
};

/*******************************************************
 * Parse VCAP_SERVICES from Environment or Fallback
 *******************************************************/
function parseVCAPServices() {
  let vcap;
  try {
    vcap = JSON.parse(process.env.VCAP_SERVICES || JSON.stringify(FALLBACK_VCAP));
  } catch (e) {
    console.error("Error parsing VCAP_SERVICES:", e);
    vcap = FALLBACK_VCAP;
  }

  // Find a service with credentials
  for (const svcType of Object.keys(vcap)) {
    for (const svc of vcap[svcType]) {
      if (svc.credentials) {
        const c = svc.credentials;
        return {
          host: c.host,
          port: c.port,
          database: c.name || c.database,
          user: c.username,
          password: c.password
        };
      }
    }
  }

  throw new Error("No valid Postgres credentials found in VCAP_SERVICES or fallback.");
}

// 3) Build PG Pool using parsed credentials
const dbConfig = parseVCAPServices();
const pool = new Pool({
  host: dbConfig.host,
  port: dbConfig.port,
  database: dbConfig.database,
  user: dbConfig.user,
  password: dbConfig.password
});

// 4) Express App Setup
const app = express();

// -----------------------------------------------------
// SQL Statements
// -----------------------------------------------------
const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS tasks (
  groupid INT,
  trancheid INT,
  fileseqno SERIAL PRIMARY KEY,
  filename VARCHAR(100),
  status VARCHAR(50),
  ignoreIndicator BOOLEAN,
  filesize INT,
  lastupdatedtime TIMESTAMP,
  errorDesc VARCHAR(255)
);
`;

const INSERT_SQL = `
INSERT INTO tasks (
  groupid,
  trancheid,
  filename,
  status,
  ignoreIndicator,
  filesize,
  lastupdatedtime,
  errorDesc
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
`;

const SELECT_TASKS_SQL = `
SELECT *
FROM tasks
WHERE
  status = 'NOT_STARTED'
  AND ignoreIndicator = false
ORDER BY fileseqno
`;

const UPDATE_IN_PROGRESS_SQL = `
UPDATE tasks
SET
  status = 'IN_PROGRESS',
  lastupdatedtime = $1
WHERE fileseqno = $2
`;

const UPDATE_FINAL_SQL = `
UPDATE tasks
SET
  status = $1,
  lastupdatedtime = $2,
  errorDesc = $3
WHERE fileseqno = $4
`;

const SELECT_STALE_TASKS_SQL = `
SELECT * FROM tasks
WHERE status = 'IN_PROGRESS'
  AND lastupdatedtime < $1
`;

const UPDATE_STALE_SQL = `
UPDATE tasks
SET
  status = 'FAILED',
  errorDesc = 'Instance down or did not process in time'
WHERE fileseqno = $1
`;

// -----------------------------------------------------
// Database Helper Functions
// -----------------------------------------------------
async function createTable() {
  const client = await pool.connect();
  try {
    await client.query(CREATE_TABLE_SQL);
  } finally {
    client.release();
  }
}

async function insertSyntheticData(numRecords = 10) {
  const client = await pool.connect();
  try {
    for (let i = 0; i < numRecords; i++) {
      const groupid = getRandomInt(0, 2);
      const trancheid = getRandomInt(0, 2);
      const filename = randomString(8) + ".txt";
      const status = 'NOT_STARTED';
      const ignoreIndicator = false;
      const filesize = getRandomInt(1000, 5000);
      const lastupdatedtime = new Date();
      const errorDesc = null;

      await client.query(INSERT_SQL, [
        groupid,
        trancheid,
        filename,
        status,
        ignoreIndicator,
        filesize,
        lastupdatedtime,
        errorDesc
      ]);
    }
  } finally {
    client.release();
  }
}

async function selectTasksForInstance() {
  // 1) Grab tasks that are NOT_STARTED, ignoreIndicator=false
  const client = await pool.connect();
  try {
    const res = await client.query(SELECT_TASKS_SQL);
    const allTasks = res.rows;
    // 2) Filter tasks where groupid == INSTANCE_INDEX OR trancheid == INSTANCE_INDEX
    return allTasks.filter(
      t => t.groupid === INSTANCE_INDEX || t.trancheid === INSTANCE_INDEX
    );
  } finally {
    client.release();
  }
}

async function markInProgress(fileseqno) {
  const client = await pool.connect();
  try {
    await client.query(UPDATE_IN_PROGRESS_SQL, [new Date(), fileseqno]);
  } finally {
    client.release();
  }
}

async function markFinalStatus(fileseqno, finalStatus, errorDesc = null) {
  const client = await pool.connect();
  try {
    await client.query(UPDATE_FINAL_SQL, [finalStatus, new Date(), errorDesc, fileseqno]);
  } finally {
    client.release();
  }
}

async function markStaleTasks() {
  // Tasks are stale if IN_PROGRESS longer than 1 minute
  const cutoffTime = new Date(Date.now() - 60 * 1000); // 1 min ago
  const client = await pool.connect();
  try {
    const res = await client.query(SELECT_STALE_TASKS_SQL, [cutoffTime]);
    const staleTasks = res.rows;
    for (const task of staleTasks) {
      console.log(`[ALERT] Task ${task.fileseqno} is stale. Marking as FAILED.`);
      await client.query(UPDATE_STALE_SQL, [task.fileseqno]);
    }
  } finally {
    client.release();
  }
}

// -----------------------------------------------------
// Task Processor (Background Simulation)
// -----------------------------------------------------
async function taskProcessorLoop() {
  console.log(`[Instance ${INSTANCE_INDEX}] Starting task processor loop...`);

  while (true) {
    try {
      // 1) Mark stale tasks
      await markStaleTasks();

      // 2) Fetch tasks for this instance
      const tasks = await selectTasksForInstance();
      if (tasks.length === 0) {
        console.log(`[Instance ${INSTANCE_INDEX}] No tasks. Sleeping 10s...`);
        await sleep(10000);
        continue;
      }

      // 3) Process each task
      for (const task of tasks) {
        console.log(`[Instance ${INSTANCE_INDEX}] Acquired task fileseqno=${task.fileseqno}`);
        await markInProgress(task.fileseqno);

        // Simulate random processing delay (5-10s)
        const delay = getRandomInt(5, 10) * 1000;
        console.log(`[Instance ${INSTANCE_INDEX}] Processing task ${task.fileseqno} for ${delay / 1000}s...`);
        await sleep(delay);

        // Random success (80%) or fail (20%)
        if (Math.random() < 0.8) {
          await markFinalStatus(task.fileseqno, "SUCCESS", "No Error");
          console.log(`[Instance ${INSTANCE_INDEX}] Task ${task.fileseqno} SUCCESS.`);
        } else {
          await markFinalStatus(task.fileseqno, "FAILED", "Simulated error.");
          console.log(`[Instance ${INSTANCE_INDEX}] Task ${task.fileseqno} FAILED.`);
        }
      }

      console.log(`[Instance ${INSTANCE_INDEX}] Finished a batch. Sleeping 5s...`);
      await sleep(5000);
    } catch (err) {
      console.error(`[Instance ${INSTANCE_INDEX}] Error in task loop:`, err);
      // In real code, handle or retry. We'll just pause then keep going.
      await sleep(5000);
    }
  }
}

// -----------------------------------------------------
// Utility Functions
// -----------------------------------------------------
function getRandomInt(min, max) {
  // inclusive range
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomString(length) {
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  let str = '';
  for (let i = 0; i < length; i++) {
    str += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return str;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// -----------------------------------------------------
// Express Routes
// -----------------------------------------------------
app.get('/', (req, res) => {
  res.send(`Hello from Instance ${INSTANCE_INDEX}!<br>
            Using DB Host: ${dbConfig.host}, DB: ${dbConfig.database}`);
});

// -----------------------------------------------------
// Main Startup
// -----------------------------------------------------
async function main() {
  try {
    // 1) Ensure table exists
    await createTable();

    // 2) Insert synthetic tasks (only if you want to do this every start)
    console.log("[INFO] Inserting synthetic data...");
    await insertSyntheticData(15);

    // 3) Start background task loop
    taskProcessorLoop(); // runs indefinitely

    // 4) Start Express server
    const port = process.env.PORT || 8080;
    app.listen(port, () => {
      console.log(`[Instance ${INSTANCE_INDEX}] App listening on port ${port}`);
    });
  } catch (e) {
    console.error("Failed to start application:", e);
    process.exit(1);
  }
}

main().catch(err => {
  console.error("Unhandled error in main:", err);
  process.exit(1);
});
