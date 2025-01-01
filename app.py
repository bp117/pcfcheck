import os
import json
import random
import string
import time
import datetime
import threading

from flask import Flask
import psycopg2
import psycopg2.extras


###############################################################################
# 1) Flask App and Configuration
###############################################################################
app = Flask(__name__)

# This simulates the CF_INSTANCE_INDEX (TAS sets this automatically).
# Default to 0 if not found (for local dev).
INSTANCE_INDEX = int(os.getenv("CF_INSTANCE_INDEX", "0"))

# Hard-coded fallback "VCAP_SERVICES" if not provided by environment.
# Use this to run locally or if you don't bind a real service.
FALLBACK_VCAP = {
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
}


def parse_vcap_services():
    """
    Parse VCAP_SERVICES from environment or fallback to hard-coded.
    Returns a dict with keys: host, port, dbname, user, password.
    """
    vcap_json = os.getenv("VCAP_SERVICES")
    if not vcap_json:
        # No real VCAP_SERVICES, so use our fallback
        vcap_json = json.dumps(FALLBACK_VCAP)

    services = json.loads(vcap_json)
    for _, svc_list in services.items():
        for svc in svc_list:
            if "credentials" in svc:
                c = svc["credentials"]
                return {
                    "host": c.get("host"),
                    "port": c.get("port"),
                    "dbname": c.get("name") or c.get("database"),
                    "user": c.get("username"),
                    "password": c.get("password")
                }
    return None


DB_CONFIG = parse_vcap_services()
if not DB_CONFIG:
    raise RuntimeError("Could not parse DB credentials from VCAP_SERVICES or fallback.")


###############################################################################
# 2) PostgreSQL Table Setup and Queries
###############################################################################

CREATE_TABLE_SQL = """
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
"""

INSERT_SQL = """
INSERT INTO tasks (
    groupid,
    trancheid,
    filename,
    status,
    ignoreIndicator,
    filesize,
    lastupdatedtime,
    errorDesc
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

SELECT_TASKS_SQL = """
SELECT *
FROM tasks
WHERE
    status = 'NOT_STARTED'
    AND ignoreIndicator = false
ORDER BY fileseqno
"""

UPDATE_IN_PROGRESS_SQL = """
UPDATE tasks
SET
    status = 'IN_PROGRESS',
    lastupdatedtime = %s
WHERE fileseqno = %s
"""

UPDATE_FINAL_SQL = """
UPDATE tasks
SET
    status = %s,
    lastupdatedtime = %s,
    errorDesc = %s
WHERE fileseqno = %s
"""

SELECT_STALE_TASKS_SQL = """
SELECT * FROM tasks
WHERE status = 'IN_PROGRESS'
  AND lastupdatedtime < %s
"""

UPDATE_STALE_SQL = """
UPDATE tasks
SET
    status = 'FAILED',
    errorDesc = 'Instance down or did not process in time'
WHERE fileseqno = %s
"""


def get_connection():
    """
    Create a new PostgreSQL connection using DB_CONFIG.
    """
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )


def create_table():
    """
    Create the 'tasks' table if it doesn't already exist.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


def insert_synthetic_data(num_records=10):
    """
    Insert synthetic tasks for demonstration.
    - groupid and trancheid are random (0..2) so multiple instances can pick them up.
    - status is 'NOT_STARTED' so they are eligible for processing.
    - ignoreIndicator = false so we don't skip them.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for _ in range(num_records):
                groupid = random.randint(0, 2)
                trancheid = random.randint(0, 2)
                filename = ''.join(random.choices(string.ascii_lowercase, k=8)) + ".txt"
                status = 'NOT_STARTED'
                ignore_indicator = False
                filesize = random.randint(1000, 5000)
                lastupdatedtime = datetime.datetime.now()
                errorDesc = None

                cur.execute(INSERT_SQL, (
                    groupid,
                    trancheid,
                    filename,
                    status,
                    ignore_indicator,
                    filesize,
                    lastupdatedtime,
                    errorDesc
                ))
        conn.commit()
    finally:
        conn.close()


def select_tasks_for_instance():
    """
    1. Select tasks where status = 'NOT_STARTED' and ignoreIndicator = false.
    2. Filter tasks by groupid == INSTANCE_INDEX OR trancheid == INSTANCE_INDEX.
    3. Return them in ascending order of fileseqno.
    """
    conn = get_connection()
    rows = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_TASKS_SQL)
            all_tasks = cur.fetchall()
            # Filter tasks for this instance
            tasks_for_this_instance = [
                row for row in all_tasks
                if row["groupid"] == INSTANCE_INDEX or row["trancheid"] == INSTANCE_INDEX
            ]
            rows = tasks_for_this_instance
    finally:
        conn.close()
    return rows


def mark_in_progress(fileseqno):
    """
    Set task status to 'IN_PROGRESS' and update lastupdatedtime = now.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(UPDATE_IN_PROGRESS_SQL, (datetime.datetime.now(), fileseqno))
        conn.commit()
    finally:
        conn.close()


def mark_final_status(fileseqno, final_status, error_desc=None):
    """
    Mark task as 'SUCCESS' or 'FAILED' with updated timestamp and optional errorDesc.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                UPDATE_FINAL_SQL,
                (final_status, datetime.datetime.now(), error_desc, fileseqno)
            )
        conn.commit()
    finally:
        conn.close()


def mark_stale_tasks():
    """
    If a task is IN_PROGRESS but hasn't been updated in over 1 minute,
    mark it as FAILED (instance presumably down).
    """
    one_min_ago = datetime.datetime.now() - datetime.timedelta(minutes=1)
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SELECT_STALE_TASKS_SQL, (one_min_ago,))
            stale_tasks = cur.fetchall()
            for task in stale_tasks:
                fileseqno = task["fileseqno"]
                print(f"[ALERT] Task {fileseqno} is stale. Marking as FAILED.")
                cur.execute(UPDATE_STALE_SQL, (fileseqno,))
        conn.commit()
    finally:
        conn.close()


###############################################################################
# 3) Background Simulation (Task Processor Loop)
###############################################################################
def task_processor_loop():
    """
    Continuously:
    1. Mark stale tasks as FAILED if they're IN_PROGRESS longer than 1 minute.
    2. Fetch tasks that this instance should process.
    3. Mark them IN_PROGRESS, sleep a random amount, then mark SUCCESS/FAILED.
    4. Repeat.
    """
    print(f"[Instance {INSTANCE_INDEX}] Starting task processor loop...")
    while True:
        # 1) Mark stale tasks
        mark_stale_tasks()

        # 2) Fetch tasks for this instance
        tasks_to_process = select_tasks_for_instance()
        if not tasks_to_process:
            print(f"[Instance {INSTANCE_INDEX}] No tasks to process. Sleeping 10s...")
            time.sleep(10)
            continue

        # 3) Process each task
        for task in tasks_to_process:
            fileseqno = task["fileseqno"]
            print(f"[Instance {INSTANCE_INDEX}] Acquired task fileseqno={fileseqno}")

            mark_in_progress(fileseqno)

            # Simulate random processing time
            delay = random.randint(5, 10)
            print(f"[Instance {INSTANCE_INDEX}] Processing task {fileseqno} for {delay}s...")
            time.sleep(delay)

            # Randomly decide success/fail (80% success)
            if random.random() < 0.8:
                mark_final_status(fileseqno, "SUCCESS", "No Error")
                print(f"[Instance {INSTANCE_INDEX}] Task {fileseqno} SUCCESS.")
            else:
                mark_final_status(fileseqno, "FAILED", "Simulated error.")
                print(f"[Instance {INSTANCE_INDEX}] Task {fileseqno} FAILED.")

        print(f"[Instance {INSTANCE_INDEX}] Finished a batch of tasks. Sleeping 5s...")
        time.sleep(5)


###############################################################################
# 4) Flask Routes
###############################################################################
@app.route("/")
def home():
    return (f"Hello from Instance {INSTANCE_INDEX}!<br>"
            f"Using DB Host: {DB_CONFIG['host']} DB Name: {DB_CONFIG['dbname']}")


###############################################################################
# 5) Application Entry Point
###############################################################################
def main():
    # Create table if needed
    create_table()

    # Insert synthetic data (if you only want to do this once, you can comment it out)
    print("[INFO] Inserting synthetic data...")
    insert_synthetic_data(num_records=15)

    # Start background thread to process tasks
    thread = threading.Thread(target=task_processor_loop, daemon=True)
    thread.start()

    # Run the Flask app
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
