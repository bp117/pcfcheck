import os
import time
import datetime
import random  # For simulation of success/failure
import oracledb  # or "import cx_Oracle as oracledb"

###############################################################################
# Configuration
###############################################################################
ORACLE_DSN = os.getenv("ORACLE_DSN", "myhost:1521/myservice")
ORACLE_USER = os.getenv("ORACLE_USER", "myuser")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "mypassword")

# On TAS, CF_INSTANCE_INDEX is automatically set. Default to 0 if running locally.
CF_INSTANCE_INDEX = int(os.getenv("CF_INSTANCE_INDEX", "0"))

# Simulate how we consider a task "stuck" (in minutes)
STALE_THRESHOLD_MINUTES = 1

def get_oracle_connection():
    """
    Connect to Oracle using oracledb or cx_Oracle.
    """
    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )

###############################################################################
# Step 1: Fetch a Task (NOT_STARTED) for This Instance
###############################################################################
def fetch_next_task(conn):
    """
    Retrieves the next NOT_STARTED task for this instance, ordered by fileseqno or any logic.
    Returns a dict with task info, or None if no task is found.
    """
    sql = """
        SELECT fileseqno, filename, facilityid
          FROM tasks
         WHERE groupid = :groupid
           AND status = 'NOT_STARTED'
         ORDER BY fileseqno
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"groupid": CF_INSTANCE_INDEX})
        row = cur.fetchone()
        if row:
            fileseqno, filename, facilityid = row
            return {
                "fileseqno": fileseqno,
                "filename": filename,
                "facilityid": facilityid
            }
    return None

###############################################################################
# Step 2: Mark Task IN_PROGRESS
###############################################################################
def mark_in_progress(conn, fileseqno):
    """
    Update the status to IN_PROGRESS and record lastupdatedtime = NOW.
    """
    sql = """
        UPDATE tasks
           SET status = 'IN_PROGRESS',
               lastupdatedtime = :now
         WHERE fileseqno = :seq
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "seq": fileseqno,
            "now": datetime.datetime.now()
        })
    conn.commit()

###############################################################################
# Step 3: Simulate Fetching from NAS & Normalization
###############################################################################
def process_file_from_nas(filename, facilityid):
    """
    Placeholder: 
      - 'Download' or 'read' the file from an NAS path based on `filename`.
      - Extract or 'normalize' the contents.
      - Return True if success, False if there's a failure.

    Here, we'll randomly succeed 80% of the time and fail 20% as a simulation.
    """
    print(f"Simulating NAS fetch for file: {filename}, facility: {facilityid}")
    time.sleep(2)  # Simulate some delay
    # 80% chance success
    return random.random() < 0.8

###############################################################################
# Step 4: Mark Task Success or Failure
###############################################################################
def mark_task_outcome(conn, fileseqno, success, error_desc=None):
    """
    If success => status=SUCCESS
    Else => status=FAILED and errordesc=error_desc
    lastupdatedtime=NOW
    """
    new_status = "SUCCESS" if success else "FAILED"
    sql = """
        UPDATE tasks
           SET status = :new_status,
               lastupdatedtime = :now,
               errordesc = :desc
         WHERE fileseqno = :seq
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "new_status": new_status,
            "seq": fileseqno,
            "now": datetime.datetime.now(),
            "desc": error_desc
        })
    conn.commit()

###############################################################################
# Step 5: Detect & Handle Stale Tasks
###############################################################################
def mark_stale_tasks_for_retry(conn):
    """
    If a task is in IN_PROGRESS for more than STALE_THRESHOLD_MINUTES,
    we consider the instance handling it as 'down'.
    
    We'll demonstrate a simple approach: set status = 'NOT_STARTED'
    and maybe change groupid to a special 'RETRY' group or keep as is.
    Then a *separate script* or instance can pick it up next time.
    
    Alternatively, you could set status='RETRY_NEEDED' or some other approach.
    """
    cutoff_time = datetime.datetime.now() - datetime.timedelta(minutes=STALE_THRESHOLD_MINUTES)

    # Fetch tasks that are stuck in IN_PROGRESS beyond the cutoff
    sql_select = """
        SELECT fileseqno
          FROM tasks
         WHERE status = 'IN_PROGRESS'
           AND lastupdatedtime < :cutoff
    """

    # We'll "move" them to another status or group for a separate retry script
    sql_update = """
        UPDATE tasks
           SET status = 'NOT_STARTED',
               groupid = 9999, -- special group for the "retry script"
               errordesc = 'Instance died or timed out. Moved to retry.'
         WHERE fileseqno = :seq
    """

    with conn.cursor() as cur:
        cur.execute(sql_select, {"cutoff": cutoff_time})
        stuck_rows = [r[0] for r in cur.fetchall()]
        for fileseqno in stuck_rows:
            print(f"[ALERT] Task {fileseqno} stale. Moving to retry script.")
            cur.execute(sql_update, {"seq": fileseqno})
    conn.commit()

###############################################################################
# Main Loop
###############################################################################
def main():
    print(f"Starting worker for Instance Index = {CF_INSTANCE_INDEX}")
    conn = get_oracle_connection()
    try:
        while True:
            # 1) Mark stale tasks (which belong to any instance but got stuck)
            mark_stale_tasks_for_retry(conn)

            # 2) Fetch next task for this instance
            task = fetch_next_task(conn)
            if not task:
                print("No tasks for this instance. Sleeping 5s...")
                time.sleep(5)
                continue

            fileseqno = task["fileseqno"]
            filename = task["filename"]
            facilityid = task["facilityid"]

            print(f"[Instance {CF_INSTANCE_INDEX}] Got task fileseqno={fileseqno} => {filename}")

            # 3) Mark as IN_PROGRESS
            mark_in_progress(conn, fileseqno)

            # 4) Simulate processing (fetch from NAS + normalization)
            success = process_file_from_nas(filename, facilityid)

            if success:
                # 5) Mark as SUCCESS
                mark_task_outcome(conn, fileseqno, True, error_desc=None)
                print(f"[Instance {CF_INSTANCE_INDEX}] Task {fileseqno} SUCCESS.")
            else:
                # 5) Mark as FAILED
                mark_task_outcome(conn, fileseqno, False, error_desc="Error normalizing file.")
                print(f"[Instance {CF_INSTANCE_INDEX}] Task {fileseqno} FAILED.")

            # Repeat until no tasks left for this instance.
    finally:
        conn.close()

if __name__ == "__main__":
    main()
