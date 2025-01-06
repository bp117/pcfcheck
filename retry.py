import os
import datetime
import oracledb  # or cx_Oracle

ORACLE_DSN = os.getenv("ORACLE_DSN", "myhost:1521/myservice")
ORACLE_USER = os.getenv("ORACLE_USER", "myuser")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "mypassword")

def get_oracle_connection():
    return oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )

def reassign_stale_tasks(conn):
    """
    Move tasks with groupid=9999 (our 'retry queue') back to a real groupid (0, 1, etc.).
    Or just set them NOT_STARTED with a new groupid for a working instance.
    """
    # Simple example: set them to groupid=0, status=NOT_STARTED
    sql = """
        UPDATE tasks
           SET groupid = 0,
               status = 'NOT_STARTED',
               errordesc = 'Reassigned by retry script',
               lastupdatedtime = :now
         WHERE groupid = 9999
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"now": datetime.datetime.now()})
    conn.commit()

def main():
    conn = get_oracle_connection()
    try:
        reassign_stale_tasks(conn)
        print("Stale tasks reassigned to groupid=0, status=NOT_STARTED.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
