import os
import datetime
import pandas as pd
import oracledb  # or: import cx_Oracle as oracledb

###############################################################################
# 1) Configuration: Reading from Excel & Oracle Credentials
###############################################################################
EXCEL_FILE_PATH = "data.xlsx"  # Path to your Excel file

# The names of the two worksheets
FACILITY_DOC_SHEET = "Facility to document mapping"
OBLIGOR_DATA_SHEET = "Obligor data"

# Oracle connection details.
# In a real TAS environment, these might come from VCAP_SERVICES or bound service.
ORACLE_DSN = os.getenv("ORACLE_DSN", "myhostname:1521/myservice")
ORACLE_USER = os.getenv("ORACLE_USER", "myuser")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "mypassword")

# PCF instance index => groupid
CF_INSTANCE_INDEX = int(os.getenv("CF_INSTANCE_INDEX", "0"))

###############################################################################
# 2) Connect to Oracle
###############################################################################
def get_oracle_connection():
    connection = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    return connection

###############################################################################
# 3) Create Table If Not Exists (including FACILITYID)
###############################################################################
CREATE_TABLE_SQL = """
BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE tasks (
            groupid INT,
            facilityid VARCHAR2(100),
            trancheid INT,
            fileseqno INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            filename VARCHAR2(100),
            status VARCHAR2(50),
            ignoreindicator NUMBER(1),
            filesize INT,
            lastupdatedtime TIMESTAMP,
            errordesc VARCHAR2(255)
        )
    ';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            -- ORA-00955: name is already used by an existing object => table exists
            NULL;
        ELSE
            RAISE;
        END IF;
END;
"""

def create_tasks_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

###############################################################################
# 4) Read the Excel Sheets
###############################################################################
def read_excel_data():
    """
    Loads the two sheets from the Excel file into pandas DataFrames.
    """
    df_facility = pd.read_excel(EXCEL_FILE_PATH, sheet_name=FACILITY_DOC_SHEET)
    df_obligor = pd.read_excel(EXCEL_FILE_PATH, sheet_name=OBLIGOR_DATA_SHEET)

    # If the second sheet calls it "Internal Credit Facility Id",
    # rename it to "InternalCreditFacilityID" to match the first sheet
    if "Internal Credit Facility Id" in df_obligor.columns:
        df_obligor.rename(columns={"Internal Credit Facility Id": "InternalCreditFacilityID"}, inplace=True)

    # If the first sheet has a column for facility ID but spelled differently,
    # rename it to "FacilityID" or some consistent name.
    # For example, if it's called "Facility Id" in the sheet:
    # if "Facility Id" in df_facility.columns:
    #     df_facility.rename(columns={"Facility Id": "FacilityID"}, inplace=True)

    return df_facility, df_obligor

###############################################################################
# 5) Merge & Identify Fresh Rows
###############################################################################
def merge_sheets_and_find_fresh(df_facility, df_obligor, conn):
    """
    Merges the two DataFrames on 'InternalCreditFacilityID' 
    and identifies which rows haven't yet been inserted into the tasks table.
    
    We'll assume each row needs:
      - FACILITYID (derived from the facility sheet or the internal ID)
      - TRANCHEID  (from df_obligor's 'TRANCHE_ID' column)
      - FILENAME   (from df_facility's 'FileName' column)
      - GROUPID    (PCF instance index)
    """

    merged_df = pd.merge(
        df_facility, df_obligor,
        on="InternalCreditFacilityID",
        how="left"
    )

    # For example, let's assume:
    # df_facility has "FileName" and "InternalCreditFacilityID" columns.
    # df_obligor has "TRANCHE_ID" and "InternalCreditFacilityID".
    # If there's a separate "FacilityID" column, rename or unify it below.

    existing_set = set()
    with conn.cursor() as cur:
        # We'll check uniqueness by (facilityid, trancheid, filename, groupid).
        cur.execute("""
            SELECT facilityid, trancheid, filename, groupid
              FROM tasks
        """)
        for row in cur:
            # row = (facilityid, trancheid, filename, groupid)
            existing_set.add(row)

    fresh_records = []
    for _, row in merged_df.iterrows():
        # Let's define how to derive each column:
        facility_id = str(row["InternalCreditFacilityID"])
        
        # If the facility sheet itself has a separate "FacilityID" column, 
        # use that instead. For now, we assume "InternalCreditFacilityID" = "FacilityID".
        
        tranche_id = row.get("TRANCHE_ID", None)
        if pd.isna(tranche_id):
            tranche_id = 0  # Or handle differently if missing

        filename = row.get("FileName", None)
        if pd.isna(filename):
            filename = None

        # Build the key to see if it's already inserted
        key = (facility_id, tranche_id, filename, CF_INSTANCE_INDEX)
        if key in existing_set:
            # Already inserted
            continue
        
        fresh_records.append({
            "facilityid": facility_id,
            "trancheid": int(tranche_id) if tranche_id else 0,
            "filename": filename,
        })

    return fresh_records

###############################################################################
# 6) Insert Fresh Rows into TASKS
###############################################################################
def insert_fresh_records(fresh_records, conn):
    """
    Inserts the new tasks into the Oracle table, setting:
      groupid        => CF_INSTANCE_INDEX (PCF instance)
      facilityid     => from record
      trancheid      => from record
      filename       => from record
      status         => 'NOT_STARTED'
      ignoreindicator => 0
      filesize       => 0
      lastupdatedtime => now
      errordesc      => NULL
    """

    sql = """
        INSERT INTO tasks (
            groupid,
            facilityid,
            trancheid,
            filename,
            status,
            ignoreindicator,
            filesize,
            lastupdatedtime,
            errordesc
        ) VALUES (
            :groupid,
            :facilityid,
            :trancheid,
            :filename,
            :status,
            :ignoreind,
            :filesize,
            :lastupdate,
            :errdesc
        )
    """

    now = datetime.datetime.now()

    with conn.cursor() as cur:
        for rec in fresh_records:
            cur.execute(sql, {
                "groupid": CF_INSTANCE_INDEX,
                "facilityid": rec["facilityid"],
                "trancheid": rec["trancheid"],
                "filename": rec["filename"],
                "status": "NOT_STARTED",
                "ignoreind": 0,  # 0 => false
                "filesize": 0,   # Could parse from filename if needed
                "lastupdate": now,
                "errdesc": None
            })
    conn.commit()

###############################################################################
# 7) Main: Putting It All Together
###############################################################################
def main():
    conn = get_oracle_connection()
    try:
        # 1) Create the tasks table if it doesn't exist (includes facilityid column).
        create_tasks_table_if_not_exists(conn)

        # 2) Read the Excel sheets into DataFrames.
        df_facility, df_obligor = read_excel_data()

        # 3) Merge them, find which records are fresh vs. already in tasks.
        fresh_records = merge_sheets_and_find_fresh(df_facility, df_obligor, conn)
        if not fresh_records:
            print("No new records to insert.")
            return
        
        print(f"Found {len(fresh_records)} fresh records. Inserting into tasks...")
        # 4) Insert the new tasks
        insert_fresh_records(fresh_records, conn)

        print(f"Inserted {len(fresh_records)} new rows into 'tasks'.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

-- For Oracle 12c+ where IDENTITY is supported:
CREATE TABLE tasks (
    groupid          NUMBER,               -- Maps to CF_INSTANCE_INDEX
    facilityid       VARCHAR2(100),        -- Stores Facility ID
    trancheid        NUMBER,
    fileseqno        NUMBER GENERATED ALWAYS AS IDENTITY, 
    filename         VARCHAR2(100),
    status           VARCHAR2(50),
    ignoreindicator  NUMBER(1),           -- 0 or 1 (simulating BOOLEAN)
    filesize         NUMBER,
    lastupdatedtime  TIMESTAMP,
    errordesc        VARCHAR2(255),
    CONSTRAINT tasks_pk PRIMARY KEY (fileseqno)
);
CREATE SEQUENCE tasks_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER tasks_bir 
BEFORE INSERT ON tasks
FOR EACH ROW
WHEN (NEW.fileseqno IS NULL)
BEGIN
  SELECT tasks_seq.NEXTVAL INTO :NEW.fileseqno FROM DUAL;
END;
/

