from importlib.resources import files
import os
import sys
import logging
from datetime import datetime
from unittest import result
from pandas import pd
import psycopg2 
from dotenv import load_dotenv
from bronze_batch_audit import build_batch_audit, connect_postgres


# -------------------------------------------------
# Logging
# -------------------------------------------------

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)


# # Log filename includes the date so each day gets its own file
log_filename = f"logs/bronze_pipeline_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger("bronze_loader") 


load_dotenv()


# -------------------------------------------------
# Configuration
# -------------------------------------------------
BATCH_SIZE = 5000
COLUMNS = [
    ["settlement_id", "settlement_date", "store_id", "store_name", 
     "store_city", "item_id", "item_description", "category", 
     "supplier_name", "units_sold", "unit_price", "gross_sales", 
     "discount_amount", "net_sales", "cost_of_goods", "gross_margin"]
]
AUDIT_COLUMNS = ["source_file", "BatchId", "ingested_at", "is_active"]

ALL_COLUMNS = COLUMNS + AUDIT_COLUMNS
PLACEHOLDERS = ",".join(["?"] * len(ALL_COLUMNS))
COLUMN_SQL = ",".join(ALL_COLUMNS)

# -------------------------------------------------
# Postgres Connection
# -------------------------------------------------

try:
    conn = connect_postgres()
except Exception as e:
    logger.error(f"Failed to connect to Postgres: {e}")
    sys.exit(1)


# -------------------------------------------------
# findout file that Needs to Process
# -------------------------------------------------

try:
    batch_result = build_batch_audit()
except Exception as e:
    logger.error(f"Failed to build batch audit: {e}")
    sys.exit(1)


# -----------------------------------------------------
# Creating Class to load data into Postgres
# -----------------------------------------------------
class LoadingData_ToPostgres:
    def __init__(self, conn):
        self.conn = conn
        self.batch_id = batch_result["batch_id"]


    def get_last_run_id(self, conn):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT run_id, source_file, started_at
                FROM prec_pipeline_run_log
                WHERE run_status IN ('pending', 'failed')
                ORDER BY run_id DESC
                LIMIT 1
                '''
            )
            row = curr.fetchone()
            self.batch_id, self.source_file, self.started_at = row if row else (None, None, None)
            return self.batch_id, self.source_file, self.started_at
        
    def get_file_location(self, file_name):
        folder_path = os.getenv("LCBO_RAW_FOLDER_PATH")
        file_path = os.path.join(folder_path, file_name)
        return file_path


    def read_csv_file_inbatch(self, file_path):
        try:
            for chunk in pd.read_csv(file_path, chunksize=BATCH_SIZE):
                yield chunk
        except Exception as e:
            logger.error(f"Error reading CSV file in batches: {e}")
            raise

    def insert_batch_to_postgres(self, df, conn):
        try:
            with conn.cursor() as curr:
                for _, row in df.iterrows():
                    values = tuple(row[col] for col in ALL_COLUMNS)
                    curr.execute(
                        f"INSERT INTO prec_lcbo_raw ({COLUMN_SQL}) VALUES ({PLACEHOLDERS})",
                        values
                    )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error inserting batch to Postgres: {e}")
            self.conn.rollback()
            raise
    

    



def get_last_run_id(conn):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT MAX(run_id) FROM prec_pipeline_run_log
                where run_status in ('pending', 'failed')
                '''
            )
            max_batch_id = curr.fetchone()[0]
        return max_batch_id


def get_file_to_process(conn):
    with conn.cursor() as curr:
        curr.execute(
            '''
            SELECT DISTINCT file_name FROM prec_pipeline_run_log
            WHERE run_id = %s
            ''',
            (get_last_run_id(conn),)
        )

        file = [row[0] for row in curr.fetchall()]
    return file


if batch_result["all_processed_files"]:
    logger.info("There are no new files to process.")

else:
    # code is going to chnage from here.

    if not batch_result["batch_created"]:
        last_run_id = get_last_run_id(conn)
        batch_result["batch_created"] = True
        batch_result["batch_id"] = last_run_id
        logger.info(f"Pending records exist in the log table. Last run ID: {last_run_id}. Exiting pipeline.")
        sys.exit(0)

    file_name = get_file_to_process(conn)

    try:
        if file_name:
            logger.info(f"File to process: {file_name[0]}")
            folder_path = os.getenv("LCBO_RAW_FOLDER_PATH")
            file_path = os.path.join(folder_path, file_name[0])
            raw_df = pd.read_csv(file_path)
        
        else:
            logger.info("No new files to process in batch_audit table.")

    except Exception as e:
        logger.error(f"Error while fetching the raw file to process: {e}")
        sys.exit(1)


    
    











