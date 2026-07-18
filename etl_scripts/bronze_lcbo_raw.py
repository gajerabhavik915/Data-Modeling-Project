from importlib.resources import files
import os
import sys
import logging
from datetime import datetime
from unittest import result
import pandas as pd
import psycopg2 
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from bronze_batch_audit import connect_postgres


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
    ],
    force=True  
)


logger = logging.getLogger("bronze_loader") 


load_dotenv()


# -------------------------------------------------
# Configuration
# -------------------------------------------------
BATCH_SIZE = 5000
COLUMNS = ["settlement_id", "settlement_date", "store_id", "store_name", 
     "store_city", "item_id", "item_description", "category", 
     "supplier_name", "units_sold", "unit_price", "gross_sales", 
     "discount_amount", "net_sales", "cost_of_goods", "gross_margin"]
    
AUDIT_COLUMNS = ["source_file", "batch_id", "ingested_at", "is_active", "file_hash"]

ALL_COLUMNS = COLUMNS + AUDIT_COLUMNS
PLACEHOLDERS = ",".join(["%s"] * (len(ALL_COLUMNS)))
COLUMN_SQL = ",".join(ALL_COLUMNS)

# -------------------------------------------------
# Postgres Connection
# -------------------------------------------------

try:
    conn = connect_postgres()
    logger.info("Successfully connected to Postgres")
except Exception as e:
    logger.error(f"Failed to connect to Postgres: {e}")
    sys.exit(1)


# -----------------------------------------------------
# Creating Class to load data into Postgres
# -----------------------------------------------------
class LoadingData_ToPostgres:
    def __init__(self, conn):
        # create all required varibales to be used in the class. good practice to create all variables in the init method.
        self.conn = conn
        self.batch_id = None
        self.source_file = None 
        self.started_at = None
        self.file_hash = None
        logger.info("LoadingData_ToPostgres class initialized successfully.")
        

    def get_last_run_id(self, conn):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT run_id, source_file, started_at, file_hash
                FROM bronze.pipeline_run_log
                WHERE status IN ('pending', 'failed')
                ORDER BY run_id 
                LIMIT 1
                '''
            )
            row = curr.fetchone()
            
            self.batch_id, self.source_file, self.started_at, self.file_hash = row if row else (None, None, None, None)
            return self.batch_id, self.source_file, self.started_at, self.file_hash
        

    def get_file_location(self, file_name):
        folder_path = os.getenv("source_file_path")
        file_path = os.path.join(folder_path, file_name)
        return file_path

    
    def data_exists_in_postgres(self, conn, file_hash):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT distinct(file_hash) FROM bronze.stg_lcbo_settlement
                WHERE file_hash = %s and is_active = 'true'
                ''',
                (file_hash,)
            )
            result = curr.fetchone()
            return result is not None
            

    def read_csv_file_inbatch(self, file_path):
        try:
            for chunk in pd.read_csv(file_path, chunksize=BATCH_SIZE):
                yield chunk
        except Exception as e:
            logger.error(f"Error reading CSV file in batches: {e}")
            raise


    def insert_batch_to_postgres(self, df):
        with self.conn.cursor() as curr:
            rows = [tuple(row[col] for col in ALL_COLUMNS) for _, row in df.iterrows()]
            execute_values(
                curr,
                f"INSERT INTO bronze.stg_lcbo_settlement ({COLUMN_SQL}) VALUES %s",
                rows
            )
            return len(rows)
            
  
    def add_ingestion_metadata(self, df):
        df["source_file"] = self.source_file
        df["batch_id"] = self.batch_id
        df["ingested_at"] = datetime.now()
        df["is_active"] = True
        df["file_hash"] = self.file_hash
        return df

    
    def process_file(self):
        self.get_last_run_id(self.conn)
        if not self.source_file:
            logger.info("No pending or failed runs found. Exiting pipeline.")
            sys.exit(0)
        
        if self.data_exists_in_postgres(self.conn, self.file_hash):
            logger.info(f"Data from file {self.source_file} already exists in Postgres. Skipping processing.")
            sys.exit(0)

        logger.info(f"Processing file: {self.source_file} with batch ID: {self.batch_id} and file hash: {self.file_hash}")

        file_path = self.get_file_location(self.source_file)
        logger.info(f"Found file: {file_path}")
    
        try:
            for chunk in self.read_csv_file_inbatch(file_path):
                chunk_with_metadata = self.add_ingestion_metadata(chunk)
                self.insert_batch_to_postgres(chunk_with_metadata)
                logger.info(f"Inserted batch of size {len(chunk)} into Postgres.")

            self.conn.commit()
            logger.info("File processed successfully.")
            return True
        
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            self.conn.rollback()
            sys.exit(1)
            

    def close_connection(self):
        if self.conn:
            self.conn.close()
            logger.info("Postgres connection closed.")


# -------------------------------------------------
# Airflow-compatible function
# -------------------------------------------------

def execute_pipeline(**kwargs):
    mig = LoadingData_ToPostgres(conn)
    try:
        mig.process_file()
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        if conn:
            mig.close_connection()
        raise
    finally:
        mig.close_connection()


# -------------------------------------------------
# Manual Execution
# -------------------------------------------------

if __name__ == "__main__":
    try:
        execute_pipeline()
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


    











