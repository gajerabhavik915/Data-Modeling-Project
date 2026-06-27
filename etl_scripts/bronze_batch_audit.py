import os
import sys
import logging
from datetime import datetime
import psycopg2 
from dotenv import load_dotenv

# -------------------------------------------------
# Logging
# -------------------------------------------------

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# # Log filename includes the date so each day gets its own file
log_filename = f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"


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
# Postgres Connection
# -------------------------------------------------

def connect_postgres():
    try:
        conn = psycopg2.connect(
            host = os.getenv("PG_HOST"),
            port = os.getenv("PG_PORT"),
            database = os.getenv("PG_DATABASE"),
            user = os.getenv("PG_USER"),
            password = os.getenv("PG_PASSWORD")
        )

        logger.info("Successfully connected to Postgres")

        return conn
    
    except Exception as e:
        logger.error(f"Error connecting to Postgres: {e}")
        raise


# -------------------------------------------------
# Build Batch Audit
# -------------------------------------------------
def build_batch_audit():
    conn = connect_postgres()
    curr = conn.cursor




    def pending_records_exist(conn):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT COUNT(*) FROM prec_pipeline_run_log
                where status in ('pending', 'failed')
                '''
            )
            pending_count = curr.fetchone()[0]
            return pending_count > 0   



    def get_last_run_id(conn):
        with conn.cursor() as curr:
            curr.execute(
                '''
                SELECT MAX(run_id) FROM prec_pipeline_run_log
                '''
            )
            max_batch_id = curr.fetchone()[0]
        return max_batch_id
        


    def get_processed_files(conn):
        with conn.cursor() as curr:  # This will automatically close the cursor when jumping out of the function (not the connection).
            curr.execute(
                '''
                SELECT DISTINCT source_file from prec_pipeline_run_log
                where status = 'success'
                '''
            )

            processed = {row[0] for row in curr.fetchall()} # returns a set of all the files which are already processed in past successful runs
            processed = set(processed)

        logger.info(f"Found {len(processed)} already processed files in DB")
        return processed
    


    def get_folder_files(source_file_path):
        try:
            files = [f for f in os.listdir(source_file_path) if os.path.isfile(os.path.join(source_file_path, f))]
            files = set(files)  # convert to set for easier comparison later
            logger.info(f"Found {len(files)} files in folder {source_file_path}")
            return files
        except Exception as e:
            logger.error(f"Error accessing folder {source_file_path}: {e}")
            raise


    
    def get_pending_files(conn, source_file_path):
        processed = get_processed_files(conn)
        all_files  = get_folder_files(source_file_path)

        pending_files = sorted(all_files - processed)  # set difference.
        logger.info(f"Pending files to process: {pending_files}")

        return pending_files
    


    def create_new_batch(conn, first_pending_file, max_batch_id):
        now = datetime.now()
        new_batch_id = int(max_batch_id) + 1
        
        with conn.cursor() as curr:
            curr.execute(
                '''
                INSERT INTO prec_pipeline_run_log(run_id, source_file, started_at, status)
                VALUES (%s, %s, %s, %s)
                ''',
                (new_batch_id, str(first_pending_file), now, 'pending')

            )
            conn.commit()
        logger.info(f"Created new batch with run_id {new_batch_id} for {first_pending_file} at {now}")

        return {
            "batch_created": True,
            "batch_id": new_batch_id,
            "related_date": now
        }
     


    def conn_close(conn):
        if conn:
            conn.close()
            logger.info("Postgres connection closed.") 


    ### Step 1
    # If any pending/failed record exist in Database, then no action should be taken, 
    # user should resolve the pending/failed records first before creating a new batch.

    pending_db_rows_exists = pending_records_exist(conn)

    if pending_db_rows_exists:
        logger.error("There are pending records in the pipeline run log. Cannot proceed with batch audit.")
        logger.info("Please check the pipeline run log and resolve any issues before starting a new batch.")

        conn_close(conn)  # Close the connection after the audit is done

        return {
            "batch_created": False,
            "batch_id": None,
            "related_date": None
        }

    ### Step 2 
    # If no pending records exist in DB, then get the last run_id from the log table to create a new batch with incremented run_id.

    max_batch_id = get_last_run_id(conn)

    if not max_batch_id:
        logger.error('log table is empty, cannot proceed with batch audit')
        curr.close()
        conn.close()

        #this is very IMP, 
        #later you can use these varibales to check the output of this func.
        return {
            "batch_created": False,
            "batch_id": None,
            "related_date": None
        }


    ### Step 3
    # if no pending records exist in DB, then check if any new files are present in the source folder which are not yet processed.

    pending_files = get_pending_files(conn, os.getenv("source_file_path"))

    if pending_files:

        logger.info(f"{len(pending_files)} new file(s) to process: {pending_files}")
            
        # only process one by one file,
        first_pending_file = pending_files[0] 
        logger.info(f"Started with {first_pending_file}")

        try:
            batch_creating = create_new_batch(conn, first_pending_file, max_batch_id)
            conn_close(conn) # Close the connection after the audit is done
            return batch_creating
        
        except Exception as e:
            conn.rollback()
            conn_close(conn) # Close the connection after the audit is done
            logger.error(f" Failed to create a batch for {first_pending_file} : {e}")
            return {
                "batch_created": False}
        
            
    else:
        logger.info("No new files to process - pipeline is upto date")
        logger.info("Exiting batch audit.")

        return True
        

# sometimes you might see (_main_), if this entire file is being called by other file then we should use that to get file name in log filename.
# if we don't mention that then when we import in other file it will get executed 
# because while importing the entire file will be executed and it will create a new log file for that file which is not what we want. 
# We want to use the same log file for all the files in the project. 
# So we use __name__ to get the name of the file which is being executed.


if __name__ == "__main__":
    try:
        result = build_batch_audit()
        logger.info(f"batch audit result: {result}")
    except Exception as e:
        logger.error(f"Batch audit failed: {e}")
        sys.exit(1)    




    