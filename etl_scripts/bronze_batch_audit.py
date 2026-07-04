import hashlib
import os
import sys
import logging
from datetime import datetime
import psycopg2 
from dotenv import load_dotenv
import hashlib

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
    file_hash_dict = {}




    def pending_records_exist(conn):
        with conn.cursor() as curr:
            curr.execute(
                # out of all pending and failed records, we only want to run the first one, so we will order by run_id ascending
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
                SELECT DISTINCT file_hash from prec_pipeline_run_log
                where status = 'success'
                '''
            )

            processed_file_hash = {row[0] for row in curr.fetchall()} # returns a set of all the files which are already processed in past successful runs
            processed_file_hash = set(processed_file_hash)

        logger.info(f"Found {len(processed_file_hash)} already processed files in DB")
        return processed_file_hash

    
    def compute_file_hash(file_path):
        """Read a file's bytes and return its SHA-256 fingerprint."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:                     # "rb" = read as raw bytes
            for block in iter(lambda: f.read(8192), b""):    # read in 8KB chunks
                hasher.update(block)                          # feed each chunk into the hash
        return hasher.hexdigest() 
    

    def get_folder_files(source_file_path):
        try:
            file_hash_dict.clear()  # Clear the dictionary before populating it
            for f in os.listdir(source_file_path):
                file_path = os.path.join(source_file_path, f)
                if os.path.isfile(file_path):
                    file_hash = compute_file_hash(file_path)
                    file_hash_dict[file_hash] = f


            files_hash = set(file_hash_dict.keys())
            logger.info(f"Found {len(files_hash)} files in folder {source_file_path}")
            return files_hash
        except Exception as e:
            logger.error(f"Error accessing folder {source_file_path}: {e}")
            raise


    

     


    
    def get_pending_files(conn, source_file_path):
        processed_files_hash = get_processed_files(conn)
        all_files_hash  = get_folder_files(source_file_path)

        pending_files_hash = sorted(all_files_hash - processed_files_hash)  # set difference.
        logger.info(f"Pending files to process: {pending_files_hash}")

        return pending_files_hash
    


    def create_new_batch(conn, first_pending_file, max_batch_id, first_pending_file_hash):
        now = datetime.now()
        new_batch_id = int(max_batch_id) + 1
        
        with conn.cursor() as curr:
            curr.execute(
                '''
                INSERT INTO prec_pipeline_run_log(run_id, source_file, started_at, status, file_hash)
                VALUES (%s, %s, %s, %s, %s)
                ''',
                (new_batch_id, str(first_pending_file), now, 'pending', first_pending_file_hash)
            )
            conn.commit()
        logger.info(f"Created new batch with run_id {new_batch_id} for {first_pending_file} at {now}")

        return {
            "batch_created": True,
            "batch_id": new_batch_id,
            "related_date": now,
            "file_hash": first_pending_file_hash,
            "all_processed_files": False
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
        logger.info("Please check the pipeline run log and resolve any pending/failed file issues before starting a new batch.")

        conn_close(conn)  # Close the connection after the audit is done

        return {
            "batch_created": False,
            "batch_id": None,
            "related_date": None,
            "all_processed_files": None,
            "file_hash": None
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
            "related_date": None,
            "all_processed_files": False,
            "file_hash": None
        }


    ### Step 3
    # if no pending records exist in DB, then check if any new files are present in the source folder which are not yet processed.

    pending_files_hash = get_pending_files(conn, os.getenv("source_file_path"))

    if pending_files_hash:

        logger.info(f"{len(pending_files_hash)} new file(s) to process: {pending_files_hash}")
            
        # only process one by one file,
        first_pending_file_hash = list(pending_files_hash)[0] 
        first_pending_file_name = file_hash_dict[first_pending_file_hash]
        logger.info(f"Started with {first_pending_file_hash}")

        try:
            batch_creating = create_new_batch(conn, first_pending_file_name, max_batch_id, first_pending_file_hash)
            conn_close(conn) # Close the connection after the audit is done
            return batch_creating
        
        except Exception as e:
            conn.rollback()
            conn_close(conn) # Close the connection after the audit is done
            logger.error(f" Failed to create a batch for {first_pending_file_hash} : {e}")
            return {
                    "batch_created": False,
                    "batch_id": None,
                    "related_date": None,
                    "all_processed_files": False,
                    "file_hash": None
                    }
                
            
    else:
        logger.info("No new files to process - pipeline is upto date")
        logger.info("Exiting batch audit.")

        return {
                    "batch_created": False,
                    "batch_id": None,
                    "related_date": None,
                    "all_processed_files": True,
                    "file_hash": None
                    }
        

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



# there are three types of output from this file,
# 1. if there are pending records in the log table, then it will 
# return batch_created: False, batch_id: None, related_date: None, all_processed_files: None

# 2. if there are no pending records in the log table, 
# but there are new files to process, then it will 
# return batch_created: True, batch_id: <new_batch_id>, related_date: <timestamp>, all_processed_files: False, file_hash: <file_hash>

# 3. if there are no pending records in the log table, 
# and there are no new files to process, then it will 
# return True.   