import pandas as pd
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


#-------------------------------------------------
# Connecting to Postgres database
#-------------------------------------------------
try:
    conn = connect_postgres()
    logger.info("Connected to Postgres database successfully.")
except Exception as e:
    logger.error(f"Error connecting to Postgres database: {e}")
    sys.exit(1)



# -------------------------------------------------
# reading product cost data from the CSV file
#-------------------------------------------------

def read_product_cost_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Product cost data read successfully from {file_path}.")
        return df
    except Exception as e:
        logger.error(f"Error reading product cost data from {file_path}: {e}")
        sys.exit(1)

    
# Need to pull table and schema name from .env file
def create_dim_product_cost_table(conn):
    try:
        cursor = conn.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS bronze.dim_product_cost (
                item_id INT PRIMARY KEY,
                item_description VARCHAR(255),
                category VARCHAR(255),
                cost_per_item DECIMAL(10, 2),
                cost_effective_date DATE
            )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("dim_product_cost table created successfully.")
    except Exception as e:
        logger.error(f"Error creating dim_product_cost table: {e}")
        conn.rollback()


def truncate_table(conn, table_name):
    try:
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE bronze.{table_name}"
        cursor.execute(truncate_query)
        conn.commit()
        logger.info(f"Table {table_name} truncated successfully.")

        return True  # Return True to indicate successful truncation
    
    except Exception as e:
        logger.error(f"Error truncating table {table_name}: {e}")
        conn.rollback()
        return False  # Return False to indicate failure to truncate

# function to insert product cost data into Postgres database into chunks of 5000 records at a time.

def insert_product_cost_data_in_chunks(df, conn):
    try:
        cursor = conn.cursor()

        
        insert_query = """
            INSERT INTO bronze.dim_product_cost (item_id, item_description, category, cost_per_item, cost_effective_date)
            VALUES %s
        """
        execute_values(cursor, insert_query, df.values.tolist())
        conn.commit()
        logger.info("total records inserted: %d into bronze.dim_product_cost", cursor.rowcount)

        return cursor.rowcount  # Return the number of rows inserted/updated
    
    except Exception as e:
        logger.error(f"Error inserting/updating product cost data: {e}")
        conn.rollback()


# creating a chuncks of 5000 records from the product cost dataframe 

def chunk_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size): 
        yield df.iloc[start:start + chunk_size]


def main_loading():
    data_file_path = os.getenv("dim_products_file_path")
    file_name = "dim_product_cost.csv"

    joined_path = os.path.join(data_file_path, file_name)

    product_cost_df = read_product_cost_data(joined_path)

    empty_table = truncate_table(conn, "dim_product_cost")

    if not empty_table:
        logger.error("dim_product_cost table not found, creating new table.")
        create_dim_product_cost_table(conn) 


    # converting product_cost_df to chunks of 5000 records 
    while True:
        try:
            chunks = chunk_dataframe(product_cost_df, int(os.getenv("chunk_size", "1000")))
            for chunk in chunks:
                inserted_rows = insert_product_cost_data_in_chunks(chunk, conn)
                logger.info(f"Inserted {inserted_rows} rows into bronze.dim_product_cost table.")

            logger.info("All chunks processed successfully.")
            break  # Exit the loop after processing all chunks
        except Exception as e:
            logger.error(f"Error processing chunks: {e}")
            break  # Exit the loop on error



def close_connection():
    if conn:
        conn.close()
        logger.info("Postgres connection closed.")


# -------------------------------------------------
# Manual Execution
# -------------------------------------------------

if __name__ == "__main__":
    try:
        main_loading()
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        close_connection()



