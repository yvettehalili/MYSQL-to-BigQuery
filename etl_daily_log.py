import pymysql
import pandas as pd
from google.cloud import bigquery
import json
import logging
from datetime import datetime
import os
import tempfile
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Set up logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/scripts/etl_mysql_to_bigquery/logs/HISTORICAL_daily_log_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load MySQL credentials from JSON file
CREDENTIALS_PATH = "/backup/scripts/etl_mysql_to_bigquery/configs/db_credentials.json"
with open(CREDENTIALS_PATH, "r") as f:
    mysql_config = json.load(f)

# Load table schemas from JSON file
SCHEMA_PATH = "/backup/scripts/etl_mysql_to_bigquery/configs/MYSQL_to_BigQuery_tables.json"
with open(SCHEMA_PATH, "r") as f:
    schema_config = json.load(f)

# Google BigQuery configuration
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = mysql_config["GOOGLE_APPLICATION_CREDENTIALS"]
bq_client = bigquery.Client()
project_id = mysql_config["BQ_PROJECT_ID"]
dataset_id = mysql_config["BQ_DATASET_ID"]
table_name = 'daily_log'

def create_engine_url():
    """Create SQLAlchemy engine URL safely."""
    password = quote_plus(mysql_config['DB_PWD'])
    return create_engine(
        f"mysql+pymysql://{mysql_config['DB_USR']}:{password}@{mysql_config['DB_HOST']}:{mysql_config['DB_PORT']}/{mysql_config['DB_NAME']}"
    )

def extract_from_mysql():
    """Extract historical data from MySQL table daily_log where backup_date is less than 2025-03-07."""
    engine = None
    try:
        engine = create_engine_url()

        # Filter data where backup_date is less than 2025-03-07
        query = "SELECT * FROM daily_log WHERE DATE(backup_date) < '2025-03-07'"

        df = pd.read_sql(query, engine)

        # Convert datetime columns to string format
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"Successfully extracted {len(df)} rows from MySQL table: daily_log")

        return df
    except Exception as e:
        logging.error(f"Error extracting data from MySQL table daily_log: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

def transform_data(df):
    """Transform the data according to table requirements."""
    try:
        df = df.rename(columns={
            'backup_date': 'BackupDate',
            'server': 'Server',
            'database': 'Database',
            'size': 'Size',
            'state': 'State',
            'last_update': 'LastUpdate',
            'fileName': 'FileName'
        })

        logging.info("Transformed data for table: daily_log")
        return df
    except Exception as e:
        logging.error(f"Error transforming data for table daily_log: {e}")
        raise

def get_schema_from_config():
    """Get BigQuery schema from JSON file."""
    if table_name not in schema_config:
        raise ValueError(f"No schema defined for table: {table_name}")

    schema = [
        bigquery.SchemaField(field["name"], field["type"])
        for field in schema_config[table_name]
    ]

    return schema

def load_to_bigquery(df):
    """Load data into BigQuery."""
    try:
        if df.empty:
            logging.info("No new data to load for table: daily_log")
            return

        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate the table before loading

        # Create a temporary file with a custom name
        temp_file_name = f"mysql_to_bq_historical_{CURRENT_DATE}_{table_name}.json"
        temp_file_path = os.path.join(tempfile.gettempdir(), temp_file_name)

        with open(temp_file_path, 'w') as temp_file:
            # Write DataFrame to the temporary file as newline-delimited JSON
            df.to_json(temp_file.name, orient='records', lines=True)
            logging.info(f"Temporary JSON file created: {temp_file_path}")
            
            # Load data to BigQuery
            with open(temp_file_path, 'rb') as json_file:
                job = bq_client.load_table_from_file(
                    json_file,
                    table_ref,
                    job_config=job_config
                )
                job.result()  # Wait for the job to complete
                logging.info(f"Data loaded from temporary file to BigQuery table: daily_log")

        # Remove the temporary file
        os.remove(temp_file_path)
        logging.info(f"Temporary file deleted: {temp_file_path}")

        table = bq_client.get_table(table_ref)
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table: daily_log")
        logging.info(f"Total rows in table after load: {table.num_rows}")

    except Exception as e:
        logging.error(f"Error loading data into BigQuery table daily_log: {e}")
        raise

def run_etl():
    """Main ETL process."""
    try:
        df = extract_from_mysql()
        if not df.empty:
            df = transform_data(df)
            load_to_bigquery(df)
        else:
            logging.warning("No data extracted for table: daily_log")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    try:
        run_etl()
        logging.info("Historical ETL process for daily_log completed successfully")
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise
