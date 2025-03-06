import pymysql
import pandas as pd
from google.cloud import bigquery
import json
import logging
from datetime import datetime, timedelta
import os
import glob
import argparse
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import tempfile

# Set up logging
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = f"/backup/scripts/etl_mysql_to_bigquery/logs/MYSQL_to_BQ_{CURRENT_DATE}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the dumps directory
DUMPS_DIR = "/backup/scripts/etl_mysql_to_bigquery/dumps"
os.makedirs(DUMPS_DIR, exist_ok=True)

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

def create_engine_url():
    """Create SQLAlchemy engine URL safely."""
    password = quote_plus(mysql_config['DB_PWD'])
    return create_engine(
        f"mysql+pymysql://{mysql_config['DB_USR']}:{password}@{mysql_config['DB_HOST']}:{mysql_config['DB_PORT']}/{mysql_config['DB_NAME']}"
    )

def cleanup_old_files():
    """Delete JSON files older than 7 days from the dumps directory."""
    try:
        current_time = datetime.now()
        json_files = glob.glob(os.path.join(DUMPS_DIR, "*.json"))

        for file_path in json_files:
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if (current_time - file_modified_time) > timedelta(days=7):
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")
    except Exception as e:
        logging.error(f"Error during cleanup of old files: {e}")

def extract_from_mysql(table_name, date_column):
    """Extract data from MySQL table for the current day."""
    engine = None
    try:
        engine = create_engine_url()

        # Filter data based on the current date
        today = datetime.now().strftime('%Y-%m-%d')
        query = f"SELECT * FROM {table_name} WHERE DATE({date_column}) = '{today}'"

        df = pd.read_sql(query, engine)

        # Convert datetime columns to string format
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"Successfully extracted {len(df)} rows from MySQL table: {table_name}")

        return df
    except Exception as e:
        logging.error(f"Error extracting data from MySQL table {table_name}: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

def transform_data(df, table_name):
    """Transform the data according to table requirements."""
    try:
        if table_name == 'database_list':
            bool_columns = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat',
                            'encrypted', 'ssl', 'backup', 'load', 'size', 'active']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)

        if table_name == 'daily_log':
            df = df.rename(columns={
                'backup_date': 'BackupDate',
                'server': 'Server',
                'database': 'Database',
                'size': 'Size',
                'state': 'State',
                'last_update': 'LastUpdate',
                'fileName': 'FileName'
            })

        logging.info(f"Transformed data for table: {table_name}")
        return df
    except Exception as e:
        logging.error(f"Error transforming data for table {table_name}: {e}")
        raise

def get_schema_from_config(table_name):
    """Get BigQuery schema from JSON file."""
    if table_name not in schema_config:
        raise ValueError(f"No schema defined for table: {table_name}")

    schema = [
        bigquery.SchemaField(field["name"], field["type"])
        for field in schema_config[table_name]
    ]

    return schema

def load_to_bigquery(df, table_name):
    """Load data into BigQuery."""
    try:
        if df.empty:
            logging.info(f"No new data to load for table: {table_name}")
            return

        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig()
        job_config.schema = get_schema_from_config(table_name)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Create a temporary file with a custom name
        temp_file_name = f"mysql_to_bq_{CURRENT_DATE}_{table_name}.json"
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
                logging.info(f"Data loaded from temporary file to BigQuery table: {table_name}")

        # Remove the temporary file
        os.remove(temp_file_path)
        logging.info(f"Temporary file deleted: {temp_file_path}")

        table = bq_client.get_table(table_ref)
        logging.info(f"Successfully loaded {len(df)} rows into BigQuery table: {table_name}")
        logging.info(f"Total rows in table after load: {table.num_rows}")

    except Exception as e:
        logging.error(f"Error loading data into BigQuery table {table_name}: {e}")
        raise

def get_mysql_tables():
    """Get list of MySQL tables."""
    allowed_tables = schema_config.keys()
    engine = None
    try:
        engine = create_engine_url()
        with engine.connect() as connection:
            result = connection.execute(text("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"))
            tables = [row[0] for row in result if row[0] in allowed_tables]
            return tables
    finally:
        if engine:
            engine.dispose()

def run_etl():
    """Main ETL process."""
    try:
        tables = get_mysql_tables()
        logging.info(f"Found tables in MySQL: {tables}")

        for table_name in tables:
            logging.info(f"Processing table: {table_name}")

            # Determine the date column to filter by
            date_column = 'creation_date' if table_name == 'database_list' else 'backup_date'

            df = extract_from_mysql(table_name, date_column)
            if not df.empty:
                df = transform_data(df, table_name)
                load_to_bigquery(df, table_name)
            else:
                logging.warning(f"No data extracted for table: {table_name}")

        cleanup_old_files()

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    try:
        run_etl()
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise
