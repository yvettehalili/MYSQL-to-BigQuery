# ETL MySQL to BigQuery

This project demonstrates an ETL (Extract, Transform, Load) process that extracts data from a MySQL database, transforms it, and loads it into Google BigQuery.

## Directory Structure
```
/backup/scripts/etl_mysql_to_bigquery/
│
├── configs/
│   ├── db_credentials.json
│   ├── MYSQL_to_BigQuery_tables.json
│
├── logs/
│
├── dumps/
│
├── run_etl.sh
├── etl_mysql_to_bigquery.py
├── requirements.txt
└── README.md
```

## Configuration

### MySQL Credentials
Create a `db_credentials.json` file in the `configs/` directory with the following structure:
```json
{
    "DB_USR": "your_mysql_username",
    "DB_PWD": "your_mysql_password",
    "DB_HOST": "localhost",
    "DB_NAME": "ti_db_inventory",
    "DB_PORT": 3306,
    "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/your/google/credentials.json",
    "BQ_PROJECT_ID": "your_bigquery_project_id",
    "BQ_DATASET_ID": "your_bigquery_dataset_id"
}
```

### BigQuery Schema
Create a `MYSQL_to_BigQuery_tables.json` file in the `configs/` directory with the schema for your BigQuery tables.

## Running the Script

### Install Dependencies
Install the required Python packages using pip:
```bash
pip install -r requirements.txt
```

### Run ETL Process
To run the ETL process, execute the following command:
```bash
./run_etl.sh
```

To run the daily ETL process, execute the following command:
```bash
./run_etl.sh --daily
```

## Logging
Logs are stored in the `logs/` directory with the filename format `MYSQL_to_BQ_<current_date>.log`.

## Cleanup
The script automatically deletes JSON files older than 7 days from the `dumps/` directory.

## License
This project is licensed under the MIT License.
