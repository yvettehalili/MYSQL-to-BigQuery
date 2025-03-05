#!/bin/bash

# Activate the virtual environment
source /backup/environments/backupv1/bin/activate

# Run the ETL script
python /backup/scripts/etl_mysql_to_bigquery/etl_mysql_to_bigquery.py "$@"
