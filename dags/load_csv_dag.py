from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import psycopg2
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT

# Set airflow arguments
default_args = {
    'owner': 'manuelgomez',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define function to process CSV files


def process_file(filename):
    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOST,
        port=DATABASE_PORT
    )

    conn.set_session(autocommit=True)
    cursor = conn.cursor()

    try:
        # Process the file by micro batches (20)
        for chunk_df in pd.read_csv(f'/opt/airflow/data/{filename}.csv', chunksize=20, parse_dates=['timestamp']):
            # Cast and handle price column as int
            chunk_df['price'] = chunk_df['price'].fillna(0).astype(int)
            logging.info(f"DataFrame content:\n{chunk_df}")
            # Iterate over each row in the chunk dataFrame
            for index, row in chunk_df.iterrows():
                cursor.execute(
                    "INSERT INTO data (timestamp, price, user_id) VALUES (%s, %s, %s)",
                    (row['timestamp'], row['price'], row['user_id'])
                )
            logging.info(f"Chunk of file {filename} processed successfully")

    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# Define DAG

with DAG('process_csv_files',
         default_args=default_args,
         schedule_interval=timedelta(days=1),
         description='Dag that allow process csv files and save it into DB',
         catchup=False) as dag:

    task1 = PythonOperator(
        task_id='process_2012_1',
        python_callable=process_file,
        op_args=['2012-1']
    )

    task1
