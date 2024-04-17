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

# Set airflow settings
default_args = {
    'owner': 'manuelgomez',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define function to process CSV files


def process_file(filename, ti):
    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOST,
        port=DATABASE_PORT
    )

    conn.set_session(autocommit=True)
    cursor = conn.cursor()

    # Define variables for statistics, if already exist get the value from previus task
    total_rows = ti.xcom_pull(key='total_rows') or 0
    sum_prices = ti.xcom_pull(key='sum_prices') or 0
    min_price = ti.xcom_pull(key='min_price') or float('inf')
    max_price = ti.xcom_pull(key='max_price') or float('-inf')

    try:
        # Process the file by micro batches (1) to get new statistics for every row
        for chunk_df in pd.read_csv(f'/opt/airflow/data/{filename}.csv', chunksize=1, parse_dates=['timestamp']):
            logging.info(f"[Content]\n{chunk_df}")

            # Cast and handle price column
            chunk_df['price'] = chunk_df['price'].astype(float)

            # Update statistics
            total_rows += len(chunk_df)
            sum_prices += chunk_df['price'].sum(skipna=True)
            min_price = min(min_price, chunk_df['price'].min())
            max_price = max(max_price, chunk_df['price'].max())
            avg_price = round(sum_prices / total_rows if total_rows else 0, 1)

            # Iterate over dataframe object for save into DB
            for index, row in chunk_df.iterrows():
                # Save row
                cursor.execute(
                    "INSERT INTO data (timestamp, price, user_id) VALUES (%s, %s, %s)",
                    (row['timestamp'], row['price'] if pd.notna(
                        row['price']) else None, row['user_id'])
                )

                # Recreate statistics
                cursor.execute(
                    "INSERT INTO statistics (filename, total_rows, avg_price, min_price, max_price) VALUES (%s, %s, %s, %s, %s)",
                    (f"{filename}.csv", total_rows, avg_price,
                     int(min_price), int(max_price))
                )
                logging.info(
                    f"[Final Statistics] Rows count ({total_rows}) | Avg price ({avg_price}) | Min price ({int(min_price)}) | Max price ({int(max_price)})")

        logging.info(
            f"File: {filename}.csv processed successfully")

        # Push last statistic data to the next task
        ti.xcom_push(key='total_rows', value=total_rows)
        ti.xcom_push(key='sum_prices', value=sum_prices)
        ti.xcom_push(key='min_price', value=min_price)
        ti.xcom_push(key='max_price', value=max_price)

    except Exception as e:
        logging.error(f"Error processing file {filename}.csv: {e}")
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
        task_id=f'process_2012_1',
        python_callable=process_file,
        op_args=['2012-1'],
        provide_context=True
    )

    task2 = PythonOperator(
        task_id=f'process_2012_2',
        python_callable=process_file,
        op_args=['2012-2'],
        provide_context=True
    )

    task3 = PythonOperator(
        task_id=f'process_2012_3',
        python_callable=process_file,
        op_args=['2012-3'],
        provide_context=True
    )

    task4 = PythonOperator(
        task_id=f'process_2012_4',
        python_callable=process_file,
        op_args=['2012-4'],
        provide_context=True
    )
    5
    task5 = PythonOperator(
        task_id=f'process_2012_5',
        python_callable=process_file,
        op_args=['2012-5'],
        provide_context=True
    )

    task1 >> task2 >> task3 >> task4 >> task5
