from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import process_file
from pipelines import process_validation

# Set DAG settings
default_args = {
    'owner': 'manuelgomez',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG('process_csv_files',
         default_args=default_args,
         schedule_interval=None,
         description='Dag that allow process csv files and save it into DB',
         catchup=False) as dag:

    # Each task corresponds to processing a CSV file
    task_file_1 = PythonOperator(
        task_id=f'process_2012_1',
        python_callable=process_file,
        op_args=['2012-1'],
        provide_context=True
    )

    task_file_2 = PythonOperator(
        task_id=f'process_2012_2',
        python_callable=process_file,
        op_args=['2012-2'],
        provide_context=True
    )

    task_file_3 = PythonOperator(
        task_id=f'process_2012_3',
        python_callable=process_file,
        op_args=['2012-3'],
        provide_context=True
    )

    task_file_4 = PythonOperator(
        task_id=f'process_2012_4',
        python_callable=process_file,
        op_args=['2012-4'],
        provide_context=True
    )

    task_file_5 = PythonOperator(
        task_id=f'process_2012_5',
        python_callable=process_file,
        op_args=['2012-5'],
        provide_context=True
    )

    # This taks corresponds to processing the validation file. executed after all previous files
    task_validation_file = PythonOperator(
        task_id=f'process_validation',
        python_callable=process_validation,
        op_args=['validation'],
        provide_context=True
    )

    task_file_1 >> task_file_2 >> task_file_3 >> task_file_4 >> task_file_5 >> task_validation_file
