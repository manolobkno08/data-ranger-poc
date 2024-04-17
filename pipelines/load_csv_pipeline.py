import pandas as pd
import logging
from typing import Tuple
from utils.constants import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT
from utils.db import DatabaseManager
from airflow.models import TaskInstance


def get_stats_from_db() -> Tuple[int, int, int, int]:
    """
    Retrieve statistics from the 'data' table in the database

    Returns:
        A tuple containing the count of rows, average price, minimum price, and maximum price
        If no data is found, returns None
    """
    db_manager = DatabaseManager(
        DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT)
    query_stats = """
            SELECT
                COUNT(id) AS rows_count,
                FLOOR(AVG(price)) AS avg_price,
                MIN(price) AS min_price,
                MAX(price) AS max_price
            FROM
                data;
            """

    try:
        res = db_manager.execute_query(query=query_stats, fetch=True)
        return res[0] if res else None
    finally:
        db_manager.close()


def open_and_save_file_into_db(filename: str, total_rows: int, sum_prices: float, min_price: float, max_price: float) -> Tuple[int, float, float, float]:
    """
    Open a CSV file, process its content by micro batches, and save data into the database

    Args:
        filename: The name of the CSV file to process
        total_rows: The total number of rows processed
        sum_prices: The sum of the 'price' values processed
        min_price: The minimum 'price' value processed
        max_price: The maximum 'price' value processed

    Returns:
        A tuple containing updated statistics after processing the file
    """
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
            avg_price = int(sum_prices / total_rows if total_rows else 0)

            # Iterate over dataframe object for save into DB
            for index, row in chunk_df.iterrows():
                # Query and params for save row
                save_row_query = """
                INSERT INTO data (timestamp, price, user_id) VALUES (%s, %s, %s)
                """
                save_row_args = (row['timestamp'], row['price'] if pd.notna(
                    row['price']) else None, row['user_id'])

                # Query and params for recreate statistics
                save_statistics_query = """
                INSERT INTO statistics (filename, total_rows, avg_price, min_price, max_price) VALUES (%s, %s, %s, %s, %s)
                """
                save_statistics_args = (
                    f"{filename}.csv", total_rows, avg_price, int(min_price), int(max_price))

                # Invoke db instance and save data
                with DatabaseManager(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT) as db_manager:
                    for query, args in [(save_row_query, save_row_args), (save_statistics_query, save_statistics_args)]:
                        try:
                            db_manager.execute_query(
                                query=query, args=args, fetch=False)
                        except Exception as e:
                            logging.error(f"Error executing query: {e}")

                logging.info(
                    f"[Final Statistics] Rows count ({total_rows}) | Avg price ({avg_price}) | Min price ({int(min_price)}) | Max price ({int(max_price)})")

        logging.info(
            f"File: {filename}.csv processed successfully")

    except Exception as e:
        logging.error(f"Error processing file {filename}.csv: {e}")

    return total_rows, sum_prices, min_price, max_price


def process_file(filename: str, ti: TaskInstance) -> None:
    """
    Process a single CSV file and save statistics using XCom to pass them between tasks

    Args:
        filename: The name of the CSV file to process
        ti: The TaskInstance object
    """
    # Define variables for statistics, if already exist get the value from previus task
    total_rows = ti.xcom_pull(key='total_rows') or 0
    sum_prices = ti.xcom_pull(key='sum_prices') or 0
    min_price = ti.xcom_pull(key='min_price') or float('inf')
    max_price = ti.xcom_pull(key='max_price') or float('-inf')

    # Process csv files
    total_rows, sum_prices, min_price, max_price = open_and_save_file_into_db(
        filename, total_rows, sum_prices, min_price, max_price)

    # Push last statistic data to the next task
    ti.xcom_push(key='total_rows', value=total_rows)
    ti.xcom_push(key='sum_prices', value=sum_prices)
    ti.xcom_push(key='min_price', value=min_price)
    ti.xcom_push(key='max_price', value=max_price)


def process_validation(filename: str, ti: TaskInstance) -> None:
    """
    Process the validation CSV file and compare the statistics before and after processing

    Args:
        filename: The name of the CSV file to process for validation
        ti: The TaskInstance object
    """
    # Get current stats
    total_rows = ti.xcom_pull(key='total_rows')
    sum_prices = ti.xcom_pull(key='sum_prices')
    min_price = ti.xcom_pull(key='min_price')
    max_price = ti.xcom_pull(key='max_price')
    avg_price = int(sum_prices / total_rows)

    logging.info(
        f"[Statistics in execution] Rows count ({total_rows}) | Avg price ({avg_price}) | Min price ({int(min_price)}) | Max price ({int(max_price)})")

    try:
        stats = get_stats_from_db()
        logging.info(
            f"[Statistics from DB] Rows count ({stats[0]}) | Avg price ({stats[1]}) | Min price ({stats[2]}) | Max price ({stats[3]})")

        # Process validation.csv file
        open_and_save_file_into_db(
            filename, total_rows, sum_prices, min_price, max_price)

        # Get new statistics
        stats = get_stats_from_db()
        logging.info(
            f"[Statistics with validation] Rows count ({stats[0]}) | Avg price ({stats[1]}) | Min price ({stats[2]}) | Max price ({stats[3]})")
    except Exception as e:
        logging.error(f"Error in processing file {filename}.csv: {e}")
