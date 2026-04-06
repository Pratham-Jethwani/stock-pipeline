from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

PROJECT_DIR = '/opt/airflow/project'
PYTHON = '/usr/local/bin/python'  

default_args = {
    'owner':            'pratham',
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

def run_script(script_path):
    result = subprocess.run(
        [PYTHON, script_path],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    if result.returncode != 0:
        raise Exception(f"script failed:\n{result.stderr}")
    print(result.stdout)

def task_producer():
    run_script(f'{PROJECT_DIR}/ingestion/producer.py')

def task_consumer():
    import time
    run_script(f'{PROJECT_DIR}/kafka_consumer/consumer.py')
     # wait 2 mins for consumer to flush to s3

def task_bronze_to_silver():
    run_script(f'{PROJECT_DIR}/s3/bronze_to_silver.py')

def task_silver_to_gold():
    run_script(f'{PROJECT_DIR}/s3/silver_to_gold.py')

def task_load_to_redshift():
    run_script(f'{PROJECT_DIR}/s3/load_to_redshift.py')

# DAG 1 — runs during market hours every 60 seconds
with DAG(
    dag_id='stock_intraday_producer',
    schedule_interval='* 3-10 * * 1-5',  # every minute 9:15–15:30 IST
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag1:

    t1 = PythonOperator(
        task_id='fetch_and_produce',
        python_callable=task_producer,
    )

    t2 = PythonOperator(
        task_id='consume_to_bronze',
        python_callable=task_consumer,
    )

    t1 >> t2

# DAG 2 — runs once after market close
with DAG(
    dag_id='stock_daily_processing',
    schedule_interval='30 10 * * 1-5',  # 4 PM IST
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag2:

    t3 = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=task_bronze_to_silver,
    )
    t4 = PythonOperator(
        task_id='silver_to_gold',
        python_callable=task_silver_to_gold,
    )
    t5 = PythonOperator(
        task_id='load_to_redshift',
        python_callable=task_load_to_redshift,
    )
    t6 = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/stock_pipeline && dbt run --profiles-dir /opt/airflow/project/stock_pipeline',
    )
    t7 = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/stock_pipeline && dbt test --profiles-dir /opt/airflow/project/stock_pipeline',
    )

    t3 >> t4 >> t5 >> t6 >> t7