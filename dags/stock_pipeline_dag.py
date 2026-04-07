from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import subprocess

PROJECT_DIR = '/opt/airflow/project'
PYTHON      = '/usr/local/bin/python'

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
    run_script(f'{PROJECT_DIR}/kafka_consumer/consumer.py')

def task_bronze_to_silver():
    run_script(f'{PROJECT_DIR}/s3/bronze_to_silver.py')

def task_silver_to_gold():
    run_script(f'{PROJECT_DIR}/s3/silver_to_gold.py')

def task_load_to_redshift():
    run_script(f'{PROJECT_DIR}/s3/load_to_redshift.py')

with DAG(
    dag_id='stock_market_day',
    schedule_interval='15 3 * * 1-5',  # 9:15 AM IST = 3:15 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    # producer and consumer run in parallel
    with TaskGroup('intraday_collection') as intraday:
        t1 = PythonOperator(
            task_id='producer',
            python_callable=task_producer,
            execution_timeout=timedelta(hours=7),
        )
        t2 = PythonOperator(
            task_id='consumer',
            python_callable=task_consumer,
            execution_timeout=timedelta(hours=7),
        )

    # EOD processing runs after both producer and consumer exit
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
        bash_command='''
            cd /opt/airflow/project/stock_pipeline &&
            dbt run --profiles-dir /opt/airflow/project/stock_pipeline --threads 1
        ''',
    )
    t7 = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/stock_pipeline && dbt test --profiles-dir /opt/airflow/project/stock_pipeline',
    )

    intraday >> t3 >> t4 >> t5 >> t6 >> t7
    
    
    
    
    
    