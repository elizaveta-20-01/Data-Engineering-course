from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
from dbt_operator import DbtOperator

from python_scripts.train_model import process_iris_data


ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
PROJECT_DIR = os.getenv('AIRFLOW_HOME') + "/dags/dbt/homework"
PROFILE = 'homework'

env_vars = {
    'ANALYTICS_DB': ANALYTICS_DB,
    'DBT_PROFILE': PROFILE,
    'DBT_LOG_PATH': '/tmp',
    'DBT_TARGET_PATH': '/tmp/dbt_target'
}

dbt_vars = {
    'is_test': False,
    'data_date': '{{ ds }}',
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id="process_iris",
    default_args=default_args,
    start_date=datetime(2025, 4, 22),
   # end_date=datetime(2025, 4, 24),
    schedule_interval='0 1 22-24 4 *',
    catchup=False,
    tags=["iris", "ml", "dbt"],
    template_searchpath=['/opt/airflow/dags/sql']
) 


dbt_seed = DbtOperator(
    task_id='dbt_seed',
    dag=dag,
    command='seed',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    env_vars=env_vars,
    vars=dbt_vars
)

add_stage = DbtOperator(
    task_id='dbt_add_stage',
    dag=dag,
    command='run',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    models = ['staging'],
    env_vars=env_vars,
    vars=dbt_vars
)

add_mart = DbtOperator(
    task_id='dbt_add_mart',
    dag=dag,
    command='run',
    profile=PROFILE,
    project_dir=PROJECT_DIR,
    models = ['mart'],
    env_vars=env_vars,
    vars=dbt_vars
)

iris_ml = PythonOperator(
    task_id='iris_ml_processor',
    dag=dag,
    python_callable=process_iris_data,
    provide_context=True,
)


dbt_seed >> add_stage >> add_mart >> iris_ml 