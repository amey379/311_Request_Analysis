from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
#PYTHON_PATH = "/home/amey8/anaconda3/envs/base_env/bin/python3"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 9),
    'retries': 1,
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Run all ETL scripts in sequence',
    schedule_interval=None,
    catchup=False
)

PYTHON_PATH = "/home/amey379/airflow-venv/bin/python3"  # Update to your Python path

stage_load = BashOperator(
    task_id='stage_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/stage_load.py',
    dag=dag
)

lookup_load = BashOperator(
    task_id='lookup_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/lookup_load.py',
    dag=dag
)

dim_date_time_load = BashOperator(
    task_id='dim_date_time_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/dim_date_time_load.py',
    dag=dag
)

dim_location_load = BashOperator(
    task_id='dim_location_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/dim_location_load.py',
    dag=dag
)

dim_request_dtl_load = BashOperator(
    task_id='dim_request_dtl_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/dim_request_dtl_load.py',
    dag=dag
)

dim_source_load = BashOperator(
    task_id='dim_source_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/dim_source_load.py',
    dag=dag
)

fact_311_request_load = BashOperator(
    task_id='fact_311_request_load',
    bash_command=f'{PYTHON_PATH} /home/amey379/airflow/scripts/fact_311_request_load.py',
    dag=dag
)

stage_load >> lookup_load >> dim_date_time_load >> dim_location_load >> dim_request_dtl_load >> dim_source_load >> fact_311_request_load
