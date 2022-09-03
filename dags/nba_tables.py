from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from chuckwalla2.etl.nba import create_all, drop_all,\
    games_extract, games_transform_and_load,\
    box_scores_extract, box_scores_transform_and_load
from datetime import datetime, timedelta


with DAG(
    'nba_tables',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='NBA Table re-creation DAG. Run this to drop all NBA data and recreate the tables',
    schedule_interval=timedelta(days=100000),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    arguments = {
        "date_string": "{{ ds }}",
        "production": False
    }

    drop_all_operator = PythonOperator(
        python_callable=drop_all.drop_all,
        op_kwargs=arguments,
        task_id="drop_all"
    )

    create_all_operator = PythonOperator(
        python_callable=create_all.create_all,
        op_kwargs=arguments,
        task_id="create_all"
    )

    drop_all_operator >> create_all_operator

