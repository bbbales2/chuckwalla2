from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import chuckwalla2.etl as etl
import chuckwalla2.schemas as schemas
from datetime import datetime, timedelta


with DAG(
    'nba_v1',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2020, 1, 1),
    catchup=False,
) as dag:
    arguments = {
        "date_string" : "{{ ds }}",
        "production" : False
    }

    create_all = PythonOperator(
        etl.nba.create_all,
        op_kwargs = arguments
    )

    extract = PythonOperator(
        etl.nba.raw.games.extract,
        op_kwargs = arguments
    )

    transform_and_load = PythonOperator(
        etl.nba.games.transform_and_load,
        op_kwargs = arguments
    )

    create_all >> extract >> transform_and_load
