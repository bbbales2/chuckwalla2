from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from chuckwalla2.etl.nba import \
    games_extract, games_transform_and_load,\
    box_scores_extract, box_scores_transform_and_load,\
    play_by_play_extract, play_by_play_transform_and_load
from datetime import datetime, timedelta


with DAG(
    'nba',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='NBA DAG. Runs daily to download new data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 10, 1),
    catchup=True,
) as dag:
    arguments = {
        "date_string": "{{ ds }}",
        "production": True
    }

    games_extract_operator = PythonOperator(
        python_callable=games_extract.extract,
        op_kwargs=arguments,
        task_id="games_extract"
    )

    games_transform_and_load_operator = PythonOperator(
        python_callable=games_transform_and_load.transform_and_load,
        op_kwargs=arguments,
        task_id="games_transform_and_load"
    )

    box_scores_extract_operator = PythonOperator(
        python_callable=box_scores_extract.extract,
        op_kwargs=arguments,
        task_id="box_score_extract"
    )

    box_scores_transform_and_load_operator = PythonOperator(
        python_callable=box_scores_transform_and_load.transform_and_load,
        op_kwargs=arguments,
        task_id="box_score_transform_and_load"
    )

    play_by_play_extract_operator = PythonOperator(
        python_callable=play_by_play_extract.extract,
        op_kwargs=arguments,
        task_id="play_by_play_extract"
    )

    play_by_play_transform_and_load_operator = PythonOperator(
        python_callable=play_by_play_transform_and_load.transform_and_load,
        op_kwargs=arguments,
        task_id="play_by_play_transform_and_load"
    )

    (
        games_extract_operator
        >> games_transform_and_load_operator
        >> box_scores_extract_operator
        >> box_scores_transform_and_load_operator
        >> play_by_play_extract_operator
        >> play_by_play_transform_and_load_operator
    )
