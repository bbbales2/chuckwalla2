from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from chuckwalla2.etl.nba import \
    raw_games, clean_games, dw_games,\
    raw_box_scores, clean_box_scores, dw_box_scores,\
    raw_play_by_play, clean_play_by_play, dw_play_by_play
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
        "partition_date": "{{ ds }}",
    }

    raw_games_operator = PythonOperator(
        python_callable=raw_games.run,
        op_kwargs=arguments,
        task_id="raw_games"
    )

    clean_games_operator = PythonOperator(
        python_callable=clean_games.run,
        op_kwargs=arguments,
        task_id="clean_games"
    )

    dw_games_operator = PythonOperator(
        python_callable=dw_games.run,
        op_kwargs=arguments,
        task_id="dw_games"
    )

    raw_box_scores_operator = PythonOperator(
        python_callable=raw_box_scores.run,
        op_kwargs=arguments,
        task_id="raw_box_scores"
    )

    clean_box_scores_operator = PythonOperator(
        python_callable=clean_box_scores.run,
        op_kwargs=arguments,
        task_id="clean_box_scores"
    )

    dw_box_scores_operator = PythonOperator(
        python_callable=dw_box_scores.run,
        op_kwargs=arguments,
        task_id="dw_box_scores"
    )
    
    raw_play_by_play_operator = PythonOperator(
        python_callable=raw_play_by_play.run,
        op_kwargs=arguments,
        task_id="raw_play_by_play"
    )

    clean_play_by_play_operator = PythonOperator(
        python_callable=clean_play_by_play.run,
        op_kwargs=arguments,
        task_id="clean_play_by_play"
    )

    dw_play_by_play_operator = PythonOperator(
        python_callable=dw_play_by_play.run,
        op_kwargs=arguments,
        task_id="dw_play_by_play"
    )

    (
        raw_games_operator
        >> clean_games_operator
        >> dw_games_operator
    )

    (
        dw_games_operator
        >> raw_box_scores_operator
        >> clean_box_scores_operator
        >> dw_box_scores_operator
    )

    (
        dw_games_operator
        >> raw_play_by_play_operator
        >> clean_play_by_play_operator
        >> dw_play_by_play_operator
    )
