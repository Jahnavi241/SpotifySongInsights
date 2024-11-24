import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from popular_songs import popular_songs_pipeline
from recent_playlist import spotify_datapipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2024,11,23),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}


def get_recent_played_songs():
    print("Started")
    df=spotify_datapipeline()
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    sql_table = df.to_sql('recent_playlists', engine, if_exists='replace')
    print(sql_table)
    print("Completed")
    return sql_table


def get_popular_songs():
    print("Started")
    df=popular_songs_pipeline()
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    sql_table = df.to_sql('popular_playlists', engine, if_exists='replace')
    print(sql_table)
    print("Completed")
    return sql_table



with DAG('spotify_scheduler_dag', default_args=default_args, description='Spotify ETL process hourly', schedule_interval=None) as dag:

    recent_songs_table= PostgresOperator(
        task_id='recent_songs_table',
        postgres_conn_id='postgre_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS recent_playlists(
            id SERIAL PRIMARY KEY,
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            track_number INT,
            popularity INT,
            played_time VARCHAR(200),
            played_date VARCHAR(200)
        );
        """
    )

    popular_songs_table = PostgresOperator(
        task_id='popular_songs_table',
        postgres_conn_id='postgre_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS popular_playlists(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            popularity INT,
            played_time VARCHAR(200),
            count INT,
            popular BOOLEAN,
            CONSTRAINT primary_key_constraint PRIMARY KEY (song_name, artist_name)
        );
        """
    )

    recent_songs = PythonOperator(
        task_id='recent_songs',
        python_callable=get_recent_played_songs,
    )

    popular_songs = PythonOperator(
        task_id='popular_songs',
        python_callable=get_popular_songs,
    )

recent_songs_table >> popular_songs_table >> recent_songs >> popular_songs