from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    CreateTablesOperator,
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,)
from helpers import SqlQueries, LoadModes, DataCheck
from operator import eq, gt

REDSHIFT_CONNECTION_ID = 'redshift'
AWS_CONNECTION_ID = 'aws_credentials'
S3_SONGS_DATA = 's3://udacity-dend/song_data/A/R/M'
S3_EVENTS_DATA = 's3://udacity-dend/log_data'
S3_EVENTS_MANIFEST_JSON = 's3://udacity-dend/log_json_path.json'
RAW_DATA_REGION = 'us-west-2'

default_args = {
    'owner': 'moshe',
    'start_date': datetime(2019,1,1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_operator = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    aws_connection_id=AWS_CONNECTION_ID,
    s3file=S3_EVENTS_DATA,
    table='public.staging_events',
    region=RAW_DATA_REGION,
    json_statement=S3_EVENTS_MANIFEST_JSON
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    aws_connection_id=AWS_CONNECTION_ID,
    s3file=S3_SONGS_DATA,
    table='public.staging_songs',
    region=RAW_DATA_REGION,
    json_statement='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table='public.songplays',
    load_source=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table='public.users',
    load_source=SqlQueries.user_table_insert,
    load_mode=LoadModes.overwrite,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table='public.songs',
    load_source=SqlQueries.song_table_insert,
    load_mode=LoadModes.overwrite,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table='public.artists',
    load_source=SqlQueries.artist_table_insert,
    load_mode=LoadModes.overwrite,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table='public."time"',
    load_source=SqlQueries.time_table_insert,
    load_mode=LoadModes.overwrite,
)


run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks.artists',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        checks=[
            DataCheck(
                "SELECT COUNT(*) FROM public.songplays WHERE playid is null",
                eq,
                0),
            DataCheck(
                "SELECT COUNT(*) FROM public.songs WHERE songid is null",
                eq,
                0),
            DataCheck(
                "SELECT COUNT(*) FROM public.users WHERE userid is null",
                eq,
                0),
            DataCheck(
                "SELECT COUNT(*) FROM public.artists WHERE artistid is null",
                eq,
                0),
            DataCheck(
                "SELECT COUNT(*) FROM public.\"time\" WHERE start_time is null",
                eq,
                0),
        ] + [
            DataCheck(
                f"SELECT COUNT(*) FROM {table}",
                gt,
                0)
            for table in [
                "public.songplays",
                "public.users",
                "public.songs",
                "public.artists",
                "public.\"time\""
            ]
        ]
    )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_operator  >> [
    stage_events_to_redshift, 
    stage_songs_to_redshift
    ] >> load_songplays_table >> [
        load_user_dimension_table,
        load_artist_dimension_table,
        load_song_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator