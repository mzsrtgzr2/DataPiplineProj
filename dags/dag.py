from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    CreateTablesOperator,
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,)
from helpers import SqlQueries, LoadModes

REDSHIFT_CONNECTION_ID = 'redshift'
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

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
    s3file=os.path.join(
        Variable.get('s3_events_data'),
            # '{{execution_date.year}}',
            # '{{execution_date.month}}'
    ),
    table='public.staging_events',
    region=Variable.get('raw_data_region'),
    iam_role=Variable.get('copy_iam_role'),
    json_statement=Variable.get('s3_events_json_manifest')
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    s3file=Variable.get('s3_songs_data'),
    table='public.staging_songs',
    region=Variable.get('raw_data_region'),
    iam_role=Variable.get('copy_iam_role'),
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

fanout_tests_operator = DummyOperator(task_id='Fanout_tests',  dag=dag)


run_quality_checks = [
    DataQualityOperator(
        task_id='Run_data_quality_checks.artists',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        query='select count(artist_id) from public.artists',
        expected=1
    ),
    DataQualityOperator(
        task_id='Run_data_quality_checks.songs',
        dag=dag,
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        query='select count(song_id) from public.songs',
        expected=1
    ),
]

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_operator  >> [
    stage_events_to_redshift, 
    stage_songs_to_redshift
    ] >> load_songplays_table >> [
        load_user_dimension_table,
        load_artist_dimension_table,
        load_song_dimension_table,
        load_time_dimension_table
    ] >> fanout_tests_operator >> run_quality_checks >> end_operator