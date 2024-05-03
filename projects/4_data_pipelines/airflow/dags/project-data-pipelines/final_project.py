from datetime import datetime, timedelta
import pendulum
import os

from airflow.models import Variable
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_helpers.sql_statements import SqlQueries

# default parameters
# - The DAG does not have dependencies on past runs
# - On failure, the task are retried 3 times
# - Retries happen every 5 minutes
# - Catchup is turned off
# - Do not email on retry
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

s3_stage_bucket = Variable.get('s3_stage_bucket')
s3_stage_events_key = Variable.get('s3_stage_events_key')
s3_stage_songs_key = Variable.get('s3_stage_songs_key')
s3_json_format_file = Variable.get('s3_json_format_file')
s3_json_path = f"s3://{s3_stage_bucket}/{s3_json_format_file}"

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1,
)
def data_pipelines_final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # staging_events
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket=s3_stage_bucket,
        s3_key=s3_stage_events_key,
        region='us-east-1',
        json_format=s3_json_path,
        delete_on_load=True,
    )

    # staging_songs
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=s3_stage_bucket,
        s3_key=s3_stage_songs_key,
        region='us-east-1',
        json_format='auto ignorecase',
        delete_on_load=True,
    )

    # songplays table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
        delete_on_load=True
    )

    # users table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        delete_on_load=True
    )

    # songs table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        delete_on_load=True
    )

    # artists table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        delete_on_load=True
    )

    # time table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        delete_on_load=True
    )

    # Tables should have records
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

data_pipelines_final_project_dag = data_pipelines_final_project()