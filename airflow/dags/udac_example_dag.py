from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    CreateRedshiftTableOperator
)

from helpers import SqlQueries

# required for additional DQ checks
import operator


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
}

# https://airflow.apache.org/docs/apache-airflow/1.10.14/dag-run.html?highlight=email_on_retry
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          # below for quick experiments (note: not useful)
          #,schedule_interval='@daily'
          #,catchup=True
          #,start_date=datetime(2018, 11, 1)
          #,end_date=datetime(2018, 11, 30)
        )


begin_execution = DummyOperator(task_id='Begin_execution',  dag=dag)

begin_table_create = DummyOperator(task_id='Begin_table_create',  dag=dag)

##############################################################
# Create tables

create_staging_events_table = CreateRedshiftTableOperator(
    task_id="Create_staging_events_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    drop_existing=True,
    table_create_sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = CreateRedshiftTableOperator(
    task_id="Create_staging_songs_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    drop_existing=True,
    table_create_sql=SqlQueries.staging_songs_table_create
)

create_songplays_fact_table = CreateRedshiftTableOperator(
    task_id="Create_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    drop_existing=True,
    table_create_sql=SqlQueries.songplays_table_create
)

create_songs_dim_table = CreateRedshiftTableOperator(
    task_id="Create_songs_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    drop_existing=True,
    table_create_sql=SqlQueries.songs_table_create
)

create_artists_dim_table = CreateRedshiftTableOperator(
    task_id="Create_artists_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    drop_existing=True,
    table_create_sql=SqlQueries.artists_table_create
)

create_users_dim_table = CreateRedshiftTableOperator(
    task_id="Create_users_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    drop_existing=True,
    table_create_sql=SqlQueries.users_table_create
)

create_time_dim_table = CreateRedshiftTableOperator(
    task_id="Create_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    drop_existing=True,
    table_create_sql=SqlQueries.time_table_create
)


##############################################################
# Stage tables

begin_staging_table_load = DummyOperator(task_id='Begin_staging_table_load',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    table_load_sql=SqlQueries.staging_events_copy,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    #s3_key="log-data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json",
    s3_key="log-data",
    s3_region="us-west-2",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    table_load_sql=SqlQueries.staging_songs_copy,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data",
    #s3_key="song-data/A/A/A",
    s3_region="us-west-2"
)


##############################################################
# Load fact tables

begin_fact_table_load = DummyOperator(task_id='Begin_fact_table_load',  dag=dag)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_to_load_tbl=SqlQueries.songplays_table_insert
)

##############################################################
# Load dimension tables

begin_dim_table_load = DummyOperator(task_id='Begin_dim_table_load',  dag=dag)

load_users_dim_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_to_load_tbl=SqlQueries.users_table_insert,
    reset_table=True    
)

load_songs_dim_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_to_load_tbl=SqlQueries.songs_table_insert,
    reset_table=True
)

load_artists_dim_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_to_load_tbl=SqlQueries.artists_table_insert,
    reset_table=True
)

load_time_dim_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_to_load_tbl=SqlQueries.time_table_insert,
    reset_table=True
)


##############################################################
# Run quality checks

check_staging_events_table = DataQualityOperator(
    task_id='check_staging_events_fact_table',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift"
)

check_staging_songs_table = DataQualityOperator(
    task_id='check_staging_songs_fact_table',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
)

check_songplays_fact_table = DataQualityOperator(
    task_id='check_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift"    
)

check_songs_dim_table = DataQualityOperator(
    task_id='check_songs_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift"    
)

check_artists_dim_table = DataQualityOperator(
    task_id='check_artists_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift"    
)

check_users_dim_table = DataQualityOperator(
    task_id='check_users_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift"
)

check_time_dim_table = DataQualityOperator(
    task_id='check_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    # additional data quality checks
    dq_checks=[
        {
            "test_description": "Total number of rows must be greater than 0.",
            "test_sql": "SELECT COUNT(*) FROM time",
            "expected_value": 0,
            "comparison": operator.gt
        },
         {
            "test_description": "Primary key column must be distinct: total rows must equate total distinct unique_key values.",
            "test_sql": "SELECT count(*) - count(distinct start_time) FROM time",
            "expected_value": 0,
            "comparison": operator.eq
        }
    ]
)

##############################################################
# End
                                   
stop_execution = DummyOperator(task_id='Stop_execution',  dag=dag)



##############################################################
# Graph view ordering. Note: we check right after loading.


##############################################################
# start
begin_execution >> begin_table_create

##############################################################
# Create tables
begin_table_create >> create_staging_events_table
begin_table_create >> create_staging_songs_table
begin_table_create >> create_songplays_fact_table
begin_table_create >> create_songs_dim_table
begin_table_create >> create_artists_dim_table
begin_table_create >> create_users_dim_table
begin_table_create >> create_time_dim_table

create_staging_events_table >> begin_staging_table_load
create_staging_songs_table >> begin_staging_table_load
create_songplays_fact_table >> begin_staging_table_load
create_songs_dim_table >> begin_staging_table_load
create_artists_dim_table >> begin_staging_table_load
create_users_dim_table >> begin_staging_table_load
create_time_dim_table >> begin_staging_table_load

##############################################################
# Staging Load
begin_staging_table_load >> stage_events_to_redshift
begin_staging_table_load >> stage_songs_to_redshift

stage_events_to_redshift >> check_staging_events_table
stage_songs_to_redshift >> check_staging_songs_table

check_staging_events_table >> begin_fact_table_load
check_staging_songs_table >> begin_fact_table_load

##############################################################
# fact Load
begin_fact_table_load >> load_songplays_fact_table
load_songplays_fact_table >> check_songplays_fact_table
check_songplays_fact_table >> begin_dim_table_load

##############################################################
# dim Load
begin_dim_table_load >> load_songs_dim_table 
begin_dim_table_load >> load_users_dim_table
begin_dim_table_load >> load_artists_dim_table
begin_dim_table_load >> load_time_dim_table

load_songs_dim_table >> check_songs_dim_table
load_users_dim_table >> check_users_dim_table
load_artists_dim_table >> check_artists_dim_table
load_time_dim_table >> check_time_dim_table


check_songs_dim_table >> stop_execution
check_users_dim_table >> stop_execution
check_artists_dim_table >> stop_execution
check_time_dim_table >> stop_execution

##############################################################
# end

