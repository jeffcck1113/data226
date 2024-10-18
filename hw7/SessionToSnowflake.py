from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_stage():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        sql = """
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        URL = 's3://s3-geospatial/readonly/'
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Failed to create stage!!!!!!")
        raise e

@task
def load_data():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        
        # Execute the first COPY command
        copy_user_sql = """
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv
        ON_ERROR = 'CONTINUE';
        """
        cur.execute(copy_user_sql)
        
        # Execute the second COPY command
        copy_session_sql = """
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv
        ON_ERROR = 'CONTINUE';
        """
        cur.execute(copy_session_sql)
        cur.execute("COMMIT;")
        
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Failed to load data: {e}")
        raise e

# Define the DAG
default_args = {
    'start_date': datetime(2024, 10, 15),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='SessionToSnowflake',
    default_args=default_args,
    schedule_interval=None,
    tags=['ETL']
) as dag:

    # Define task dependencies
    create_stage_task = create_stage()
    load_data_task = load_data()

    create_stage_task >> load_data_task