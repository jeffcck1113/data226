from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Import TriggerDagRunOperator
import logging
from datetime import datetime
import requests
import pandas as pd


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


# Extract task
@task
def extract(api_key):
    symbol_id = 'BITSTAMP_SPOT_BTC_USD'
    endpoint = f'https://rest.coinapi.io/v1/ohlcv/{symbol_id}/latest'

    headers = {'X-CoinAPI-Key': api_key}
    params = {
        'period_id': '15MIN',
        'limit': 100
    }

    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()  # Raise error for unsuccessful requests
    data = response.json()
    return data

# Transform task
@task
def transform(data):
    df = pd.DataFrame(data)
    # Convert timestamp to datetime format and change timezone to Los Angeles
    df['time_period_start'] = pd.to_datetime(df['time_period_start']).dt.tz_convert('America/Los_Angeles')
    
    df['symbol'] = 'Bitcoin'

    # Select the specified columns
    df = df[['symbol', 'time_period_start', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded']]

    
    # Convert Timestamp to string to make it JSON serializable
    df['time_period_start'] = df['time_period_start'].astype(str)
    
    # Convert dataframe to dictionary for easier insertion into Snowflake
    return df.to_dict(orient='records')


# Load task
@task
def load(cur, data, target_table):
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
            TIME TIMESTAMP_TZ PRIMARY KEY,
            symbol VARCHAR(10),
            PRICE_OPEN FLOAT,
            PRICE_HIGH FLOAT,
            PRICE_LOW FLOAT,
            PRICE_CLOSE FLOAT,
            VOLUME_TRADED FLOAT
        )
        """)

        insert_sql = f"""
        INSERT INTO {target_table} (TIME, symbol, PRICE_OPEN, PRICE_HIGH, PRICE_LOW, PRICE_CLOSE, VOLUME_TRADED)
        VALUES (%(time_period_start)s, %(symbol)s, %(price_open)s, %(price_high)s, %(price_low)s, %(price_close)s, %(volume_traded)s)
        """
        for row in data:
            cur.execute(insert_sql, row)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()

# Define the DAG
with DAG(
    dag_id='bitcoin_price',
    start_date=datetime(2024, 11, 11),
    catchup=False,
    tags=['ETL'],
    schedule_interval='*/15 * * * *',
) as dag:
    target_table = 'dev.raw_data.bitcoin_price'
    api_key = Variable.get('coinapi_key')
    cur = return_snowflake_conn()
    
    extracted_data = extract(api_key)
    transformed_data = transform(extracted_data)
    load_data = load(cur, transformed_data, target_table)

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="BuildELT_dbt",  # DAG ID of the target DAG
        wait_for_completion=True,      # Set to True if you want DAG 1 to wait for DAG 2 to finish
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trading",
        trigger_dag_id="get_taking_singal_trade",  # DAG ID of the target DAG
        wait_for_completion=False,      # Set to True if you want DAG 1 to wait for DAG 2 to finish
    )

    load_data >> trigger_dbt_dag >> trigger_dag2

