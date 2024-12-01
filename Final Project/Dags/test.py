#%%

from datetime import datetime
import requests
import time
import pandas as pd
import numpy as np
# from binance import Client
import snowflake.connector
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

#%%

def line_notify(msg, token):
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": "Bearer " + token}
    data = {"message": msg}
    requests.post(url, headers=headers, data=data)

def return_snowflake_conn():
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user='JJCCK1113188',
        password='Azxkn4695qwew',
        account='DSB93743',  # Example: 'xyz12345.us-east-1'
        warehouse='compute_wh',
        database='dev'
    )
    return conn.cursor()






# @task
def get_taking_singal():
    cur = return_snowflake_conn()
    requ_data=cur.execute('SELECT * FROM dev.analytics.bitcoin_moving_avg')
    data=[]
    for i in requ_data:
        data.append(list(i))

    data=pd.DataFrame(data,columns=['Symbol','Time','Close Price','60_MA','5_MA'])


    sma60 = data.iloc[0,3]
    sma5 = data.iloc[0,4]
    psma60 = data.iloc[1,3]
    psma5 = data.iloc[1,4]

    ret = "Hold"
    if sma5 > sma60 and psma5 < psma60:
        ret = "BUY"

    elif sma5 < sma60 and psma5 > psma60:
        ret = "SELL"

    if ret == "BUY" or ret == "SELL":
        line_notify(
            "\n"
            + str(data["Symbol"][0])
            + "\n"
            + str(round(float(data["Close Price"][0]), 2))
            + "\n"
            + ret
            + "\n"
            + "------------------------",
            note_tok,
        )
    elif int(time.strftime("%M")) <5:
        print('123')
        line_notify(
            "\n"
            + str(data["Symbol"][0])
            + "\n"
            + str(round(float(data["Close Price"][0]), 2))
            + "\n"
            + ret
            + "\n"
            + "------------------------",
            note_tok,
        )

    return ret
'''
@tesk
def trade(ret):
    PUBLIC = Variable.get('PUBLIC') 
    SECRET = Variable.get('SECRET') 

    client = Client(PUBLIC, SECRET)
    client.API_URL = "https://api.binance.com/api"

    sym='BTCUSDT'
    QUANTITY=0.0001
    client.futures_create_order(symbol=sym,
                    side=ret,
                    type="MARKET",
                    quantity=QUANTITY,
                )
'''






with DAG(
    dag_id='get_taking_singal_trade',
    start_date=datetime(2024, 11, 11),
    catchup=False,
    schedule_interval = None,
) as dag:
    trading_mode=0
    note_tok = Variable.get('api_line') 


    get_taking_singal()

    # if trading_mode:
    #     trade()

    



    


