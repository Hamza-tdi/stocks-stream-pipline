import random
import time
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests
def store_data_task(**kwargs):
    records = kwargs.get['ti'].xcom_pull(key='records')
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    for record in records:
        pg_hook.run("""
                        INSERT INTO stocks_data (
                            symbol, current_price, change, percent_change,
                            high_price_of_the_day, low_price_of_the_day,
                            open_price_of_the_day, previous_close_price
                        ) VALUES (
                            %(symbol)s, %(current_price)s, %(change)s, %(percent_change)s,
                            %(high_price_of_the_day)s, %(low_price_of_the_day)s,
                            %(open_price_of_the_day)s, %(previous_close_price)s
                        )
                        ON CONFLICT (symbol) DO UPDATE SET
                            current_price = EXCLUDED.current_price,
                            change = EXCLUDED.change,
                            percent_change = EXCLUDED.percent_change,
                            high_price_of_the_day = EXCLUDED.high_price_of_the_day,
                            low_price_of_the_day = EXCLUDED.low_price_of_the_day,
                            open_price_of_the_day = EXCLUDED.open_price_of_the_day,
                            previous_close_price = EXCLUDED.previous_close_price
                    """, record)
        print("Data inserted successfully.")
def fetch_data_task(**kwargs):
    print('Fetching Data')
    variables = Variable.get('variables', deserialize_json=True)
    url = variables[0]['url']
    token = variables[0]['api_key']
    records = []
    for i in range(len(variables[0]['companies'][0:3])):
        quote = variables[0]['quote'].replace('{SYMBOL}', variables[0]['companies'][i]['symbol'])
        res = requests.get(f'{url}{quote}{token}')
        if res.status_code == 429:
            print(f'API Limit Reached')
            time.sleep(10)
        elif res.status_code == 200:
            record = {
                'id': f"{variables[0]['companies'][i]['symbol']}_{res['t']}",
                'symbol': variables[0]['companies'][i]['symbol'],
                'current_price': res['c'],
                'change': res['d'],
                'percent_change': res['d'],
                'high_price_of_the_day': res['h'],
                'low_price_of_the_day': res['l'],
                'open_price_of_the_day': res['o'],
                'previous_close_price': res['pc'],
                'timestamp': res['t']
            }
            print(record)
            records.append(record)
        else:
            print(f"Unexpected status code: {res.status_code}")
            return None
    kwargs['ti'].xcom_push(key='records', value=records)
def check_market_status_task(**kwargs):
    print('Checking Market Status')
    variables = Variable.get('variables', deserialize_json=True)
    url = variables[0]['url']
    token = variables[0]['api_key']
    status_url = variables[0]['status_check'].replace('{EXCHANGE}', 'US')
    print(f'{url}{status_url}{token}')
    res = requests.get(f'{url}{status_url}{token}')
    if res.status_code == 429:
        print(f'API Limit Reached')
        time.sleep(10)
    elif res.status_code == 200:
        print(res.json())
    else:
        print(f"Unexpected status code: {res.status_code}")
        return None
with DAG(
dag_id="stream_stocks_data",
schedule_interval="* * * * *",
default_args={
        "owner": "airflow",
"retries": 1,
"retry_delay": timedelta(minutes=1),
"start_date":datetime(2024,11,29)
},
    catchup=False
                        ) as f:
    fetch_data =PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        provide_context=True,
    )
    check_market_status_task = PythonOperator(
        task_id="check_market_status_task",
        python_callable=check_market_status_task,
        provide_context=True
    )
    create_table_task = PostgresOperator(
        task_id='create_postgres_stocks_table',
        postgres_conn_id='postgres_local',
        sql='''
        CREATE TABLE IF NOT EXISTS stocks_data (
            id VARCHAR(25) PRIMARY KEY,
            symbol VARCHAR(10),
            current_price DECIMAL(10, 2) NOT NULL,
            change DECIMAL(10, 2) NOT NULL,
            percent_change DECIMAL(5, 2) NOT NULL,
            high_price_of_the_day DECIMAL(10, 2),
            low_price_of_the_day DECIMAL(10, 2),
            open_price_of_the_day DECIMAL(10, 2),
            previous_close_price DECIMAL(10, 2),
            timestamp BIGINT
        )
        '''
    )

create_table_task >> check_market_status_task >> fetch_data_task >>store_data_task