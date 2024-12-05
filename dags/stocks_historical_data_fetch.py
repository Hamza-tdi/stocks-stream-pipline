import random
import time
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from psycopg2.errors import NotNullViolation
import requests
def fetch_data_task(**kwargs):
    passed = []
    records = []
    variables = Variable.get('variables', deserialize_json=True)
    for i in range(len(variables[0]['companies'][0:3])):
        url = 'https://api.polygon.io/v2/aggs/ticker/{SYMBOL}/range/1/day/2020-12-04/2024-12-04?adjusted=true&sort=desc&apiKey=W1hb4igasSSrIR9MV2ZXvXwnjFVitjWB'.replace('{SYMBOL}', variables[0]['companies'][i]['symbol'])
        res = requests.get(url)
        print(res.status_code)
        print(res.json())

        for result in res.json()['results']:
            print(result)
            print('INSIDE LOOP')
            passed.append(variables[0]['companies'][i]['symbol'])
            res_raw = res.json()
            record = {
                'id': f"{variables[0]['companies'][i]['symbol']}_{result['t']}",
                'symbol': variables[0]['companies'][i]['symbol'],
                'name': variables[0]['companies'][i]['name'],
                'current_price': result['c'],
                'change': None,
                'percent_change': None,
                'high_price_of_the_day': result['h'],
                'low_price_of_the_day': result['l'],
                'open_price_of_the_day': result['o'],
                'previous_close_price': None,
                'timestamp': result['t']
            }
            print(record)
            records.append(record)


    kwargs['ti'].xcom_push(key='records', value=records)
    print(records)
    print(f'passed: {passed}')

def store_data_task(**kwargs):
    records = kwargs.get('ti').xcom_pull(key='records')
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    for record in records:
        try:
            pg_hook.run("""
                    INSERT INTO stocks_data (
                        id,
                        symbol,
                        name,
                        current_price,
                        change,
                        percent_change,
                        high_price_of_the_day,
                        low_price_of_the_day,
                        open_price_of_the_day,
                        previous_close_price,
                        timestamp
                    ) VALUES (
                        %(id)s,
                        %(symbol)s,
                        %(name)s,
                        %(current_price)s,
                        %(change)s,
                        %(percent_change)s,
                        %(high_price_of_the_day)s,
                        %(low_price_of_the_day)s,
                        %(open_price_of_the_day)s,
                        %(previous_close_price)s,
                        %(timestamp)s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        name = EXCLUDED.name,
                        current_price = EXCLUDED.current_price,
                        change = EXCLUDED.change,
                        percent_change = EXCLUDED.percent_change,
                        high_price_of_the_day = EXCLUDED.high_price_of_the_day,
                        low_price_of_the_day = EXCLUDED.low_price_of_the_day,
                        open_price_of_the_day = EXCLUDED.open_price_of_the_day,
                        previous_close_price = EXCLUDED.previous_close_price,
                        timestamp = EXCLUDED.timestamp;
                """, parameters=record)
        except NotNullViolation as e:
            print('Record contains Null levels, Skipped.')
with DAG(
dag_id="fetch_historical_stocks_data",
schedule_interval="* * * * *",
default_args={
        "owner": "airflow",
"retries": 1,
"retry_delay": timedelta(minutes=1),
"start_date":datetime(2024,11,29)
},
    catchup=False
                        ) as f:
    fetch_data_task = PythonOperator(
        task_id="fetch_data_task",
        python_callable=fetch_data_task,
        provide_context=True,
    )
    store_data_task = PythonOperator(
        task_id="store_data_task",
        python_callable=store_data_task,
        provide_context=True
    )
fetch_data_task >> store_data_task