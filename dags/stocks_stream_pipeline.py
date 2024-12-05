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
def store_data_task(**kwargs):
    records = kwargs.get('ti').xcom_pull(key='records')
    peers = kwargs.get('ti').xcom_pull(key='peers')
    recommended_trends = kwargs.get('ti').xcom_pull(key='recommendation_trends')
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
            for item in recommended_trends:
                for record in item:
                    pg_hook.run("""
                        INSERT INTO recommendation_trends (
                            symbol,
                            buy,
                            hold,
                            sell,
                            strong_buy,
                            strong_sell,
                            period
                        ) VALUES (
                            %(symbol)s,
                            %(buy)s,
                            %(hold)s,
                            %(sell)s,
                            %(strong_buy)s,
                            %(strong_sell)s,
                            %(period)s
                        )
                        ON CONFLICT (symbol, period) DO UPDATE SET
                            buy = EXCLUDED.buy,
                            hold = EXCLUDED.hold,
                            sell = EXCLUDED.sell,
                            strong_buy = EXCLUDED.strong_buy,
                            strong_sell = EXCLUDED.strong_sell;
                    """, parameters=record)
            for peer in peers:
                pg_hook.run("""
                    INSERT INTO peers (
                        symbol,
                        sector,
                        industry,
                        subIndustry
                    ) VALUES (
                        %(symbol)s,
                        %(sector)s,
                        %(industry)s,
                        %(subIndustry)s
                    )
                    ON CONFLICT (symbol) DO UPDATE SET
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        subIndustry = EXCLUDED.subIndustry;
                """, parameters=peer)
            print("Data inserted successfully.")
        except NotNullViolation as e:
            print('Record contains Null levels, Skipped.')
def fetch_data_task(**kwargs):
    print('Fetching Data')
    variables = Variable.get('variables', deserialize_json=True)
    url = variables[0]['url']
    token = variables[0]['api_key']
    # Fetch Recommendations
    records = []
    for i in range(len(variables[0]['companies'][0:3])):
        recommendation_trends = variables[0]['recommendation_trends'].replace('{SYMBOL}', variables[0]['companies'][i]['symbol'])
        try:
            res = requests.get(f'{url}{recommendation_trends}{token}')
            res_raw = res.json()
            if res.status_code == 429:
                print(f'API Limit Reached')
                time.sleep(10)
            elif res.status_code == 200:
                record = []
                for elm in res_raw:
                    item = {
                        'symbol': variables[0]['companies'][i]['symbol'],
                        'buy': elm['buy'],
                        'sell': elm['sell'],
                        'hold': elm['hold'],
                        'strong_buy': elm['strongBuy'],
                        'strong_sell': elm['strongSell'],
                        'period': elm['period']
                    }
                    record.append(item)
                if not record:
                    print('Record is Empty')
                else:
                    print(record)
                    records.append(record)
            else:
                print(f"Unexpected status code: {res.status_code}")
                return None
        except Exception as e:
            print(e)
            continue
    kwargs['ti'].xcom_push(key='recommendation_trends', value=records)

    # Fetch Peers
    records = []
    for i in range(len(variables[0]['companies'][0:3])):
        groups = []
        for item in ['sector', 'industry', 'subIndustry']:
            peers = variables[0]['peers'].replace('{SYMBOL}', variables[0]['companies'][i]['symbol'])
            peers = peers.replace('{GROUPING}', item)
            try:
                res = requests.get(f'{url}{peers}{token}')
                res_raw = res.json()
                if res.status_code == 429:
                    print(f'API Limit Reached')
                    time.sleep(10)
                elif res.status_code == 200:
                    groups.append(res_raw)
                else:
                    print(f"Unexpected status code: {res.status_code}")
                    return None
                try:
                    record = {
                        'symbol': variables[0]['companies'][i]['symbol'],
                        'sector': groups[0],
                        'industry': groups[1],
                        'subIndustry': groups[2]
                    }
                    print(record)
                    records.append(record)
                except IndexError as e:
                    print('Empty List, Symbol has no data')
            except Exception as e:
                print(e)
                continue
    kwargs['ti'].xcom_push(key='peers', value=records)

    # Fetch Quotes
    records = []
    for i in range(len(variables[0]['companies'][0:3])):
        quote = variables[0]['quote'].replace('{SYMBOL}', variables[0]['companies'][i]['symbol'])
        try:
            res = requests.get(f'{url}{quote}{token}')
            res_raw = res.json()
            if res.status_code == 429:
                print(f'API Limit Reached')
                time.sleep(10)
            elif res.status_code == 200:
                record = {
                    'id': f"{variables[0]['companies'][i]['symbol']}_{res_raw['t']}",
                    'symbol': variables[0]['companies'][i]['symbol'],
                    'name': variables[0]['companies'][i]['name'],
                    'current_price': res_raw['c'],
                    'change': res_raw['d'],
                    'percent_change': res_raw['dp'],
                    'high_price_of_the_day': res_raw['h'],
                    'low_price_of_the_day': res_raw['l'],
                    'open_price_of_the_day': res_raw['o'],
                    'previous_close_price': res_raw['pc'],
                    'timestamp': res_raw['t']
                }
                print(record)
                records.append(record)
            else:
                print(f"Unexpected status code: {res.status_code}")
                return None
        except Exception as e:
            print(e)
            continue
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
    fetch_data_task =PythonOperator(
task_id="fetch_data_task",
python_callable=fetch_data_task,
provide_context=True,
    )
    check_market_status_task = PythonOperator(
        task_id="check_market_status_task",
        python_callable=check_market_status_task,
        provide_context=True
    )
    store_data_task = PythonOperator(
        task_id="store_data_task",
        python_callable=store_data_task,
        provide_context=True
    )
    create_tables_task = PostgresOperator(
        task_id="create_postgres_tables",
        postgres_conn_id="postgres_local",
        sql=
        '''
            CREATE TABLE IF NOT EXISTS stocks_data (
                id VARCHAR(25) PRIMARY KEY,
                symbol VARCHAR(10),
                name VARCHAR(255),
                current_price DECIMAL(10, 2),
                change DECIMAL(10, 2),
                percent_change DECIMAL(5, 2),
                high_price_of_the_day DECIMAL(10, 2),
                low_price_of_the_day DECIMAL(10, 2),
                open_price_of_the_day DECIMAL(10, 2),
                previous_close_price DECIMAL(10, 2),
                timestamp BIGINT
            );
            
            CREATE TABLE IF NOT EXISTS recommendation_trends (
                symbol VARCHAR(10),
                buy INT,
                hold INT,
                sell INT,
                strong_buy INT,
                strong_sell INT,
                period DATE,
                primary key (symbol, period)
            );
            
            CREATE TABLE IF NOT EXISTS peers(
                symbol VARCHAR(10) PRIMARY KEY,
                sector TEXT[][],
                industry TEXT[][],
                subIndustry TEXT[][]
            )
        '''
)


create_tables_task >> check_market_status_task >> fetch_data_task >> store_data_task