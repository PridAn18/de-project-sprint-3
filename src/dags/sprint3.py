import time
import requests
import json
import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

t_log = logging.getLogger("airflow.task")

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

POSTGRES_CONN_ID = 'postgresql_de'

NICKNAME = 'a.navaro'
COHORT = '29'

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460',
    'Content-Type': 'application/x-www-form-urlencoded'
}



def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=HEADERS)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    t_log.info(f'Response is {response.content}')
    


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=HEADERS)
        response.raise_for_status()
        t_log.info(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError('Request get_report failed')

    ti.xcom_push(key='report_id', value=report_id)
    t_log.info(f'Report_id={report_id}')
    


def get_increment(date, ti):
    t_log.info('Making request get_increment')
    
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=HEADERS)
    response.raise_for_status()
    t_log.info(f'Response is {response.content}')
    

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    
    ti.xcom_push(key='increment_id', value=increment_id)
    t_log.info(f'increment_id={increment_id}')
    


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{increment_id}/{filename}'
    t_log.info(f's3_filename={s3_filename}')
    local_filename = date.replace('-', '') + '_' + filename
    t_log.info(f'local_filename={local_filename}')
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    t_log.info(f'Response is {response.content}')

    df = pd.read_csv(local_filename,index_col=0)
    
    if 'uniq_id' in df.columns:
        df=df.drop_duplicates(subset=['uniq_id'])
    
    if filename == 'user_order_log_inc.csv':
        if 'status' not in df.columns:
            df['status'] = 'shipped'
 
    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    t_log.info(f'{row_count} rows was inserted')
    


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5
}

business_dt = '{{ ds }}'

with DAG(
        'sales_mart',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

   

    dimension_tasks = []    
    for i in ['d_city', 'd_item', 'd_customer']:
        dimension_tasks.append(PostgresOperator(
            task_id = f'load_{i}',
            postgres_conn_id = POSTGRES_CONN_ID,
            sql = f'sql/mart.{i}.sql')
        )
    
    fact_tasks = []    
    for i in ['f_sales', 'f_customer_retention']:
        fact_tasks.append(PostgresOperator(
            task_id = f'load_{i}',
            postgres_conn_id = POSTGRES_CONN_ID,
            sql = f'sql/mart.{i}.sql',
            parameters={"date": {business_dt}})
        )

    upload_to_staging = []    
    for i in ['user_activity_log', 'user_order_log', 'customer_research']:
        upload_to_staging.append(PythonOperator(
            task_id = f'upload_{i}_inc',
            python_callable = upload_data_to_staging,
            op_kwargs={'date': business_dt,
                   'filename': f'{i}_inc.csv',
                   'pg_table': f'{i}',
                   'pg_schema': 'staging'})
        )
    
    

    (
            generate_report >> get_report >> get_increment
            >> upload_to_staging[0] >> upload_to_staging[1] >> upload_to_staging[2]
            >> dimension_tasks[0] >> dimension_tasks[1] >> dimension_tasks[2]
            >> fact_tasks[0] >> fact_tasks[1]            
    )
