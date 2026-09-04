import requests
import pandas as pd
import sqlite3
import boto3
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date, datetime


def extract_data(coin_id, vs_currency, days):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
    "vs_currency" : vs_currency,
    "days" : days
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data


def transform_data(ti, coin_id):
    raw_data = ti.xcom_pull(task_ids='extract_data_task')
    prices = raw_data["prices"]
    df = pd.DataFrame(prices, columns = ["timestamp", "price"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["coin_id"] = coin_id
    df = df.rename(columns={"timestamp":"date"})
    return df

def load_to_sqlite(ti, db_name):
    df = ti.xcom_pull(task_ids='transform_data_task')
    conn = sqlite3.connect(db_name)
    df.to_sql("daily_prices", conn, if_exists = "append", index = False)
    query = """
    DELETE FROM daily_prices
    WHERE rowid NOT IN(
        SELECT MIN(rowid)
        From daily_prices
        GROUP BY date, coin_id
    )
    """
    conn.execute(query)
    conn.commit()
    conn.close()

def load_to_s3(ti, bucket_name):
    df = ti.xcom_pull(task_ids='transform_data_task')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3', region_name='ap-southeast-2')
    today = date.today()
    coin_id = df["coin_id"].iloc[0]
    file_key = f"crypto-data/{coin_id}_{today}.csv"
    
    s3.put_object(
    Bucket=bucket_name,
    Key=file_key,
    Body=csv_buffer.getvalue()
    )

default_args = {
    'owner': 'jenesis',
    'start_date': datetime(2026, 8, 20),
}

dag = DAG(
    'coin_prices_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    op_kwargs={'coin_id': 'bitcoin', "vs_currency" : "usd", "days" : 100},
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    op_kwargs={'coin_id': 'bitcoin'},
    dag=dag
)

load_sqlite_task = PythonOperator(
    task_id='load_to_sqlite_task',
    python_callable=load_to_sqlite,
    op_kwargs={'db_name': 'crypto_data.db'},
    dag=dag
)

load_s3_task = PythonOperator(
    task_id='load_to_s3_task',
    python_callable=load_to_s3,
    op_kwargs={'bucket_name': 'my-etl-data-jenesis-2026'},
    dag=dag
)

extract_task >> transform_task >> load_sqlite_task >> load_s3_task