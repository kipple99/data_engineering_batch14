from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import yfinance as yf
import pandas as pd
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_historical_prices(symbol):
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

    return records


def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date,
    "open" float,
    high float,
    low float,
    close float,
    volume bigint,
    created_date timestamp default GETDATE()
);""")


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, False)
        
        for r in records:
            # ROW_NUMBER()를 사용하여 Primary Key가 동일한 레코드 처리
            sql = f"""
            INSERT INTO {schema}.{table}
            SELECT '{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]}, GETDATE()
            FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) as rn
                FROM {schema}.{table}
                WHERE date = '{r[0]}'
            ) subquery
            WHERE subquery.rn = 1;
            """
            print(sql)
            cur.execute(sql)

        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;") 
        raise
    logging.info("load done")


with DAG(
    dag_id='UpdateSymbol_v3',
    start_date=datetime(2023, 5, 30),
    catchup=False,
    tags=['API'],
    schedule='0 10 * * *'
) as dag:
    results = get_historical_prices("AAPL")
    load("sungwoodat99", "stock_info_v3", results)

