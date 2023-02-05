import os
import argparse
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(params):

    url = params.url

    #parquet_name = 'yellow_tripdata_2021-01.parquet'
    parquet_name = 'output.parquet'

    # download the csv
    os.system(f"wget {url} -O {parquet_name}")

    df = pd.read_parquet(parquet_name)

    #print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))
    #print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passanger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passanger count: {df['passenger_count'].isin([0]).sum()}")

    return df

@task(log_prints=True, retries=3)
def ingest_data(params, df):

    table_name = params.table_name

    database_block=SqlAlchemyConnector.load("ny-taxi-connection")
    with database_block.get_connection(begin=False) as engine:
    #engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}') # no need more because the block was created
    #engine.connect()# no need more because the block was created
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(params):
    print(f"Logging Subflow for: {params.table_name}")

@flow(name="Ingest Flow")
def main_flow():

    parser = argparse.ArgumentParser(description='Ingestion parquet to postgreSQL')

    parser.add_argument('--user', help='postgresql username')
    parser.add_argument('--password', help='postgresql password')
    parser.add_argument('--host', help='postgresql host')
    parser.add_argument('--port', help='postgresql port')
    parser.add_argument('--db', help='postgresql db name')
    parser.add_argument('--table_name', help='postgresql table name')
    parser.add_argument('--url', help='csv url')

    args = parser.parse_args()

    log_subflow(args)
    raw_data = extract_data(args)
    data = transform_data(raw_data)
    ingest_data(args, data)

if __name__ == '__main__':
    main_flow()