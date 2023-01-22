import os

import pandas as pd
from sqlalchemy import create_engine

import argparse


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.pport
    db = params.db
    table_name = params.table_name
    url = params.url

    #parquet_name = 'yellow_tripdata_2021-01.parquet'
    parquet_name = 'output.parquet'

    # download the csv
    os.system(f"wget {url} -O {parquet_name}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_parquet(parquet_name)

    #print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))
    #print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingestion parquet to postgreSQL')


    parser.add_argument('--user', help='postgresql username')
    parser.add_argument('--password', help='postgresql password')
    parser.add_argument('--host', help='postgresql host')
    parser.add_argument('--pport', help='postgresql port')
    parser.add_argument('--db', help='postgresql db name')
    parser.add_argument('--table_name', help='postgresql table name')
    parser.add_argument('--url', help='csv url')

    args = parser.parse_args()

    main(args)
