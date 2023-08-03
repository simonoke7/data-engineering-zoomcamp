import argparse
import dask.dataframe as dd
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system(f"wget {url} -O {parquet_name}")

    uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(uri)

    # load parquet file as dataframe
    df = pd.read_parquet(parquet_name)

    # create dask dataframe with desired chunk size
    ddf = dd.from_pandas(df, chunksize=100000)

    ddf.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    t_start = time()
    ddf.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    print('data transfer took %.3f second' % (t_end - t_start))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='destination table of results')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)