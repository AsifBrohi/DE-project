
import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_itr = pd.read_csv(csv_name, iterator=True,chunksize= 100000)
    df = next(df_itr)
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine,if_exists='append')
    print("inserted heading columns")


    while True:
        try:
            t_start = time()
            df = next(df_itr)
            # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            t_end = time()
            print(('inserted another chunk, took %.3f second' % (t_end - t_start)))
        except StopIteration as stop:
            print("completed")
            print(stop)
            break
    print("all data is now inserted")

if __name__=='__main__':


    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url for csv')

    args = parser.parse_args()
    main(args)
