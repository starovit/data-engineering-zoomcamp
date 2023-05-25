#!/usr/bin/env python
# coding: utf-8

from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"
    
    
    file_name = csv_name+".gz"
    os.system(f"wget {url} -O {file_name}")
    os.system(f"gunzip {file_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)


    while True:

        t_start = time()

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(con=engine, name=table_name, if_exists="append")

        t_end = time()
        print("Inserted another chunk: %.3f seconds" % (t_end - t_start))
    
    pass
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Insert CSV data to Postgres.')
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of database table where we will \
                        write result to")
    parser.add_argument("--url", help="url of the .csv file")
    args = parser.parse_args()

    main(args)










