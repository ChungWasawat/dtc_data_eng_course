#!/usr/bin/env python
# coding: utf-8

import argparse
import os

import pandas as pd
from sqlalchemy import create_engine
from time import time

# data workflow ~data pipeline
# ^www (source~input) -> ^wget (.parquet~output 1) [using params i.e. url] -> 
# -> ^ingest (parquet~output 2) [using params i.e. password] -> ^postgres database (table in postgresql~output 3)
# can store data to several cloud providers ~GCP&AWS in parallel to back data up
# ^ = job / (output) = dependency

# additional things to keep in mind
# how frequent it will retrieve data every month? !the first parameter of this workflow is month (like global para for every job )
# if the pipeline is failed, how to response the incident? ~retry?
# correct order? logs? 

# tools ~luigi, airflow, prefect, etc


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    data_name = "output.parquet"


    # can add sleep to wait when it (wget) doesn't download successfully
    # downloads the parquet
    os.system(f"wget {url} -O {data_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df = pd.read_parquet(data_name)


    t_start = time()

    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    t_end = time()
    print("inserted data..., took %.3f second" %(t_end-t_start) )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres.')

    # user, password, host, port, database name, table name,
    # url of the data

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the data')

    args = parser.parse_args()

    main(args)







