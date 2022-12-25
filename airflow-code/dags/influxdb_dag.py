from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator

from influxdb import DataFrameClient
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import pandas_gbq

dag = DAG(
    'influxdb_query_operator',
    start_date=datetime(2021, 1, 1),
    tags=['influxdb_query'],
    catchup=False,
    schedule_interval='10 * * * *'
    )

def cpu0():
    bucket = "telegraf"
    org = "ISTDSA"
    token = "Q1lTaftYtvjsjOKwjm6WHpVPp3AGY2HFO5dMjOwqz7ho3wgTiJ6TXVsp5sLsFL0M4EfCKZUciTQR3qJn9gNrMQ=="
    # Store the URL of your InfluxDB instance
    url="http://35.230.119.4:8086"

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    query_api = client.query_api()

    query = 'from(bucket: "telegraf")\
         |> range(start: -5m, stop: now())\
         |> filter(fn: (r) => r["_measurement"] == "cpu")\
         |> filter(fn: (r) => r["_field"] == "usage_system")\
         |> filter(fn: (r) => r["cpu"] == "cpu0")\
         |> filter(fn: (r) => r["host"] == "influxdb-test")\
         |> map(fn: (r) => ({ r with _value: r._value*100.0 }))\
         |> aggregateWindow(every: 1m, fn: last, createEmpty: false)'

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time(), record.get_field(), record.get_value()))

    print(results)


    df = client.query_api().query_data_frame(org=org, query=query)
    df = df.drop(['table', 'result'], axis=1)


    # TODO: Set project_id to your Google Cloud Platform project ID.
    project_id = "bigquery-public-data-296010"

    # TODO: Set table_id to the full destination table ID (including the
    #       dataset ID).
    table_id = "ISTDSA.results-cpu"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')



def cpu1():
    bucket = "telegraf"
    org = "ISTDSA"
    token = "Q1lTaftYtvjsjOKwjm6WHpVPp3AGY2HFO5dMjOwqz7ho3wgTiJ6TXVsp5sLsFL0M4EfCKZUciTQR3qJn9gNrMQ=="
    # Store the URL of your InfluxDB instance
    url="http://35.230.119.4:8086"

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    query_api = client.query_api()

    query = 'from(bucket: "telegraf")\
         |> range(start: -5m, stop: now())\
         |> filter(fn: (r) => r["_measurement"] == "cpu")\
         |> filter(fn: (r) => r["_field"] == "usage_system")\
         |> filter(fn: (r) => r["cpu"] == "cpu1")\
         |> filter(fn: (r) => r["host"] == "influxdb-test")\
         |> map(fn: (r) => ({ r with _value: r._value*100.0 }))\
         |> aggregateWindow(every: 1m, fn: last, createEmpty: false)'

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time(), record.get_field(), record.get_value()))

    print(results)

    df = client.query_api().query_data_frame(org=org, query=query)
    df = df.drop(['table', 'result'], axis=1)


    # TODO: Set project_id to your Google Cloud Platform project ID.
    project_id = "bigquery-public-data-296010"

    # TODO: Set table_id to the full destination table ID (including the
    #       dataset ID).
    table_id = "ISTDSA.results-cpu"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')



def disk():
    bucket = "telegraf"
    org = "ISTDSA"
    token = "Q1lTaftYtvjsjOKwjm6WHpVPp3AGY2HFO5dMjOwqz7ho3wgTiJ6TXVsp5sLsFL0M4EfCKZUciTQR3qJn9gNrMQ=="
    # Store the URL of your InfluxDB instance
    url="http://35.230.119.4:8086"

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    query_api = client.query_api()

    query = 'from(bucket: "telegraf")\
        |> range(start: -5m, stop: now())\
    	|> filter(fn: (r) => r["_measurement"] == "disk")\
    	|> filter(fn: (r) => r["_field"] == "total")\
    	|> filter(fn: (r) => r["device"] == "sda1")\
    	|> filter(fn: (r) => r["fstype"] == "ext4")\
    	|> filter(fn: (r) => r["host"] == "influxdb-test")\
    	|> filter(fn: (r) => r["mode"] == "rw")\
    	|> filter(fn: (r) => r["path"] == "/")\
    	|> aggregateWindow(every: 1m, fn: last, createEmpty: false)'


    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time(), record.get_field(), record.get_value()))

    print(results)

    df = client.query_api().query_data_frame(org=org, query=query)
    df = df.drop(['table', 'result'], axis=1)


    # TODO: Set project_id to your Google Cloud Platform project ID.
    project_id = "bigquery-public-data-296010"

    # TODO: Set table_id to the full destination table ID (including the
    #       dataset ID).
    table_id = "ISTDSA.results-disk"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')



def mem():
    bucket = "telegraf"
    org = "ISTDSA"
    token = "Q1lTaftYtvjsjOKwjm6WHpVPp3AGY2HFO5dMjOwqz7ho3wgTiJ6TXVsp5sLsFL0M4EfCKZUciTQR3qJn9gNrMQ=="
    # Store the URL of your InfluxDB instance
    url="http://35.230.119.4:8086"

    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    query_api = client.query_api()

    query = 'from(bucket: "telegraf")\
        |> range(start: -5m, stop: now())\
        |> filter(fn: (r) => r["_measurement"] == "mem")\
        |> filter(fn: (r) => r["_field"] == "total")\
        |> filter(fn: (r) => r["host"] == "influxdb-test")\
        |> aggregateWindow(every: 1m, fn: last, createEmpty: false)'


    client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
    )

    result = query_api.query(org=org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_time(), record.get_field(), record.get_value()))

    print(results)

    df = client.query_api().query_data_frame(org=org, query=query)
    df = df.drop(['table', 'result'], axis=1)


    # TODO: Set project_id to your Google Cloud Platform project ID.
    project_id = "bigquery-public-data-296010"

    # TODO: Set table_id to the full destination table ID (including the
    #       dataset ID).
    table_id = "ISTDSA.results-mem"

    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')



with dag:
    influxdb_cpu0_task = PythonOperator(
        task_id='influxdb_cpu0_task',
        python_callable = cpu0
    )

    influxdb_cpu1_task = PythonOperator(
        task_id='influxdb_cpu1_task',
        python_callable = cpu1
    )

    influxdb_disk_task = PythonOperator(
        task_id='influxdb_disk_task',
        python_callable = disk
    )

    influxdb_mem_task = PythonOperator(
        task_id='influxdb_mem_task',
        python_callable = mem
    )

    command = 'scripts/command.sh'

    bash_task = BashOperator(
         task_id='bash_task',
         bash_command=command
    )


influxdb_cpu0_task >> influxdb_cpu1_task >> [influxdb_disk_task, influxdb_mem_task] >> bash_task


