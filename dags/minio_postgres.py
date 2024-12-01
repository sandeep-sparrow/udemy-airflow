import json

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from minio import Minio
from airflow.decorators import dag
from airflow.hooks.base import BaseHook

BUCKET_NAME = 'ecommerce-data'

default_args = {
    'owner': 'prajapat21',
}

def _ecommerce_data(bucked_name, file_key):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('://')[1],
        # host.docker.internal:9000
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    response = client.get_object(bucked_name, file_key)
    json_data = response.read().decode('utf-8')
    response.close()
    response.release_conn()

    return json.loads(json_data)

def generate_postgres_schema(table_name, json_data):
    columns = json_data[0].keys()
    column_types = []

    for col in columns:
        value = json_data[0][col]
        if isinstance(value, int):
            column_types.append(f"{col} INT")
        elif isinstance(value, float):
            column_types.append(f"{col} FLOAT")
        elif isinstance(value, bool):
            column_types.append(f"{col} BOOLEAN")
        else:
            column_types.append(f"{col} TEXT")

    column_definitions = ", ".join(column_types)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});"
    return create_table_query

def generate_query_from_minio(**kwargs):
    # MinIO Configuration
    bucket_name = BUCKET_NAME
    file_key = 'users.json'

    # Fetch JSON and generate schema
    json_data = _ecommerce_data(bucket_name, file_key)
    create_query = generate_postgres_schema('Users', json_data)

    # Push the query to XCom
    return create_query

def generate_insert_queries(table_name, json_data):
    # Extract columns and rows
    columns = json_data[0].keys()
    column_list = ', '.join(columns)

    # Generate row data
    values = []
    for row in json_data:
        row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()])
        values.append(f"({row_values})")

    # Combine all rows into one INSERT query
    values_string = ', '.join(values)
    insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES {values_string};"

    return insert_query

def generate_insert_query_from_minio(**kwargs):
    # MinIO Configuration
    bucket_name = BUCKET_NAME
    file_key = 'users.json'

    # Fetch JSON data
    json_data = _ecommerce_data(bucket_name, file_key)

    # Generate INSERT query
    insert_query = generate_insert_queries('Users', json_data)

    # Push the query to XCom
    return insert_query

@dag(
    dag_id='minio_postgres',
    default_args=default_args,
    description='Minio Postgres DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['minio', 'postgres'],
    max_active_runs = 1
)
def minio_postgres():

    # Generate CREATE TABLE query dynamically
    generate_query = PythonOperator(
        task_id='generate_create_table_query',
        python_callable=generate_query_from_minio,
        provide_context=True
    )

    # Use PostgresOperator to create the table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="{{ ti.xcom_pull(task_ids='generate_create_table_query') }}"
    )

    # Generate INSERT INTO query dynamically
    generate_insert_query = PythonOperator(
        task_id='generate_insert_query',
        python_callable=generate_insert_query_from_minio,
        provide_context=True
    )

    # Use PostgresOperator to insert data into the table
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres',
        sql="{{ ti.xcom_pull(task_ids='generate_insert_query') }}"
    )

    # Task dependencies
    generate_query >> create_table >> generate_insert_query >> insert_data

minio_postgres()
