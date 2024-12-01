import requests, os, json
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO

file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# modeled as extraction jobs

def pull_user_data():
    user_data = requests.get('http://host.docker.internal:8000/users')
    return json.dumps(user_data.json())

def pull_product_data():
    product_data = requests.get('http://host.docker.internal:8000/products')
    return json.dumps(product_data.json())

def pull_transaction_data():
    transaction_data = requests.get('http://host.docker.internal:8000/transactions')
    return json.dumps(transaction_data.json())

def store_ecommerce_data(ecommerce_data, type):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('://')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    bucket_name = 'ecommerce-data'

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    ecommerce_data = json.loads(ecommerce_data)
    data = json.dumps(ecommerce_data, ensure_ascii=False).encode('utf-8')

    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{type}.json',
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{type}.json'

def store_vrg_response(vrg_response):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('://')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    bucket_name = 'smash-conflation-data'

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    data = json.dumps(vrg_response, ensure_ascii=False).encode('utf-8')

    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'repaired.json',
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/repaired.json'


sample_output_file_path = '/Users/prajapat21/dev-projects/udemy_airflow/dags/data/output/sample_vrg_response.json'

def read_input_file_task(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    return data

json_data = read_input_file_task(sample_output_file_path)
columns = json_data.keys()
for column in columns:
    print(column)
print(",".join(columns))

endpoint = "http://host.docker.internal:9000"
print(endpoint.split("://")[1])
print(endpoint.split("://")[0])
