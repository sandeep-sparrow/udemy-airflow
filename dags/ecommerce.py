from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.utils.task_group import TaskGroup
import requests, os, sys

# Add the parent directory of utils to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.app_utils import *

default_args = {
    'owner': 'prajapat21',
}

@dag(
    default_args=default_args,
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'http', 'minio']
)
def ecommerce():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_server_available() -> PokeReturnValue:
        url = 'http://host.docker.internal:8000/'
        response = requests.get(url)
        response_data = response.json()
        condition = response_data.get('message') == "Hello, Alpine Docker FastAPI!"
        return PokeReturnValue(is_done=condition, xcom_value=url)

    with TaskGroup("extract_data") as extract_data:
        extract_user_data = PythonOperator(
            task_id="extract_user_data",
            python_callable=pull_user_data,
        )

        extract_product_data = PythonOperator(
            task_id="extract_product_data",
            python_callable=pull_product_data,
        )

        extract_transaction_data = PythonOperator(
            task_id="extract_transaction_data",
            python_callable=pull_transaction_data,
        )

    with TaskGroup("store_data") as store_data:
        store_user_data = PythonOperator(
            task_id="store_user_data",
            python_callable=store_ecommerce_data,
            op_kwargs={
                'ecommerce_data': '{{ task_instance.xcom_pull(task_ids="extract_data.extract_user_data") }}',
                'type': 'users'
            }
        )

        store_product_data = PythonOperator(
            task_id="store_product_data",
            python_callable=store_ecommerce_data,
            op_kwargs={
                'ecommerce_data': '{{ task_instance.xcom_pull(task_ids="extract_data.extract_product_data") }}',
                'type': 'products'
            }
        )

        store_transaction_data = PythonOperator(
            task_id="store_transaction_data",
            python_callable=store_ecommerce_data,
            op_kwargs={
                'ecommerce_data': '{{ task_instance.xcom_pull(task_ids="extract_data.extract_transaction_data") }}',
                'type': 'transactions'
            }
        )

    is_server_available() >> extract_data >> store_data

ecommerce()