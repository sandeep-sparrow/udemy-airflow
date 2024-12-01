import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier

default_args = {
    'owner': 'airflow',
}

SYMBOL = 'AAPL'

# TaskFlow API
@dag(
    dag_id='stock_market',
    default_args=default_args,
    description='Stock Market DAG',
    start_date=datetime(2024, 11, 15),
    schedule='@daily',
    catchup=False,
    tags=['stock_market', 'udemy', 'prajapat21'],
    max_active_runs=1,
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has successfully been executed.',
        channel='general'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has failed.',
        channel='general'
    )
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_price = PythonOperator(
        task_id='get_stock_price',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_price") }}'}
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format-prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}',
        },
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path=f's3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids="get_formatted_csv") }}}}', conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public',
            )
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host,
        }
    )

    is_api_available() >> get_stock_price >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market()