from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'prajapat21',
}

@dag(
    dag_id='taskflow',
    description='A simple taskflow',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval='@daily',
    tags=['taskflow', 'api'],
)
def taskflow():

    @task
    def _task_a():
        print("Task A")
        return 42

    @task
    def _task_b(value):
        print("Task B")
        print(value)

    _task_b(_task_a())

taskflow()