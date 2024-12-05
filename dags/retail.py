from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2024, 11, 1),
    schedule='@weekly',
    tags=['retail'],
)
def retail():

    @task
    def start():
        print('Hi')

    start()

retail()