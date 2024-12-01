import random

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'prajapat21',
}

@task
def generate_random_number(start: int, end: int) -> int:
    return random.randint(start, end)

@task
def check_odd_or_even_number(number: int) -> str:
    if number % 2 == 0:
        return "even"
    else:
        return "odd"

@dag(
    default_args=default_args,
    dag_id='assignment1',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'assignments'],
)
def assignment1():

    random_number = generate_random_number(1, 10)

    check_odd_or_even_number(random_number)

assignment1()