from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mohith',
    'start_date': datetime(2024, 6, 15),
    'catchup': False
}

dag = DAG(
    'test_dag',
    default_args = default_args,
    schedule=timedelta(days=1)
)

t1 = BashOperator(
    task_id='task1',
    bash_command='echo "Hello from Task 1!"',
    dag=dag
)

t2 = BashOperator(
    task_id='task2',
    bash_command='echo "Hello from Task 2!"',
    dag=dag
)

t1 >> t2