from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello from Task 1!")

def print_done():
    print("Task 2 completed!")

with DAG(
    dag_id='test_dag',
    start_date=datetime(2025, 8, 30),
    schedule_interval=None,
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    task2 = PythonOperator(
        task_id='print_done',
        python_callable=print_done,
    )

    task1 >> task2