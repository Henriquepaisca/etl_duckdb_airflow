from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def helloWorld():
    print("Hello World")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025,4,16),
    schedule_interval="* * * * *",
    catchup=False) as dag:

    task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld
        )