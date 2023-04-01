from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello():
    print("Hello gente de Platzi, mi nombre es Enzo y ya soy un Data Engineer")

with DAG(dag_id="pythonoperator",
        description="Nuestro primer utilizando Python Operator",
        start_date=datetime(2022, 8, 1),
        schedule_interval="@once") as dag:

    t1 = PythonOperator(task_id="Helloo_with_python",
                python_callable=print_hello)

    t1