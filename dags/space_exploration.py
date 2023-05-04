import pandas as pd

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from bots.EmailHelper import send

def _generate_platzi_dataset(**kwargs):

    data = pd.DataFrame(
        {"student_name": ["Enzo", "Gisela", "Janis", "Rodolfo"],
        "cutoff_date": [kwargs["ds"], kwargs["ds"], kwargs["ds"], kwargs["ds"]],
        "audtiminsert_date": [kwargs["ts"], kwargs["ts"], kwargs["ts"], kwargs["ts"]]}
    )

    data.to_csv(f"/tmp/platzi_data{kwargs['ds_nodash']}.csv", header=True, index=False)


#def _choose(**context):
#
#    if context["logical_date"].date() < datetime(2023, 4, 16):
#
#        return "notify_marketing_analysts"
#    
#    return "notify_risk_analysts"


dag_owner = 'ecardenas1123'

default_args = {'owner': dag_owner,
        'start_date': datetime(2023, 4, 1),
        'max_active_runs': 3,
        'end_date': datetime(2023, 4, 30),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1
        }

with DAG(dag_id='space_exploration',
        default_args=default_args,
        description='Final Project',
        schedule_interval='@daily') as dag:

    task_1 = BashOperator(task_id = "nasa_confirmation_response",
                    bash_command="sleep 20 && echo 'Confirmation from NASA, you can proceed.' > /tmp/success_response_{{ ds_nodash }}.txt")
    

    task_2 = FileSensor(task_id = "waiting_nasa_confirmation_response_file",
                        filepath = "/tmp/success_response_{{ ds_nodash }}.txt")
    
    task_3 = BashOperator(task_id = "obtain_spacex_data",
                    bash_command = "curl https://api.spacexdata.com/v4/launches/past > /tmp/spacex_{{ds_nodash}}.json")

    task_4 = PythonOperator(task_id="platzi_satellite_response",
                    python_callable=_generate_platzi_dataset)
    
#    task_5 = BranchPythonOperator(task_id = "branch",
#                                  python_callable = _choose)
#    
#    task_6 = EmailOperator(task_id='notify_marketing_analysts',
#                    to = 'email@gmail.com',
#                    subject = "[GMM] Notification Satellite Data",
#                    html_content = "Notice to analysts, the data is available")
#    
#    task_7 = EmailOperator(task_id='notify_risk_analysts',
#                    to = 'email@gmail.com',
#                    subject = "[GRM] Notification Satellite Data",
#                    html_content = "Notice to analysts, the data is available") 

    task_5 = PythonOperator(task_id="send_email",
                    python_callable=send)

    task_1 >> task_2 >> task_3 >> task_4 >> task_5 # >> [task_6, task_7]

