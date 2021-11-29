import os
from airflow import DAG
from airflow import configuration
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'mauludinr',
    'start_date': datetime(2021,11,26),
    'retries' : 1,
}

dag= DAG(
    dag_id = "dag_bq_to_bq",
    default_args=default_args,
    catchup=False,
    #Because i want to start this at 08:00 PM UTC at airflow timezone, i need to set the schedule to 01:00 PM based on my local timezone in WIB(+07:00) 
    schedule_interval='0 13 * * *',
    tags=['ecommerce']
)

with dag:

    task_bash= BashOperator(
    	task_id='bq_to_bq',
    	bash_command='sh /home/mauludinr/run_bq.sh ',
     )
		
     task_bash
