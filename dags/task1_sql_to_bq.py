import os
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow import configuration
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'mauludinr',
    'start_date': datetime(2021,11,26),
    'retries' : 1,
}

BUCKET_NAME = Variable.get('BUCKET_NAME')
PY_FILE = '/home/mauludinr/financial.py'
PROJECT_ID = Variable.get('PROJECT_ID')
GCS_TEMP_LOCATION = Variable.get('GCS_TEMP_LOCATION')
GCS_STG_LOCATION = Variable.get('GCS_STG_LOCATION')
DATASET_ID = Variable.get('DATASET_ID')
SETUP_FILE = '/home/mauludinr/setup.py'

dag= DAG(
    dag_id = "dag_sql_to_bq",
    default_args=default_args,
    catchup=False,
    #I want to start this dag at 08:00 PM UTC at airflow timezone
    schedule_interval='0 20 * * *',
    tags=['financial','loan','relational_fit']
)

pipeline_options = {'tempLocation': GCS_TEMP_LOCATION,
                    'stagingLocation': GCS_STG_LOCATION,
                    'dataset_id' : DATASET_ID,
                    'setup_file' : SETUP_FILE,
}
with dag:

    dataflow_task = BeamRunPythonPipelineOperator(
        task_id='job_sql_to_bq',
        runner='DataflowRunner',
        gcp_conn_id='google_cloud_default',
        py_file=PY_FILE,
        py_requirements=['apache-beam[gcp]==2.34.0','beam-mysql-connector==1.8.5'],
        py_system_site_packages=True,
        py_interpreter='python3',
        pipeline_options=pipeline_options,
        dataflow_config=DataflowConfiguration(
            job_name="sql_to_bq{{ ds_nodash }}",
            project_id=PROJECT_ID,
            location="asia-southeast1",
            wait_until_finished=True
        )
    )

    dataflow_task
