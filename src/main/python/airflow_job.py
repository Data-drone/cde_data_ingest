from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

 
default_args = {

    'owner': 'brianlaw',
    'depends_on_past': False,
    'wait_for_downstream': True,
    'start_date': datetime(2021,5,1,0),
    'job_name': 'process_data'
}

cde_process_dag = DAG(
    'cde_process',
    default_args=default_args,
    catchup=False,
    schedule_interval="*/7 * * * *",
    is_paused_upon_creation=False
) 

start = DummyOperator(task_id='start', dag=cde_process_dag)

load_data_job = CDEJobRunOperator(
    task_id='loader',
    dag=cde_process_dag,
    job_name='load_data_job'
)

etl_job = CDEJobRunOperator(
    task_id='transform_n_report',
    dag=cde_process_dag,
    job_name='etl_job'
)
 
end = DummyOperator(task_id='end', dag=cde_process_dag)

# setting the dependency chain
start >> load_data_job >> etl_job >> end

