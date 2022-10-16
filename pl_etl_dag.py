from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from etl_pl_extract import etl_pl_extract

defualt_args = {
    'owner':'Alberto',
    'depends_on_past':False,
    'start_date':datetime(2022,10,15),
    'email':['albertomarconi.a@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'etl_pl_dag',
    description='pulling premier league data',
    default_args=defualt_args,
    schedule_interval='@daily',
)

run_etl = PythonOperator(
    task_id='etl_pl',
    python_callable=etl_pl_extract,
    dag=dag,
)

run_etl