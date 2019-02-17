# airflow related
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
import datetime as dt
from datetime import timedelta

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'vyshaalnarayanam@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=2),
}

dag = DAG(
  dag_id='consumer_insights_dag',
  description='Consumer Insights DAG',
  default_args=default_args,
  schedule_interval="5,10,15 21 * * *")


year = 1999


def batch_run():
    os.system("spark-submit ~/consumer-insights/src/spark/batch.py "+str(year))
    year += 1


start_spark = BashOperator(task_id='start_spark', bash_command="echo 'Hey, I have started the sparkjob'", dag=dag)

# run_spark = BashOperator(task_id='run_spark',
#                          bash_command='spark-submit ~/consumer-insights/src/spark/batch.py '+'2000', dag=dag)
run_spark = PythonOperator(task_id='run_spark', python_callable=batch_run, dag=dag)

stop_spark = BashOperator(task_id='stop_spark', bash_command="echo 'Finished writing data to elasticsearch'", dag=dag)

start_spark >> run_spark >> stop_spark
