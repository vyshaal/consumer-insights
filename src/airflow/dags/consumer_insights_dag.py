# airflow related
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
import datetime as dt
from datetime import timedelta, datetime

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 18),
    'email': 'vyshaalnarayanam@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
  dag_id='consumer_insights_dag',
  description='Consumer Insights DAG',
  default_args=default_args,
  schedule_interval="0,10,20 22 18 2 *")


year = 1998


def get_year():
    global year
    # os.system("spark-submit ~/consumer-insights/src/spark/batch.py "+str(year))
    year += 1
    return str(year)


start_spark = BashOperator(task_id='start_spark', bash_command="echo 'Hey, I have started the spark job'", dag=dag)

run_spark = BashOperator(task_id='run_spark',
                         bash_command='spark-submit ~/consumer-insights/src/spark/batch.py '+get_year(), dag=dag)

stop_spark = BashOperator(task_id='stop_spark', bash_command="echo 'Finished writing data to Elasticsearch'", dag=dag)

start_spark >> run_spark >> stop_spark
