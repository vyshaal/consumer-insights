# airflow related
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
import datetime as dt
from datetime import timedelta, datetime


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
  schedule_interval="0 0-16 19 2 *"
)

run_spark = BashOperator(task_id='run_spark',
                         bash_command='spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.10:6.6.0 '
                                      '~/consumer-insights/src/spark/spark_job.py', dag=dag)

notify = BashOperator(task_id='notify', bash_command="echo 'Hey, Spark job is finished successfully'", dag=dag)

run_spark >> notify
