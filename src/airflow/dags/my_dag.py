# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
import datetime as dt
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '*/1 * * * *',
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=5),
}

dag = DAG(
  dag_id='my_dag', 
  description='Simple tutorial DAG',
  default_args=default_args)


#spark_job = BashOperator(task_id='spark_job',
#                         bash_command='spark-submit --jars jars/elasticsearch-spark-20_2.11-6.6.0.jar batch.py',
#                         dag=dag)

spark_job = BashOperator(task_id='spark_job', bash_command="echo 'Hey, I have started sparkjob'", dag=dag)

elastic_updated = BashOperator(task_id='elastic_updated',
                               bash_command="echo 'Hey, I have indexed new reviews to the elasticsearch'",
                               dag=dag)

spark_job >> elastic_updated
