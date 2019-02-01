from config.variables import AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MY_BUCKET
import boto3
from pyspark.sql import SparkSession
from elasticsearch5 import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

spark = SparkSession.builder.appName("consumer-insights").getOrCreate()

s3_resource = boto3.resource('s3', region_name='us-east-1')
s3_connection = s3_resource.meta.client
bucket = s3_connection.get_bucket(MY_BUCKET)

parquetFile = spark.read.parquet('../input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

parquetFile.printSchema()
parquetFile.createOrReplaceTempView("products")
print('Done')
