from config.variables import AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MY_BUCKET
import boto3
import findspark
findspark.init()
from airflow import DAG
from pyspark.sql import SparkSession, SQLContext, Row
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


spark = SparkSession.builder.appName("consumer-insights").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY_ID)
hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

df = sqlContext.read.parquet("s3://amazon-customer-reviews-dataset/parquet/product_category=Camera/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet")

s3 = boto3.resource('s3', region_name='us-east-1')
s3_path = "s3n://{0}:{1}@amazon-customer-reviews-dataset/parquet/product_category=Camera/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet'.format(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"

# parquetFile = spark.read.parquet(s3_path)
parquetFile = spark.read.parquet('../input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

parquetFile.printSchema()
parquetFile.createOrReplaceTempView("products")
print('Done')
