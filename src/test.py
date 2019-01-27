import boto3
from elasticsearch5 import Elasticsearch
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import *
import json
import gzip
import pandas as pd

s3 = boto3.resource('s3', region_name='us-east-1')
bucket_name = 'amazon-customer-reviews-dataset'
bucket = s3.Bucket(bucket_name)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

sparkSession = SparkSession.builder.appName("consumer-insights").getOrCreate()
sqlContext = SQLContext(sparkSession)

parquetFile = sqlContext.read.parquet('/Users/vyshaalnarayanam/Insight/consumer-insights/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')
# parquetFile = sqlContext.read.parquet('s3://amazon-customer-reviews-dataset/parquet/product_category=Camera/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

parquetFile.printSchema()

subset = parquetFile.take(1000)

# es.indices.delete(index='consumer-insights')
# es.search(index='electronics', doc_type='review')
es.delete_by_query(index='electronics', doc_type='review', body={})

INDEX_NAME = 'electronics'
if INDEX_NAME not in es.indices.get_alias():
    res = es.indices.create(INDEX_NAME)
    print(res)

for row in subset:
    res = es.index(index='electronics', doc_type='review', body=row.asDict())
    print(res)

response = es.search(index='electronics', doc_type='review', body={"query": {"match": {"marketplace": "US"}}})

response = es.search(index='electronics', doc_type='review', body={"query": {"match": {"product_title": "Bluetooth"}}})

for hit in response['hits']['hits']:
    print(hit['_source'])

sparkSession.stop()
print('Done')
