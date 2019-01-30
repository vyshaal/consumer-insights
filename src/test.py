import boto3
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import time
from collections import Counter
start_time = time.time()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

s3_resource = boto3.resource('s3', region_name='us-east-1')
bucket_name = 'amazon-customer-reviews-dataset'
my_bucket = s3_resource.Bucket(bucket_name)

spark = SparkSession.builder.appName("consumer-insights").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# parquetFile = spark.read.parquet("../input/*")
parquetFile = spark.read.parquet('../input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')
# parquetFile = sqlContext.read.parquet('s3://amazon-customer-reviews-dataset/parquet/product_category=Camera/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

parquetFile.printSchema()
parquetFile.createOrReplaceTempView("product_reviews")
# products = spark.sql("SELECT product_id, product_title, star_rating, count(review_id) as tally FROM product_reviews group by product_id, product_title, star_rating order by product_id, star_rating asc limit 10")
products = parquetFile.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"))

es.indices.delete(index='products', ignore=[404])
es.indices.delete(index='reviews', ignore=[404])

for row in products.collect():
    body = row.asDict()
    ratings_counter = Counter(body['ratings'])
    body.update({f'{k}-stars': v for k, v in ratings_counter.items()})
    body.update({"Total Reviews": len(body["ratings"]),
                 "Product Rating": round(sum(body["ratings"])/len(body["ratings"]), 2)})
    del body['ratings']
    res = es.index(index='products', doc_type='product', id=row['product_id'], body=body)
    print(res)

# subset = parquetFile.take(1000)
# es.indices.delete(index='consumer-insights')
# es.search(index='electronics', doc_type='review')

# es.delete_by_query(index='electronics', doc_type='review', body={"query": {"match_all": {}}})
for row in parquetFile.collect():
    body = row.asDict()
    res = es.index(index='reviews', doc_type='review', id=row['review_id'], body=body)
    print(res)

# response = es.search(index='electronics', doc_type='review', body={"query": {"match": {"marketplace": "US"}}})
#
# response = es.search(index='electronics', doc_type='review', body={"query": {"match": {"product_title": "Bluetooth"}}})
#
# for hit in response['hits']['hits']:
#     print(hit['_source'])

spark.stop()
elapsed_time = time.time() - start_time
print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
print('Done')
