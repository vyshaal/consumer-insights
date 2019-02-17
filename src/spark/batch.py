from airflow import DAG
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch
from collections import Counter
import json
import datetime
import sys

ES_HOST = "ec2-34-237-82-149.compute-1.amazonaws.com"
YEAR = sys.argv[1]


# hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com","ec2-54-209-26-36.compute-1.amazonaws.com","ec2-3-94-235-239.compute-1.amazonaws.com"]
#hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com"]

es_cluster = [{'host': ES_HOST, 'port': 9200}]
es_client = Elasticsearch(es_cluster)


spark = SparkSession.builder\
    .master("spark://ec2-34-199-62-71.compute-1.amazonaws.com:7077")\
    .appName("consumer-insights")\
    .config("spark.executor.memory", "6gb")\
    .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

reviews = sqlContext.read.parquet("s3n://amazon-customer-reviews-dataset/timeseries/"+YEAR+"/Electronics/*.parquet")

es_client.indices.delete(index='products', ignore=[404])
es_client.indices.delete(index='reviews', ignore=[404])


products = reviews.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"),
                                                              F.collect_list("review_date").alias("review_dates"))


for product in products.collect():
    body = product.asDict()
    ratings_counter = Counter(body['ratings'])
    body.update({str(k) + "_stars": v for k, v in ratings_counter.items()})
    body.update({"total_reviews": len(body["ratings"]),
                 "product_rating": round(sum(body["ratings"]) / len(body["ratings"]), 2)})
    es_client.index(index='products', doc_type='product', id=product['product_id'], body=body)

for review in reviews.collect():
    res = es_client.index(index='reviews', doc_type='review', id=review['review_id'], body=review.asDict())


spark.stop()
print('Done')
