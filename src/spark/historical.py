# from config.aws import AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MY_BUCKET
# import boto3
# from airflow import DAG
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch
from collections import Counter
import json
import datetime

ES_REMOTE = "ec2-34-237-82-149.compute-1.amazonaws.com"
ES_LOCAL = "localhost"


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)


def compute_analytics(product):
    body = product.asDict()
    ratings_counter = Counter(body['ratings'])
    body.update({str(k) + "_stars": v for k, v in ratings_counter.items()})
    body.update({"total_reviews": len(body["ratings"]),
                 "product_rating": round(sum(body["ratings"]) / len(body["ratings"]), 2)})
    return product["product_id"], json.dumps(body, cls=Encoder)


hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com","ec2-54-209-26-36.compute-1.amazonaws.com","ec2-3-94-235-239.compute-1.amazonaws.com"]
hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com"]

es_cluster = [{'hosts': hosts, 'port': 9200}]
es_client = Elasticsearch(es_cluster)


spark = SparkSession.builder\
    .master("spark://ec2-34-199-62-71.compute-1.amazonaws.com:7077")\
    .appName("consumer-insights")\
    .config("spark.executor.memory", "6gb")\
    .getOrCreate()

# spark = SparkSession.builder\
#     .appName("consumer-insights")\
#     .getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

reviews = sqlContext.read.parquet("s3n://amazon-customer-reviews-dataset/timeseries/1999/Electronics/*.parquet")

es_client.indices.delete(index='products', ignore=[404])
es_client.indices.delete(index='reviews', ignore=[404])

es_write_products_conf = {
    "es.nodes": ES_REMOTE,
    "es.port": '9200',
    "es.resource": 'products/product',
    "es.input.json": "yes",
    "es.mapping.id": "product_id",
    "es.nodes.wan.only": "True"
}

es_write_reviews_conf = {
    "es.nodes": ES_REMOTE,
    "es.port": '9200',
    "es.resource": 'reviews/review',
    "es.input.json": "yes",
    "es.mapping.id": "review_id",
    "es.nodes.wan.only": "True"
}

products = reviews.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"),
                                                              F.collect_list("review_date").alias("review_dates"))

products = products.rdd.map(lambda product: compute_analytics(product))

reviews = reviews.rdd.map(lambda review: (review["review_id"], json.dumps(review.asDict(), cls=Encoder)))

products.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_write_products_conf
)

reviews.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_write_reviews_conf
)

spark.stop()
print('Done')
