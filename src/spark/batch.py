from airflow import DAG
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from elasticsearch import Elasticsearch
from collections import Counter
import json
import datetime
import sys
import os


ES_HOST = "ec2-34-237-82-149.compute-1.amazonaws.com"
INITIAL_YEAR = "1999"
YEAR = sys.argv[1]

# hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com","ec2-54-209-26-36.compute-1.amazonaws.com","ec2-3-94-235-239.compute-1.amazonaws.com"]
#hosts = ["ec2-34-237-82-149.compute-1.amazonaws.com"]


def save_products():
    products.foreachPartition(compute_analytics)


def compute_analytics(product_list):
    es_cluster = [{'host': ES_HOST, 'port': 9200}]
    es_client = Elasticsearch(es_cluster)
    for product in product_list:
        body = product.asDict()
        ratings_counter = Counter(body['ratings'])
        body.update({str(k) + "_stars": ratings_counter[k] if k in ratings_counter else 0 for k in range(1, 6)})
        body.update({"total_reviews": len(body["ratings"]),
                     "product_rating": round(sum(body["ratings"]) / len(body["ratings"]), 3)})
        q = {
            "script": {
                "source": "ctx._source.product_rating = ((ctx._source.total_reviews*ctx._source.product_rating) + "
                          "(params.body.total_reviews*params.body.product_rating)) / "
                          "(ctx._source.total_reviews+params.body.total_reviews);"
                          "ctx._source.total_reviews += params.body.total_reviews;"
                          "ctx._source.ratings.addAll(params.body.ratings);"
                          "ctx._source.review_dates.addAll(params.body.review_dates);"
                          "ctx._source['1_stars'] += params.body['1_stars'];"
                          "ctx._source['2_stars'] += params.body['2_stars'];"
                          "ctx._source['3_stars'] += params.body['3_stars'];"
                          "ctx._source['4_stars'] += params.body['4_stars'];"
                          "ctx._source['5_stars'] += params.body['5_stars'];",
                "params": {
                    "body": body
                }
            },
            "upsert": body
        }
        es_client.update(index="products", doc_type="product", id=product["product_id"], body=q)


def save_reviews():
    reviews_rdd = reviews.rdd.map(lambda review: (review["review_id"], json.dumps(review.asDict(), default=str)))

    es_write_conf = {
        "es.nodes": ES_HOST,
        "es.port": '9200',
        "es.resource": 'reviews/review',
        "es.input.json": "yes",
        "es.mapping.id": "review_id",
        "es.nodes.wan.only": "True"
    }

    reviews_rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf
    )


def conditional_delete_indices():
    if YEAR == INITIAL_YEAR:
        es_client.indices.delete(index='products', ignore=[404])
        es_client.indices.delete(index='reviews', ignore=[404])


if __name__ == "__main__":
    es_cluster = [{'host': ES_HOST, 'port': 9200}]
    es_client = Elasticsearch(es_cluster)

    spark = SparkSession.builder \
        .master("spark://ec2-34-199-62-71.compute-1.amazonaws.com:7077") \
        .appName("consumer-insights") \
        .config("spark.executor.memory", "6gb") \
        .getOrCreate()

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    conditional_delete_indices()

    reviews = sqlContext.read.parquet("s3n://amazon-customer-reviews-dataset/timeseries/" + YEAR +
                                      "/Electronics/*.parquet")

    products = reviews.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"),
                                                                  F.collect_list("review_date").alias("review_dates"))

    save_products()
    save_reviews()
    spark.stop()
    print('Done with the year: ' + YEAR)
