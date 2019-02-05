from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
import time
from collections import Counter


def compute_analytics(product):
    body = product.asDict()
    ratings_counter = Counter(body['ratings'])
    body.update({str(k) + "_stars": v for k, v in ratings_counter.items()})
    body.update({"total_reviews": len(body["ratings"]),
                 "product_rating": round(sum(body["ratings"]) / len(body["ratings"]), 2)})
    return product["product_id"], body


start_time = time.time()

year = 1999

es_cluster = ["localhost:9200"]
es = Elasticsearch(es_cluster)

spark = SparkSession.builder.appName("consumer-insights").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)
# reviews = spark.read.parquet("../sample_input/*")
reviews = sqlContext.read.parquet('../../sample_input/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')
# reviews = sqlContext.read.parquet('s3://amazon-customer-reviews-dataset/parquet/product_category=Camera/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet')

es.indices.delete(index='products', ignore=[404])
es.indices.delete(index='reviews', ignore=[404])

es_write_products_conf = {
        "es.nodes": 'localhost',
        "es.port": '9200',
        "es.resource": 'products/product',
        "es.input.json": "yes",
        "es.mapping.id": "product_id"
        }

es_write_reviews_conf = {
        "es.nodes": 'localhost',
        "es.port": '9200',
        "es.resource": 'reviews/review',
        "es.input.json": "yes",
        "es.mapping.id": "review_id"
        }

reviews.printSchema()
for year in range(2005, 2006):
    reviews = reviews.filter(reviews.year == year)
    products = reviews.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"),
                                                                  F.collect_list("review_date").alias("review_dates"))

    products = products.rdd.map(lambda product: compute_analytics(product))

    reviews = reviews.rdd.map(lambda review: (review["review_id"], review.asDict()))

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

    print("Done with: " + year)

spark.stop()
elapsed_time = time.time() - start_time
print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
print('Done')
