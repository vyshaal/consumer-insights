from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch
from collections import Counter
import datetime
import json
import os


ES_HOST = "ec2-34-237-82-149.compute-1.amazonaws.com"
INITIAL_YEAR = "1999"
YEAR_PATH = os.environ['YEAR_PATH']


def conditional_delete_indices(year):
    if year == INITIAL_YEAR or year == "*":
        es_client.indices.delete(index='products', ignore=[404])
        es_client.indices.delete(index='reviews', ignore=[404])
        review_settings = \
            {
              "settings": {
                "index": {
                  "number_of_shards": 3,
                  "number_of_replicas": 2,
                  "refresh_interval": "1s"
                },
                "analysis": {
                  "analyzer": {
                      "ci-analyzer": {
                          "type": "custom",
                          "filter": [
                              "standard",
                              "lowercase",
                              "synonym",
                              "snowball"
                          ],
                          "tokenizer": "standard"
                      }
                  },
                  "filter": {
                      "synonym": {
                          "type": "synonym",
                          "synonyms": ["annoy, irritate, vex, piss",
                                       "horrible, awful, terrible",
                                       "great, excellent, outstanding, awesome, extraordinary, decent, supreme",
                                       "please, satisfy, enjoy",
                                       "music, sound, bass",
                                       "quality, feature, standard"]
                      }
                  }
                }
              },
              "mappings": {
                "review": {
                  "properties": {
                    "year": {
                      "type": "long"
                    },
                    "star_rating": {
                      "type": "integer"
                    },
                    "product_parent": {
                      "type": "long"
                    },
                    "review_headline": {
                      "type": "text",
                      "analyzer": "ci-analyzer"
                    },
                    "review_body": {
                      "type": "text",
                      "analyzer": "ci-analyzer"
                    },
                    "helpful_votes": {
                      "type": "long"
                    },
                    "total_votes": {
                      "type": "long"
                    },
                    "marketplace": {
                      "type": "text"
                    },
                    "vine": {
                      "type": "text"
                    },
                    "review_id": {
                      "type": "text"
                    },
                    "verified_purchase": {
                      "type": "text"
                    },
                    "product_id": {
                      "type": "text"
                    },
                    "review_date": {
                      "type": "date"
                    },
                    "customer_id": {
                      "type": "text"
                    },
                    "product_title": {
                      "type": "text"
                    }
                  }
                }
              }
            }

        es_client.indices.create(index='reviews', body=review_settings)


def get_year():
    read_file = open(YEAR_PATH, "r")
    year = str(read_file.read())
    if year == "*":
        return year
    if year is None or year == "" or int(year) > 2015:
        year = "1999"
    return year


def update_year(year):
    if year == "*":
        return
    write_file = open(YEAR_PATH, "w")
    write_file.write(str(int(year) + 1))
    write_file.close()


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

    current_year = get_year()
    conditional_delete_indices(current_year)
    reviews = sqlContext.read.parquet("s3n://amazon-customer-reviews-dataset/timeseries/" + current_year +
                                      "/Electronics/*.parquet")

    products = reviews.groupby("product_id", "product_title").agg(F.collect_list("star_rating").alias("ratings"),
                                                                  F.collect_list("review_date").alias("review_dates"))

    save_products()
    save_reviews()
    spark.stop()
    update_year(current_year)
    print('Done with the year: ' + current_year)
