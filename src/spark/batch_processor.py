from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from elasticsearch import Elasticsearch
from collections import Counter
import json
from configparser import ConfigParser


class BatchProcessor:

    def __init__(self, es_master, spark_master):
        """
        Initializes a batch processor and reads current year from configuration file
        :param es_master: public dns of elasticsearch-cluster
        :param spark_master: public dns of spark-cluster
        """
        self.ES_HOST = es_master
        self.es_cluster = [{'host': self.ES_HOST, 'port': 9200}]
        self.es_client = Elasticsearch(self.es_cluster)

        self.spark = SparkSession.builder \
            .master("spark://"+spark_master+":7077") \
            .appName("consumer-insights") \
            .config("spark.executor.memory", "6gb") \
            .getOrCreate()

        self.sc = self.spark.sparkContext
        self.sqlContext = SQLContext(self.sc)

        config = ConfigParser()
        config.read('/home/ubuntu/consumer-insights/config.ini')
        self.INITIAL_YEAR = str(config.get('year', 'initial_year'))
        self.FINAL_YEAR = str(config.get('year', 'final_year'))
        self.YEAR_PATH = config.get('year', 'year_path')
        self.reviews = None
        self.products = None

    def process_reviews(self, current_year):
        """
        Reads reviews from s3 bucket and aggregates them by product
        :param current_year: year whose reviews need to be processed
        """
        self.reviews = self.sqlContext.read.parquet("s3n://amazon-customer-reviews-dataset/timeseries/" + current_year +
                                                    "/Electronics/*.parquet")
        self.products = self.reviews.groupby("product_id", "product_title").agg(
            F.collect_list("star_rating").alias("ratings"), F.collect_list("review_date").alias("review_dates"))

    def save_products(self):
        """
        Persisting products into the database
        """
        def compute_analytics(product_list):
            """
            Calculating the product analytics and upserting into the database
            :param product_list: List of products that are present in a partition
            """
            es_cluster = [{'host': "ec2-34-237-82-149.compute-1.amazonaws.com", 'port': 9200}]
            es_client = Elasticsearch(es_cluster)
            for product in product_list:
                body = product.asDict()
                ratings_counter = Counter(body['ratings'])
                body.update({str(k) + "_stars": ratings_counter[k] if k in ratings_counter else 0 for k in range(1, 6)})
                body.update({"total_reviews": len(body["ratings"]),
                             "product_rating": round(sum(body["ratings"]) / len(body["ratings"]), 3)})
                query = {
                    "script": {
                        "source": "ctx._source.product_rating = ((ctx._source.total_reviews*ctx._source.product_rating)"
                                  " + (params.body.total_reviews*params.body.product_rating)) / "
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
                es_client.update(index="products", doc_type="product", id=product["product_id"], body=query)

        self.products.foreachPartition(compute_analytics)

    def save_reviews(self):
        """
        Persisting reviews into the database
        """
        reviews_rdd = self.reviews.rdd.map(lambda review: (review["review_id"],
                                                           json.dumps(review.asDict(), default=str)))

        es_write_conf = {
            "es.nodes": self.ES_HOST,
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

    def conditional_delete_indices(self, year, settings, mappings):
        """
        Deletion & Creation of new indices
        :param year: the year whose reviews need to be processed
        :param settings: elasticsearch configuration settings
        :param mappings: schema for reviews
        """
        if year == self.INITIAL_YEAR or year == "*":
            self.es_client.indices.delete(index='products', ignore=[404])
            self.es_client.indices.delete(index='reviews', ignore=[404])
            review_settings = \
                {
                    "settings": settings,
                    "mappings": mappings
                }
            self.es_client.indices.create(index='reviews', body=review_settings)

    def stop_spark(self):
        """
        Stopping spark instance
        """
        self.spark.stop()
