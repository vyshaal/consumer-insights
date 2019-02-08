from flask_app import app
from flask_cors import CORS, cross_origin
from flask import jsonify
from elasticsearch import Elasticsearch
import json


cors = CORS(app)
es_cluster = ["ec2-34-237-82-149.compute-1.amazonaws.com:9200"]
es = Elasticsearch(es_cluster)


@app.route('/')
@app.route('/index')
@cross_origin()
def index():
    return "Hello, World!"


@app.route('/api/product')
@cross_origin()
def all_products():
    response = es.search(index='products', doc_type='product',
                         body={"query": {"match_all": {}}})
    return jsonify(response['hits']['hits'])


@app.route('/api/product/search/<product_name>')
@cross_origin()
def search_product(product_name=None):
    body = {
        "size": 15,
        "query":
            {
                    "match": {"product_title": product_name}
            }
        ,
        "sort":
            {
                "total_reviews": {
                    "order": "desc"
                }
            }
        }

    response = es.search(index='products', doc_type='product', body=body)
    return jsonify(response)


@app.route('/api/product/<product_id>')
@cross_origin()
def fetch_product(product_id=None):
    response = es.get(index='products', doc_type='product', id=product_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/')
@cross_origin()
def fetch_product_reviews(product_id=None):
    response = es.search(index='reviews', doc_type='review',
                         body={"query": {"match": {"product_id": product_id}}})
    return jsonify(response)


@app.route('/api/review/<review_id>')
@cross_origin()
def fetch_review(review_id=None):
    response = es.get(index='reviews', doc_type='review', id=review_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/search/<feature>')
@cross_origin()
def search_product_reviews(product_id=None, feature=None):
    body = \
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "review_body": feature
                            }
                        },
                        {
                            "match": {
                                "product_id": product_id
                            }
                        }
                    ]
                }
            }
        }
    response = es.search(index='reviews', doc_type='review', body=body)
    return jsonify(response)
