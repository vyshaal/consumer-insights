from flask_app import app
from flask_cors import CORS, cross_origin
from flask import jsonify
from elasticsearch import Elasticsearch
import json


cors = CORS(app)
config.read('/home/ubuntu/consumer-insights/config.ini')
es_cluster = [{'host': config.get('elasticsearch', 'master'), 'port': config.get('elasticsearch', 'port')}]
es_client = Elasticsearch(es_cluster)
results_size = 25


@app.route('/')
@app.route('/index')
@cross_origin()
def index():
    return "Welcome to Consumer Insights!!!"


@app.route('/api/product/search/')
@cross_origin()
def all_products():
    body = \
    {
        "size": results_size,
        "query": {
            "match_all": {}
        },
        "sort": {
            "total_reviews": {
                "order": "desc"
            }
        }
    }
    response = es_client.search(index='products', doc_type='product', body=body)
    return jsonify(response)


@app.route('/api/product/search/<product_name>')
@cross_origin()
def search_product(product_name=None):
    body = \
        {
            "size": results_size,
            "query": {
                "function_score": {
                    "query": {
                        "match": {
                            "product_title": {
                                "query": product_name,
                                "minimum_should_match": 2,
                                "fuzziness": "AUTO"
                            }
                        }
                    },
                    "field_value_factor": {
                        "field": "total_reviews",
                        "modifier": "log1p",
                        "factor": 2
                    }
                }
            }
        }
    response = es_client.search(index='products', doc_type='product', body=body)
    return jsonify(response)


@app.route('/api/product/<product_id>')
@cross_origin()
def fetch_product(product_id=None):
    response = es_client.get(index='products', doc_type='product', id=product_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/')
@cross_origin()
def fetch_product_reviews(product_id=None):
    response = es_client.search(index='reviews', doc_type='review',
                                body={"query": {"match": {"product_id": product_id}}})
    return jsonify(response)


@app.route('/api/review/<review_id>')
@cross_origin()
def fetch_review(review_id=None):
    response = es_client.get(index='reviews', doc_type='review', id=review_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/search/<feature>')
@cross_origin()
def search_product_reviews(product_id=None, feature=None):
    body = \
        {
            "size": results_size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": feature,
                                "fields": ["review_body", "review_headline^2"],
                                "type": "phrase",
                                "slop": 5
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
    response = es_client.search(index='reviews', doc_type='review', body=body)
    return jsonify(response)
