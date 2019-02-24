from flask_app import app
from flask_cors import CORS, cross_origin
from flask import jsonify
from elasticsearch import Elasticsearch
from configparser import ConfigParser
import json

"""
    Read the configurations and connecting to the database
"""
cors = CORS(app)
config = ConfigParser()
config.read('../../config.ini')
es_cluster = [{'host': config.get('elasticsearch', 'master'), 'port': config.get('elasticsearch', 'port')}]
es_client = Elasticsearch(es_cluster)
results_size = 25


@app.route('/')
@app.route('/index')
@cross_origin()
def index():
    """
    Direct to default home page
    """
    return "Welcome to Consumer Insights!!!"


@app.route('/api/product/search/')
@cross_origin()
def all_products():
    """
    Find all the popular products in the database
    :return: list of products matching the criteria
    """
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
    """
    Find the products that best matches keyword and meets few other constraints
    :param product_name: keyword that user searches for
    :return: list of products matching the criteria
    """
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
    """
    Find the product corresponding to the given id
    :param product_id: product id
    :return: product
    """
    response = es_client.get(index='products', doc_type='product', id=product_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/')
@cross_origin()
def fetch_product_reviews(product_id=None):
    """
    Find all reviews corresponding to the given product
    :param product_id: product id
    :return: list of reviews
    """
    response = es_client.search(index='reviews', doc_type='review',
                                body={"query": {"match": {"product_id": product_id}}})
    return jsonify(response)


@app.route('/api/review/<review_id>')
@cross_origin()
def fetch_review(review_id=None):
    """
    Find the review corresponding to the given id
    :param review_id: review id
    :return: review
    """
    response = es_client.get(index='reviews', doc_type='review', id=review_id)
    return jsonify(response['_source'])


@app.route('/api/product/<product_id>/review/search/<feature>')
@cross_origin()
def search_product_reviews(product_id=None, feature=None):
    """
    Find the reviews of given product that best matches the keyword and few other constraints
    :param product_id: product id
    :param feature: keyword that user searches for
    :return: list of reviews that matches the criteria
    """
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
