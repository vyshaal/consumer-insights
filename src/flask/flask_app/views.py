from flask_app import app
from flask_cors import CORS, cross_origin
from flask import jsonify
from elasticsearch5 import Elasticsearch
import json

cors = CORS(app)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


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
        "query":
            {
                    "match": {"product_title": product_name}
            }
        }

    response = es.search(index='products', doc_type='product', body=body)
    return jsonify(response['hits']['hits'])


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
    return jsonify(response['hits']['hits'])


@app.route('/api/product/<product_id>/review/<review_id>')
@cross_origin()
def fetch_product_review(product_id=None, review_id=None):
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
    return jsonify(response['hits']['hits'])
