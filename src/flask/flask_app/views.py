from flask_app import app
from elasticsearch5 import Elasticsearch
import json


@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"


@app.route('/product/<product_name>')
def retrieve_product(product_name=None):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    response = es.search(index='electronics', doc_type='review',
                         body={"query": {"match": {"product_title": product_name}}})
    response_string = json.dumps(response['hits']['hits'])
    return response_string


