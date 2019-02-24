"""
    Settings related to Reviews
"""
settings = \
    {
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
    }


"""
    Schema related to reviews
"""
mappings = \
    {
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

