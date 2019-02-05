#!/usr/bin/env bash

spark-submit --jars jars/elasticsearch-spark-20_2.11-6.6.0.jar
#--master spark://ec2-107-23-227-201.compute-1.amazonaws.com:7077
#--executor-memory 6G
spark_batch.py