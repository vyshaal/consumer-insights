#!/usr/bin/env bash

source $PRJ_DIR/setup/spark/spin-spark.sh

wait
echo "completed $CLUSTER_NAME"

source $PRJ_DIR/setup/elasticsearch/spin-elasticsearch.sh

wait
echo "completed $CLUSTER_NAME"

#source $PRJ_DIR/setup/flask/spin-flask.sh
#wait
#echo "completed $CLUSTER_NAME"