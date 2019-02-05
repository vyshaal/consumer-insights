#!/usr/bin/env bash

CLUSTER_NAME=elastic-cluster

peg up $PRJ_DIR/setup/elasticsearch/master.yml &
peg up $PRJ_DIR/setup/elasticsearch/workers.yml &
wait

peg fetch ${CLUSTER_NAME}
wait

peg install ${CLUSTER_NAME} ssh
wait

peg install ${CLUSTER_NAME} aws
wait

peg install ${CLUSTER_NAME} environment
wait

peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} elasticsearch

wait

peg service ${CLUSTER_NAME} hadoop start
peg service ${CLUSTER_NAME} elasticsearch start

# peg service ${CLUSTER_NAME} hadoop stop
# peg service ${CLUSTER_NAME} elasticsearch stop
