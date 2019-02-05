#!/usr/bin/env bash

CLUSTER_NAME=flask

peg up $PRJ_DIR/setup/flask/master.yml &

wait

peg fetch ${CLUSTER_NAME}

wait

peg install ${CLUSTER_NAME} ssh

wait

peg install ${CLUSTER_NAME} aws