#!/usr/bin/env bash

# Running the spark job
spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.10:6.6.0 ~/consumer-insights/src/spark/spark_job.py