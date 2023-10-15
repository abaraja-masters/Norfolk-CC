#!/bin/bash

# Set the Spark home directory
SPARK_HOME=/path/to/spark

# Set the path to the ETL script
ETL_SCRIPT=/path/to/etl_script.py

# Submit the Spark job
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 4G \
    --executor-cores 4 \
    $ETL_SCRIPT
