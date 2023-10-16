#!/bin/bash

# Set the path to the ETL script
ETL_SCRIPT=/path/to/etl_script.py
checkpoint_file="hdfs://path/to/checkpoint/directory"


# Check if the checkpoint file exists.
if [[ -f "$checkpoint_file" ]]; then
  # Read the checkpoint file to determine the last successful step of the Spark script.
  last_successful_step=$(cat "$checkpoint_file")
else
  # The checkpoint file does not exist, so start the Spark script from the beginning.
  last_successful_step=0
fi


# Submit the Spark job from the last successful step
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 4G \
    --executor-cores 4 \
    --checkpoint "$checkpoint_file" \
    $ETL_SCRIPT


# If the Spark script fails, save the current step in the checkpoint file.
if [[ $? -ne 0 ]]; then
  echo "$last_successful_step" > "$checkpoint_file"
fi


# Retry the Spark script after a certain interval.
sleep 60

# Repeat steps 4 and 5 until the Spark script completes successfully.
