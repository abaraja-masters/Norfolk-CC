#!/bin/bash

source /config/application.cfg

# Check if the checkpoint file exists.
if [[ -f "$CHECKPOINT_FILE" ]]; then
  # Read the checkpoint file to determine the last successful step of the Spark script.
  LAST_SUCCESSFUL_STEP=$(cat "$CHECKPOINT_FILE")
else
  # The checkpoint file does not exist, so start the Spark script from the beginning.
  LAST_SUCCESSFUL_STEP=0
fi


# Submit the Spark job from the last successful step
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --executor-memory 4G \
    --executor-cores 4 \
    --checkpoint "$CHECKPOINT_FILE" \
    $ETL_SCRIPT


# Captures the exit code from Spark Job
exit_code=$?


# If the Spark script fails, save the current step in the checkpoint file.
# Log success or failure
if [[ $exit_code -ne 0 ]]; then
  echo "$LAST_SUCCESSFUL_STEP" > "$CHECKPOINT_FILE"
  echo "Spark job failed at: $(date +%x_%r)" >> $LOG_FILE
else
  echo "Spark job succeeded at: $(date +%x_%r)" >> $LOG_FILE
fi


# Retry the Spark script after a certain interval.
sleep 60
