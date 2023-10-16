# Norfolk-CC
Norfolk Southern - Code Challenge - Spark Technical Interview
GitHub url: https://github.com/abaraja-masters/Norfolk-CC

```
## Project Structure

├── Orchestration-Airflow/
│   ├── airflow-dag.py
│   └── script.sh
├── Pipeline-XML/
│   └── XML-ParseFlattening.py
├── Streaming-Pipeline/
│   ├── consumer-app.py
│   ├── ETL-app.py
│   └── producer-app.py
├── Norfolk-StreamingPipeline.pdf
└── README.md
```

## Producer Applicaton
+ Data is being produced and published to a Kafka Topic 'data_topic'.
+ Data being produced:
    + random_number in range from 0 to 100
    + current_timestamp by the millisecond


## Consumer Applicaton
+ Data is being in ingested to a Spark DataFrame.
+ DataFrame is loaded to a HDFS raw location in parquet format.


## ETL Applicaton
+ Data from HDFS raw location loaded to a Spark DataFrame.
+ Perform Data Validation.
+ Perform Fault Tolerance Checkpointing.
+ Partition DataFrame by Date, and load to a HDFS processed location in parquet format.


## Orchestration - Airflow
+ Set up a Bash Script to execute spark submit and handle checkpointing mechanism.
+ Set up an Airflow Dag to perform automation on the Bash Script every hour.


## Proof Of Concept: XML Parsing
+ Reads an XML file, load to a Spark DataFrame.
+ Parse the XML string into a Spark DataFrame.
+ Flatten the nested Spark DataFrame.
+ Write the flattened Spark DataFrame to an output (CSV).
