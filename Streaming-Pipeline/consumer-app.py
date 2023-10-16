# Import the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from kafka import KafkaConsumer

# Create a SparkSession
spark = SparkSession.builder.appName('kafka-to-parquet').getOrCreate()

# Define the Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='my-group',
)

# Define the Kafka topic to consume from
topic = 'data_topic'

# Create a Spark Streaming DataFrame from the Kafka consumer
df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092')\
                                     .option('subscribe', topic)\
                                     .option('startingOffsets', 'earliest')\
                                     .load()

# Define the schema of the Parquet file
schema = StructType([
    col('random_number', 'int'),
    col('timestamp_ms', 'timestamp'),
])

# Write the Spark Streaming DataFrame to HDFS as a Parquet file
df.writeStream.format('parquet').option('path', 'hdfs://path/to/hdfs/raw-zone')\
                                .option('checkpointLocation', 'hdfs://path/to/checkpoint/directory')\
                                .schema(schema).start()
