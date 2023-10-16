# Import the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from kafka import KafkaConsumer
import config.application as cfg  # importing variables from config

# Create a SparkSession
spark = SparkSession.builder.appName('kafka-to-parquet').getOrCreate()

# Define the Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers = cfg.kafka_localhost,
    auto_offset_reset='latest',
    group_id='my-group',
)

# Create a Spark Streaming DataFrame from the Kafka consumer
df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', cfg.kafka_localhost)\
                                     .option('subscribe', cfg.kafka_topic)\
                                     .option('startingOffsets', 'earliest')\
                                     .load()

# Define the schema of the Parquet file
schema = StructType([
    col(cfg.col_random_num, 'int'),
    col(cfg.col_timestamp, 'timestamp'),
])

# Write the Spark Streaming DataFrame to HDFS as a Parquet file
df.writeStream.format('parquet').option('path', cfg.hdfs_raw_path)\
                                .option('checkpointLocation', cfg.checkpoint_path)\
                                .schema(schema).start()
