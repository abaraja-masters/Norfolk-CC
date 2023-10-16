import pyspark.sql
import pyspark.sql.types as StructType
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col, round

# Create a SparkSession
spark = SparkSession.builder.appName('hdfs-etl-parquet').getOrCreate()

# Define the path to the HDFS directory containing the data
hdfs_raw_path = 'hdfs://path/to/hdfs/raw-zone'
hdfs_processed_path = 'hdfs://path/to/hdfs/processed-zone'

# Read the data from HDFS as a Spark DataFrame
df = spark.read.parquet(hdfs_raw_path)

# Check for null values
df = df.filter(df.col("random_number").isNotNull())

# Dynamic data validation - confirm random_number is in range of 0-100
df = df.filter(df['random_number'].between(0, 100))

# Schema validation
required_columns = ['random_number', 'timestamp_ms']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError('The following columns are missing from the input data: {}'.format(missing_columns))

# Data type validation - [ Integer, Timestamp ]
df = df.withColumn('random_number', df['random_number'].cast("integer"))
df = df.withColumn('timestamp_ms', df['timestamp_ms'].cast("timestamp"))

# Data formatting from Integer to Double with 2 decimal places
df = df.withColumn("random_number", col("random_number").cast("double"))
df = df.withColumn("random_number", round(col("random_number"), 2))

# Date Formatting to yyyy-MM-dd
date_format_pattern = "yyyy-MM-dd"
df = df.withColumn("date", date_format(col("timestamp_ms"), date_format_pattern))

# Write the validated data back to HDFS in Parquet format
df.write.parquet("hdfs://path/to/output/file.parquet")

# Partition the data on the date field
df = df.partitionBy('date')

# Write the Spark DataFrame back to HDFS as a Parquet format
df.write.parquet(hdfs_processed_path, mode='overwrite')

# Set the checkpoint location
spark.sparkContext.setCheckpointDir('hdfs://path/to/checkpoint/directory')

# Set the recovery mode to "latest"
spark.conf.set('spark.sql.streaming.failure.recoveryMode', 'latest')