from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col, round
import config.application as cfg  # importing variables from config

# Create a SparkSession
spark = SparkSession.builder.appName('hdfs-etl-parquet').getOrCreate()

# Read the data from HDFS as a Spark DataFrame
df = spark.read.parquet(cfg.hdfs_raw_path)

# Check for null values
df = df.filter(df.col(cfg.col_random_num).isNotNull())

# Dynamic data validation - confirm random_number is in range of 0-100
df = df.filter(df[cfg.col_random_num].between(0, 100))

# Schema validation
required_columns = [cfg.col_random_num, cfg.col_timestamp]
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError('The following columns are missing from the input data: {}'.format(missing_columns))

# Data type validation - [ Integer, Timestamp ]
df = df.withColumn(cfg.col_random_num, df[cfg.col_random_num].cast("integer"))
df = df.withColumn(cfg.col_timestamp, df[cfg.col_timestamp].cast("timestamp"))

# Data formatting from Integer to Double with 2 decimal places
df = df.withColumn(cfg.col_random_num, col(cfg.col_random_num).cast("double"))
df = df.withColumn(cfg.col_random_num, round(col(cfg.col_random_num), 2))

# Date Formatting to yyyy-MM-dd
df = df.withColumn(cfg.col_date, date_format(col(cfg.col_timestamp), cfg.date_format_pattern))

# Partition the data on the date field
df = df.partitionBy(cfg.col_date)

# Write the Spark DataFrame back to HDFS as a Parquet format
df.write.parquet(cfg.hdfs_processed_path, mode='overwrite')

# Set the checkpoint location
spark.sparkContext.setCheckpointDir(cfg.checkpoint_path)

# Set the recovery mode to "latest"
spark.conf.set('spark.sql.streaming.failure.recoveryMode', 'latest')