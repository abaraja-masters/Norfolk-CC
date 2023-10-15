from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Read the Parquet file from HDFS
df = spark.read.parquet("hdfs://path/to/input/file.parquet")

# Check for null values
df = df.filter(df.col("column_name").isNotNull())

# Check for empty strings
df = df.filter(df.col("column_name").isNotEmpty())

# Check for invalid data types
# For example, you can check that the values in a column are of the expected data type, such as integer or string.

# Dynamic data validation
# For example, we can validate the age to be between 18 and 65
df = df.filter(df['age'].between(18, 65))

# Schema validation
required_columns = ['name', 'age', 'occupation']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError('The following columns are missing from the input data: {}'.format(missing_columns))

# Data type validation
df = df.withColumn('age', df['age'].cast(IntegerType()))
df = df.withColumn('occupation', df['occupation'].cast(StringType()))

# Data formatting
# For example, we can convert the occupation to lowercase
df = df.withColumn('occupation', df['occupation'].lower())


# Write the validated data back to HDFS in Parquet format
df.write.parquet("hdfs://path/to/output/file.parquet")

# Set the checkpoint location
spark.sparkContext.setCheckpointDir('/path/to/checkpoint')

# Set the recovery mode to "latest"
spark.conf.set('spark.sql.streaming.failure.recoveryMode', 'latest')