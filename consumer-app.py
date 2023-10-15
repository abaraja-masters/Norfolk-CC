# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the stream of data from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .load()

# Convert the stream to a DataFrame
df = stream.selectExpr("*")

# Write the stream to HDFS in Parquet format
df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/my-data") \
    .start()

# Wait for the stream to finish
stream.awaitTermination()
