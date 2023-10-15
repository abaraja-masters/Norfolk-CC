# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame of data to stream to Kafka
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carol"),
], ["id", "name"])

# Start a Spark Structured Streaming stream
stream = df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "my-topic") \
    .start()

# Loop until the stream is finished
while not stream.isActive():
    # Process the next batch of data
    batch = stream.read()
    # Write the batch of data to Kafka
    stream.write(batch)

# Wait for the stream to finish
stream.awaitTermination()
