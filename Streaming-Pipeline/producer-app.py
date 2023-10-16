# Import the necessary libraries
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import config.application as cfg  # importing variables from config

# Define the Kafka producer
producer = KafkaProducer(bootstrap_servers = cfg.kafka_localhost)

# Start a loop to stream random data to Kafka topics
while True:

    # Generate a random number
    random_number = random.randint(0, 100)

    # Get the current timestamp
    current_timestamp = datetime.now().timestamp()

    # Create a Kafka message
    message = KafkaMessage(
        value=str(random_number).encode('utf-8'),
        timestamp_ms=current_timestamp * 1000,
    )

    # Send the message to the Kafka topic
    producer.send(cfg.kafka_topic, message)

    # Wait for 1 second
    time.sleep(1)
