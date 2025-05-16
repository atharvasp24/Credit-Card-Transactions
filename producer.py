from pyspark.sql import SparkSession
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "transactions"
DATASET_NUMBER = "16"
dataset_path = f"dataset_{DATASET_NUMBER}/"


# Set up Spark session
spark = SparkSession.builder.appName("BankingSystem").getOrCreate()

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Load transactions data with PySpark
df_transactions = spark.read.option("header", "true").csv(
    os.path.join(dataset_path, "transactions.csv")
)

# Process each row and send to Kafka
for row in df_transactions.collect():
    transaction_data = row.asDict()  # Use raw row data
    producer.send(KAFKA_TOPIC, value=transaction_data)

# Ensure all messages are sent
producer.flush()
spark.stop()
