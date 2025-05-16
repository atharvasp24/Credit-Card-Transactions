import csv
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "transactions"
DATASET_NUMBER = "16"
dataset_path = f"dataset_{DATASET_NUMBER}/"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main():
    producer = create_producer()

    with open(os.path.join(dataset_path, "transactions.csv"), "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Send transaction to Kafka
            producer.send(KAFKA_TOPIC, row)
            print(f"Sent transaction {row['transaction_id']} to Kafka")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
