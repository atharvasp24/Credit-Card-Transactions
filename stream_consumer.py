import csv
import json
import logging
import os
import re
from typing import Dict, Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer
import mysql.connector

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "transactions"
MYSQL_HOST = os.getenv("MYSQL", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "athu24")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ayushi13")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "banking_system")


def get_mysql_connection():
    try:
        return mysql.connector.connect(
            user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE
        )
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to MySQL: {err}")
        raise


def fetch_card_info(cursor, card_id):
    try:
        cursor.execute(
            "SELECT credit_limit, current_balance FROM cards WHERE card_id = %s",
            (card_id,),
        )
        result = cursor.fetchone()
        if result:
            return {"credit_limit": result[0], "current_balance": result[1]}
        return {"credit_limit": 0, "current_balance": 0}
    except mysql.connector.Error as err:
        logging.error(f"Error fetching card info for card_id {card_id}: {err}")
        return {"credit_limit": 0, "current_balance": 0}


def fetch_customer_zip(cursor, card_id):
    try:
        cursor.execute(
            """
            SELECT c.address
            FROM customers c
            JOIN cards ca ON c.customer_id = ca.customer_id
            WHERE ca.card_id = %s
        """,
            (card_id,),
        )
        result = cursor.fetchone()
        if result:
            return extract_zip_code(result[0])
        return None
    except mysql.connector.Error as err:
        logging.error(f"Error fetching customer zip for card_id {card_id}: {err}")
        return None


def extract_zip_code(address: str) -> Optional[str]:
    """Extract zip code from an address string using regex."""
    if not address or not isinstance(address, str):
        return None
    match = re.search(r"\b\d{5}\b", address)
    return match.group(0) if match else None


def is_location_close_enough(zip1: str, zip2: str) -> bool:
    """Determine if merchant location is close enough to the customer's address.

    Args:
        zip1: Customer's zip code
        zip2: Merchant's zip code

    Returns:
        bool: True if the locations are close enough to approve, False if too far apart
    """
    if not zip1 or not zip2 or len(zip1) < 5 or len(zip2) < 5:
        return False  # Safer to reject when we can't verify

    # Check first digits of zip codes to determine proximity
    if zip1[:1] != zip2[:1]:
        return False  # Different regions

    if zip1[:2] != zip2[:2]:
        return True  # Moderately far but approve

    return True  # Same first 2+ digits - close enough


def process_and_validate_transaction(
    data: Dict, card_info: Dict, customer_zip: str, pending_balances: Dict[int, float]
) -> Dict:
    """Process and validate a transaction.

    Args:
        data: Transaction data with fields: transaction_id, card_id, merchant_name, timestamp, amount, location, transaction_type, related_transaction_id
        card_info: Card info with fields: credit_limit, current_balance
        customer_zip: Customer's zip code
        pending_balances: Dictionary tracking pending balances per card_id

    Returns:
        dict: Processed transaction with status and pending_balance
    """
    transaction_id = data.get("transaction_id")
    card_id = data.get("card_id")
    amount = float(data.get("amount", 0))
    location = data.get("location", "")
    transaction_type = data.get("transaction_type", "")

    # Initialize status and pending balance
    data["status"] = "pending"
    current_balance = float(card_info.get("current_balance", 0))
    credit_limit = float(card_info.get("credit_limit", 0))
    pending_balance = pending_balances.get(card_id, current_balance)

    # Extract merchant zip code
    merchant_zip = extract_zip_code(location)

    # Validation 1: Negative amount for non-refunded/cancelled transactions
    if amount < 0 and transaction_type not in ["refund", "cancellation"]:
        data["status"] = "declined"
        print(
            f"Transaction {transaction_id} declined: Negative amount for non-refunded/cancelled transaction"
        )
        return data

    # Validation 2: Amount ≥ 50% of credit limit
    if abs(amount) >= 0.5 * credit_limit:
        data["status"] = "declined"
        print(
            f"Transaction {transaction_id} declined: Amount {abs(amount)} is >= 50% of credit limit {credit_limit}"
        )
        return data

    # Validation 3: Merchant location too far
    if (
        merchant_zip
        and customer_zip
        and not is_location_close_enough(customer_zip, merchant_zip)
    ):
        data["status"] = "declined"
        print(
            f"Transaction {transaction_id} declined: Merchant location {merchant_zip} too far from customer {customer_zip}"
        )
        return data

    # Validation 4: Pending balance exceeds credit limit
    new_pending_balance = pending_balance + amount
    if new_pending_balance > credit_limit:
        data["status"] = "declined"
        print(
            f"Transaction {transaction_id} declined: Pending balance {new_pending_balance} exceeds credit limit {credit_limit}"
        )
        return data

    # Update pending balance for approved transactions
    if data["status"] == "pending":
        pending_balances[card_id] = new_pending_balance
        data["pending_balance"] = new_pending_balance

    return data


def main():
    logging.basicConfig(level=logging.INFO)

    output_file = "results/stream_transactions.csv"
    if os.path.exists(output_file):
        try:
            os.remove(output_file)
            logging.info(f"Deleted existing file: {output_file}")
        except OSError as e:
            logging.error(f"Error deleting file {output_file}: {e}")

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stream_trasncations",
    )

    # Initialize MySQL connection
    conn = get_mysql_connection()
    cursor = conn.cursor()

    pending_balances = {}
    output_rows = []

    try:
        for message in consumer:
            transaction = message.value
            card_id = int(transaction["card_id"])

            # Fetch card and customer info
            card_info = fetch_card_info(cursor, card_id)
            customer_zip = fetch_customer_zip(cursor, card_id)

            # Validate transaction
            validated_transaction = process_and_validate_transaction(
                transaction, card_info, customer_zip, pending_balances
            )

            # Prepare output row
            output_row = {
                "transaction_id": transaction["transaction_id"],
                "card_id": transaction["card_id"],
                "merchant_name": transaction["merchant_name"],
                "timestamp": transaction["timestamp"],
                "amount": transaction["amount"],
                "location": transaction["location"],
                "transaction_type": transaction["transaction_type"],
                "related_transaction_id": transaction.get("related_transaction_id", ""),
                "status": validated_transaction["status"],
                "pending_balance": validated_transaction.get(
                    "pending_balance", card_info["current_balance"]
                ),
            }
            output_rows.append(output_row)

            # Update MySQL with pending balance if needed
            if validated_transaction["status"] == "pending":
                cursor.execute(
                    """
                    UPDATE cards
                    SET current_balance = %s
                    WHERE card_id = %s
                """,
                    (validated_transaction["pending_balance"], card_id),
                )
                conn.commit()

    except KeyboardInterrupt:
        logging.info("Stream consumption interrupted.")

    finally:
        # Delete old CSV file if it exists
        output_file = "results/stream_transactions.csv"

        # Write to stream_transactions.csv (overwrites due to "w" mode)
        os.makedirs("results", exist_ok=True)
        with open(output_file, "w", newline="") as f:
            fieldnames = [
                "transaction_id",
                "card_id",
                "merchant_name",
                "timestamp",
                "amount",
                "location",
                "transaction_type",
                "related_transaction_id",
                "status",
                "pending_balance",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)

        logging.info(
            f"Processed {len(output_rows)} transactions, output saved to '{output_file}'."
        )

        # Cleanup resources
        cursor.close()
        conn.close()
        consumer.close()
        logging.info("MySQL connection and Kafka consumer closed.")


if __name__ == "__main__":
    main()
