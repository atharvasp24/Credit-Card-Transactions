import csv
import os
from typing import Dict

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "athu24")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ayushi13")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "banking_system")

DATASET_NUMBER = "16"
dataset_path = f"dataset_{DATASET_NUMBER}/"


def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def calculate_credit_score_adjustment(usage_percentage: float) -> int:
    """Calculate credit score adjustment based on credit usage percentage.

    Args:
        usage_percentage: Credit usage as a percentage of total available credit (0-100)

    Returns:
        int: Credit score adjustment (positive or negative)
    """
    if not isinstance(usage_percentage, (int, float)) or usage_percentage < 0:
        return 0
    if usage_percentage <= 10:
        return 15
    elif usage_percentage <= 20:
        return 10
    elif usage_percentage <= 30:
        return 5
    elif usage_percentage <= 50:
        return -5
    elif usage_percentage <= 70:
        return -15
    else:
        return -25


def calculate_new_credit_limit(old_limit: float, credit_score_change: int) -> float:
    """Calculate new credit limit based on credit score changes.

    Args:
        old_limit: Current credit limit
        credit_score_change: Amount the credit score changed

    Returns:
        float: New credit limit
    """
    if not isinstance(old_limit, (int, float)) or old_limit < 0:
        return old_limit
    if not isinstance(credit_score_change, int):
        return old_limit

    if credit_score_change >= 0:
        return old_limit

    if credit_score_change <= -20:
        reduction_factor = 0.85
    elif credit_score_change <= -10:
        reduction_factor = 0.90
    else:
        reduction_factor = 0.95

    return round(old_limit * reduction_factor, -2)


def calculate_credit_usage_percentage(
    total_balance: float, total_limit: float
) -> float:
    """Calculate credit usage percentage.

    Args:
        total_balance: Sum of current and pending balances across all cards
        total_limit: Sum of credit limits across all cards

    Returns:
        float: Credit usage percentage
    """
    if total_limit <= 0:
        return 0.0
    return (total_balance / total_limit) * 100


def main():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    # Read stream_transactions.csv
    stream_transactions = []
    with open("results/stream_transactions.csv", "r") as f:
        reader = csv.DictReader(f)
        stream_transactions = list(reader)

    # Process batch transactions (approve pending)
    batch_transactions = []
    for t in stream_transactions:
        t_copy = t.copy()
        if t["status"] == "pending":
            t_copy["status"] = "approved"
        batch_transactions.append(t_copy)

    # Write batch_transactions.csv
    os.makedirs("results", exist_ok=True)
    with open("results/batch_transactions.csv", "w", newline="") as f:
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
        writer.writerows(batch_transactions)

    # Update card balances
    card_balances = {}
    for t in batch_transactions:
        if t["status"] == "approved":
            card_id = int(t["card_id"])
            amount = float(t["amount"])
            if card_id not in card_balances:
                cursor.execute(
                    "SELECT current_balance FROM cards WHERE card_id = %s", (card_id,)
                )
                result = cursor.fetchone()
                card_balances[card_id] = result[0] if result else 0
            card_balances[card_id] += amount

    # Write updated card balances to cards_updated.csv
    cards = []
    with open(os.path.join(dataset_path, "cards.csv"), "r") as f:
        reader = csv.DictReader(f)
        cards = list(reader)

    cards_updated = []
    for card in cards:
        card_copy = card.copy()
        card_id = int(card["card_id"])
        if card_id in card_balances:
            card_copy["current_balance"] = str(card_balances[card_id])
        cards_updated.append(card_copy)

    with open("results/cards_updated.csv", "w", newline="") as f:
        fieldnames = [
            "card_id",
            "customer_id",
            "card_type_id",
            "card_number",
            "expiration_date",
            "credit_limit",
            "current_balance",
            "issue_date",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cards_updated)

    # Calculate credit usage and update credit scores
    customer_balances = {}
    customer_limits = {}
    for card in cards_updated:
        customer_id = int(card["customer_id"])
        balance = float(card["current_balance"])
        limit = float(card["credit_limit"])
        if customer_id not in customer_balances:
            customer_balances[customer_id] = 0
            customer_limits[customer_id] = 0
        customer_balances[customer_id] += balance
        customer_limits[customer_id] += limit

    customers = []
    with open(os.path.join(dataset_path, "customers.csv"), "r") as f:
        reader = csv.DictReader(f)
        customers = list(reader)

    customers_updated = []
    credit_score_changes = {}
    for customer in customers:
        customer_copy = customer.copy()
        customer_id = int(customer["customer_id"])
        if customer_id in customer_balances:
            usage_percentage = calculate_credit_usage_percentage(
                customer_balances[customer_id], customer_limits[customer_id]
            )
            score_adjustment = calculate_credit_score_adjustment(usage_percentage)
            new_score = int(customer["credit_score"]) + score_adjustment
            customer_copy["credit_score"] = str(new_score)
            credit_score_changes[customer_id] = score_adjustment
        customers_updated.append(customer_copy)

    with open("results/customers_updated.csv", "w", newline="") as f:
        fieldnames = [
            "customer_id",
            "name",
            "phone_number",
            "address",
            "email",
            "credit_score",
            "annual_income",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers_updated)

    # Adjust credit limits based on credit score changes
    final_cards_updated = []
    for card in cards_updated:
        card_copy = card.copy()
        customer_id = int(card["customer_id"])
        if (
            customer_id in credit_score_changes
            and credit_score_changes[customer_id] < 0
        ):
            old_limit = float(card["credit_limit"])
            new_limit = calculate_new_credit_limit(
                old_limit, credit_score_changes[customer_id]
            )
            card_copy["credit_limit"] = str(new_limit)
        final_cards_updated.append(card_copy)

    # Correct fieldnames for cards_updated.csv
    fieldnames = [
        "card_id",
        "customer_id",
        "card_type_id",
        "card_number",
        "expiration_date",
        "credit_limit",
        "current_balance",
        "issue_date",
    ]

    with open("results/cards_updated.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_cards_updated)

    # Update MySQL
    try:
        for card in final_cards_updated:
            cursor.execute(
                """
                UPDATE cards
                SET current_balance = %s, credit_limit = %s
                WHERE card_id = %s
            """,
                (card["current_balance"], card["credit_limit"], card["card_id"]),
            )

        for customer in customers_updated:
            cursor.execute(
                """
                UPDATE customers
                SET credit_score = %s
                WHERE customer_id = %s
            """,
                (customer["credit_score"], customer["customer_id"]),
            )

        conn.commit()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
