import csv
import mysql.connector
import os
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


def ensure_database(cursor):
    # Create database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
    cursor.execute(f"USE {MYSQL_DATABASE}")


def clear_table_data(cursor):
    # Disable foreign key checks to allow truncation
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Truncate tables to remove all data
    cursor.execute("TRUNCATE TABLE transactions")
    cursor.execute("TRUNCATE TABLE cards")
    cursor.execute("TRUNCATE TABLE customers")
    cursor.execute("TRUNCATE TABLE credit_card_types")

    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")


def load_csv_to_mysql(cursor, table_name, csv_file, columns):
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            values = [row[col] if row[col] else None for col in columns]
            placeholders = ",".join(["%s"] * len(columns))
            cursor.execute(
                f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                values,
            )


def sample_query(cursor):
    cursor.execute(
        """
        SELECT c.name, ca.card_number, t.transaction_id, t.amount, t.status
        FROM customers c
        JOIN cards ca ON c.customer_id = ca.customer_id
        JOIN transactions t ON ca.card_id = t.card_id
        WHERE t.status = 'approved'
        LIMIT 5
    """
    )
    results = cursor.fetchall()
    for row in results:
        print(row)


def main():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        ensure_database(cursor)

        # Clear existing data from tables
        clear_table_data(cursor)

        # Load CSVs
        load_csv_to_mysql(
            cursor,
            "customers",
            os.path.join(dataset_path, "customers.csv"),
            [
                "customer_id",
                "name",
                "phone_number",
                "address",
                "email",
                "credit_score",
                "annual_income",
            ],
        )
        load_csv_to_mysql(
            cursor,
            "cards",
            os.path.join(dataset_path, "cards.csv"),
            [
                "card_id",
                "customer_id",
                "card_type_id",
                "card_number",
                "expiration_date",
                "credit_limit",
                "current_balance",
                "issue_date",
            ],
        )
        load_csv_to_mysql(
            cursor,
            "credit_card_types",
            os.path.join(dataset_path, "credit_card_types.csv"),
            [
                "card_type_id",
                "name",
                "credit_score_min",
                "credit_score_max",
                "credit_limit_min",
                "credit_limit_max",
                "annual_fee",
                "rewards_rate",
            ],
        )

        conn.commit()

        # Run sample query
        print("Sample Query Results:")
        sample_query(cursor)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
