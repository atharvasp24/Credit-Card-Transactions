import mysql.connector
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "athu24")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ayushi13")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "banking_system")


# Set up Spark session
spark = SparkSession.builder.appName("BankingSystem").getOrCreate()


# Connect to MySQL
db = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
)

cursor = db.cursor()


# Insert data into MySQL (after stream and batch processing)
def update_mysql(table, df):
    if table == "cards":
        query = f"UPDATE {table} SET current_balance = %s, credit_limit = %s WHERE card_id = %s"
        data = [
            (row["current_balance"], row["credit_limit"], row["card_id"])
            for row in df.collect()
        ]
    elif table == "customers":
        query = f"UPDATE {table} SET credit_score = %s WHERE customer_id = %s"
        data = [(row["credit_score"], row["customer_id"]) for row in df.collect()]
    else:
        return

    # Execute the batch update
    cursor.executemany(query, data)
    db.commit()


# Load updated data
df_cards_updated = spark.read.option("header", "true").csv("results/cards_updated.csv")
update_mysql("cards", df_cards_updated)

df_customers_updated = spark.read.option("header", "true").csv(
    "results/customers_updated.csv"
)
update_mysql("customers", df_customers_updated)

# Close MySQL connection and Spark session
cursor.close()
db.close()
spark.stop()
