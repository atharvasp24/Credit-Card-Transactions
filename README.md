
# Banking System Data Pipeline

This project simulates a streaming + batch data pipeline for a banking system using Kafka, PySpark, and MySQL. It handles transaction validation, customer and card updates, and credit score adjustments based on usage patterns.

## 📁 Project Structure

```
.
├── dataset_16/
│   ├── customers.csv
│   ├── cards.csv
│   ├── transactions.csv
│   └── credit_card_types.csv
├── results/
│   ├── stream_transactions.csv
│   ├── batch_transactions.csv
│   ├── cards_updated.csv
│   └── customers_updated.csv
├── .env
├── main_stream.py
├── main_batch.py
├── main_kafka_producer.py
├── main_mysql_loader.py
├── main_mysql_updater.py
└── README.md
```

## ⚙️ Technologies Used

- **Python**
- **Apache Kafka**
- **Apache Spark (PySpark)**
- **MySQL**
- **dotenv**
- **csv/json**

## 🔄 Workflow

1. **Kafka Producer** - Reads CSV transactions and sends them to Kafka.
2. **Stream Processing** - Kafka consumer validates each transaction in real-time and updates the database.
3. **Batch Processing** - Approves pending transactions, updates balances, adjusts credit scores and limits.
4. **MySQL Updates** - Final Spark-based updates pushed back to the database.

## ✅ Transaction Validations

- Reject negative amounts (unless refunds).
- Reject large amounts (≥ 50% of credit limit).
- Decline transactions with far merchant locations (based on ZIP code).
- Check if transaction exceeds credit limit with pending balance.

## 📦 Installation

```bash
pip install -r requirements.txt
```

## 🧪 Running the Pipeline

1. Set up `.env` with your MySQL and Kafka configs.
2. Load initial dataset:

```bash
python main_mysql_loader.py
```

3. Start Kafka and Zookeeper.
4. Produce transactions to Kafka:

```bash
python main_kafka_producer.py
```

5. Consume and validate transactions:

```bash
python main_stream.py
```

6. Perform batch updates:

```bash
python main_batch.py
```

7. Apply updates using Spark:

```bash
python main_mysql_updater.py
```

## 📁 Outputs

- `results/stream_transactions.csv` - Transactions after stream validation.
- `results/batch_transactions.csv` - Transactions after batch approval.
- `results/cards_updated.csv` - Updated card balances and limits.
- `results/customers_updated.csv` - Updated customer credit scores.

## 👨‍💻 Author

Atharva Patil
Data Science Graduate Student @ RIT
