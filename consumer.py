import json
import os
import time

import mysql.connector
from kafka import KafkaConsumer


# -----------------------------
# Configuration
# -----------------------------
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "retail-transactions")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "assignment3-mysql-consumer")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "retail_streaming")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")


def create_mysql_connection():
    """Create a connection to the MySQL retail_streaming database."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def create_consumer():
    """Create a Kafka consumer for the retail-transactions topic."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def insert_transaction(cursor, transaction):
    """Insert one Kafka transaction into MySQL."""
    query = """
        INSERT INTO transactions
        (
            transaction_id,
            timestamp,
            store_id,
            product_id,
            category,
            quantity,
            unit_price,
            total_amount,
            payment_method
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            store_id = VALUES(store_id),
            product_id = VALUES(product_id),
            category = VALUES(category),
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price),
            total_amount = VALUES(total_amount),
            payment_method = VALUES(payment_method)
    """

    values = (
        transaction["transaction_id"],
        transaction["timestamp"],
        transaction["store_id"],
        transaction["product_id"],
        transaction["category"],
        transaction["quantity"],
        transaction["unit_price"],
        transaction["total_amount"],
        transaction["payment_method"],
    )

    cursor.execute(query, values)


def main():
    mysql_connection = None
    consumer = None

    try:
        print("Connecting to MySQL...")
        mysql_connection = create_mysql_connection()
        cursor = mysql_connection.cursor()
        print("MySQL connection successful.")

        print(f"Connecting to Kafka: {KAFKA_SERVER}")
        consumer = create_consumer()
        print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
        print("Starting Kafka → MySQL data transfer...\n")

        processed = 0

        for message in consumer:
            transaction = message.value

            try:
                insert_transaction(cursor, transaction)
                mysql_connection.commit()

                processed += 1

                print(
                    f"{processed}/100 | "
                    f"{transaction['transaction_id']} | "
                    f"₹{transaction['total_amount']}"
                )

                # The Assignment 2 producer sends 100 records.
                if processed >= 100:
                    break

            except KeyError as error:
                mysql_connection.rollback()
                print(f"Invalid transaction data. Missing field: {error}")

            except mysql.connector.Error as error:
                mysql_connection.rollback()
                print(f"MySQL insert error: {error}")

        print(f"\nSuccessfully transferred {processed} Kafka messages to MySQL.")
        print("Consumer completed.")

    except mysql.connector.Error as error:
        print(f"MySQL connection error: {error}")

    except Exception as error:
        print(f"Consumer error: {error}")

    finally:
        if consumer is not None:
            consumer.close()

        if mysql_connection is not None and mysql_connection.is_connected():
            mysql_connection.close()


if __name__ == "__main__":
    main()
