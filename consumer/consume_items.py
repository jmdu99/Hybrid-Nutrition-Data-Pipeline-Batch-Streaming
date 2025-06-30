import os
import time
import requests
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
TOPIC = "foods_topic"

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nutrition_ks")
TABLE = os.environ.get("CASSANDRA_TABLE", "items_raw")

NUTRITION_API_URL = "https://api.api-ninjas.com/v1/nutrition"
API_NINJAS_KEY = os.environ.get("API_NINJAS_KEY")


def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                auto_offset_reset="earliest",
                group_id="nutrition_consumer_group",
                enable_auto_commit=False,
            )
            print("Connected to Kafka broker.")
            return consumer
        except Exception as e:
            print(f"Error connecting to Kafka broker: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def setup_cassandra():
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
                WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
            """
            )
            session.set_keyspace(KEYSPACE)
            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    item_name text PRIMARY KEY,
                    ingestion_ts timestamp,
                    data text
                )
            """
            )
            print("Connected to Cassandra.")
            return session
        except Exception as e:
            print(f"Cassandra not available. Retrying in 5 seconds... {e}")
            time.sleep(5)


def call_nutrition_api(query):
    params = {"query": query}
    headers = {"X-Api-Key": API_NINJAS_KEY}
    try:
        response = requests.get(
            NUTRITION_API_URL, params=params, headers=headers, timeout=10
        )
        if response.status_code == 200:
            return response.text
        else:
            print("Nutrition API error:", response.status_code, response.text)
            return None
    except Exception as e:
        print("Error calling Nutrition API:", e)
        return None


def on_commit_callback(offsets, exception):
    if exception:
        print(f"Async commit error: {exception}", flush=True)
    else:
        print(f"Offsets successfully committed: {offsets}", flush=True)


def main():
    session = setup_cassandra()
    consumer = create_kafka_consumer()
    try:
        while True:
            for msg in consumer:
                try:
                    item = msg.value.decode("utf-8")
                    nutrition_data = call_nutrition_api(item)
                    if nutrition_data is None or nutrition_data.strip() == "[]":
                        print("No nutrition data for item:", item)
                    else:
                        query = f"INSERT INTO {TABLE} (item_name, ingestion_ts, data) VALUES (%s, %s, %s)"
                        session.execute(query, (item, datetime.now(), nutrition_data))
                        print("Inserted item:", item)

                    # perform async commit after processing
                    consumer.commit_async(callback=on_commit_callback)
                    time.sleep(15)  # 1 request every 15 seconds
                except Exception as inner_e:
                    print("Error processing message:", inner_e)
                    continue
    except Exception as outer_e:
        print("Error in consumer loop:", outer_e)
        time.sleep(5)
    finally:
        # Final synchronous commit before shutdown
        try:
            consumer.commit()
            print("Final synchronous commit completed.", flush=True)
        except Exception as e:
            print(f"Final commit error: {e}", flush=True)
        consumer.close()
        print("Consumer closed.", flush=True)


if __name__ == "__main__":
    main()
