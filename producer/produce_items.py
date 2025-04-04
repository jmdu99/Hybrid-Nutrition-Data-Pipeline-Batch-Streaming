import os
import time
import random
import openai
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")
TOPIC = "foods_topic"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

def generate_random_food_item():
    """Calls OpenAI to generate a short food item name."""
    try:
        prompt = "Give me one short name of a known dish or a known food item. Please be original"
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.choices[0].message.content.strip() if response.choices else None
        return text
    except Exception as e:
        print(f"OpenAI error: {e}")
        return None

def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                value_serializer=lambda v: v.encode('utf-8')
            )
            print("Connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print("Kafka broker not available. Waiting 5 seconds to retry...")
            time.sleep(5)

if __name__ == "__main__":
    producer = create_kafka_producer()
    print("Producer started - calling OpenAI for random item names")

    produced_items = set()

    while True:
        item = generate_random_food_item()
        attempts = 0
        while item in produced_items and attempts < 5:
            print(f"Duplicate item '{item}' detected. Generating a new one...")
            item = generate_random_food_item()
            attempts += 1

        if item not in produced_items:
            produced_items.add(item)
            print(f"Producing item: {item}")
            producer.send(TOPIC, value=item)
            producer.flush()
        else:
            print(f"Item '{item}' already produced multiple times, skipping.")

        time.sleep(5)  # Produce cada 5 segundos
