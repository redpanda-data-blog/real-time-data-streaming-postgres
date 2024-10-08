import random
from datetime import datetime
import time
import json

from kafka import KafkaProducer

from config import producer_config


from datetime import datetime
import random


def on_success(metadata):
    print(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")


def on_error(e):
    print(f"Error sending message: {e}")


def generate_inventory_update():
    """Generate a random inventory update"""
    store_id = random.randint(1, 10)
    product_id = random.randint(1, 200)
    product_name = f"Product-{product_id}"
    current_stock = random.randint(1, 100)

    # Construct the JSON payload
    inventory_update = {
        "store_id": store_id,
        "product_id": product_id,
        "product_name": product_name,
        "current_stock": current_stock,
        "last_updated": datetime.now().isoformat() + "Z",
    }
    print(f"Inventory update message is: {inventory_update}")

    return inventory_update


def send_updates():
    """Send inventory updates every 5 seconds"""
    topic = "inventory-updates"

    while True:
        inventory_update = generate_inventory_update()
        # Produce message to Kafka topic
        future = producer.send(
            topic, value=json.dumps(inventory_update).encode("utf-8")
        )
        future.add_callback(on_success)
        future.add_errback(on_error)
        time.sleep(5)


if __name__ == "__main__":
    producer = KafkaProducer(**producer_config)
    send_updates()
    producer.flush()
    producer.close()
