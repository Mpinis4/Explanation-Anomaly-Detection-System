import requests
from confluent_kafka import Producer
import json
import time
from config import KAFKA_BROKER,KAFKA_TOPIC

# Kafka Producer configuration
config = {
    'bootstrap.servers': KAFKA_BROKER,  # Replace with your Kafka broker address
    'acks': 'all',
}
producer = Producer(config)

# Delivery callback to confirm message delivery
def delivery_callback(err, msg):
    if err:
        print(f"Message failed delivery: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]: {msg.value().decode('utf-8')}")

location = {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194}  # Mild Mediterranean climate
# Kafka topic
topic = KAFKA_TOPIC

if __name__ == "__main__":
    print("Starting real-time weather data producer...")
    try:
        data = {
            "location_name": "San Francisco",
            "weather": "ANOMALY",  # Access the first element of the list
            "weather_description": "ANOMALY CREATION",  # Access the first element of the list
            "temperature": 9.11,
            "pressure": 1020,
            "humidity": 86,
            "wind": {
                        "speed": 1.34,
                        "deg": 121,
                        "gust": 3.47
                    },
            "rain": 0,
            "clouds": 100,
            "country": "US",
            "is_anomaly":True
        }
        
        # Serialize and send to Kafka
        producer.produce(
            topic,
            key=location['name'],
            value=json.dumps(data),
            callback=delivery_callback
        )
        producer.flush()

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Ensure all messages are delivered
        producer.flush()
