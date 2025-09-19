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
            "location_name": "Athens",
            "weather": "Sunny",  # Access the first element of the list
            "weather_description": "rainny clouds",  # Access the first element of the list
            "temperature": 29.62,
            "pressure": 999,
            "humidity": 90,
            "wind": {
                        "speed": 5.36,
                        "deg": 360,
                        "gust": 7.6
                    },
            "rain": 7.0,
            "clouds": 90,
            "country": "Greece",
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
