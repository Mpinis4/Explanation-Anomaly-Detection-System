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
        # imitate OpenWeather API's message
        anomaly_payload = {
            "weather": [
                {"main": "Extreme", "description": "tornado"}
            ],
            "main": {
                "temp": 8,
                "pressure": 1010,
                "humidity": 47 
            },
            "wind": {
                "speed": 6.0,
                "deg": 180
            },
            "clouds": {"all": 48},
            "rain": {"1h": 4},
            "sys": {"country": "IT"},

            "location_name": "Rome",
            "is_anomaly": True
        }
        
        # Serialize and send to Kafka
        producer.produce(
            topic,
            key=location['name'],
            value=json.dumps(anomaly_payload),
            callback=delivery_callback
        )
        producer.flush()

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Ensure all messages are delivered
        producer.flush()
