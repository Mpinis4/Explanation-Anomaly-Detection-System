from confluent_kafka import Producer
import json, time
from config import KAFKA_BROKER, KAFKA_TOPIC

# Kafka Producer configuration
config = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
}
producer = Producer(config)

def delivery_callback(err, msg):
    if err:
        print(f"Message failed delivery: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def send_event(data):
    producer.produce(
        KAFKA_TOPIC,
        key=data["location_name"],
        value=json.dumps(data),
        callback=delivery_callback
    )
    producer.flush()

if __name__ == "__main__":
    print("🚀 Sending test events...")

    # 10 normal events
    for i in range(10):
        normal_event = {
            "location_name": "San Francisco",
            "weather": "Clear",
            "weather_description": "sunny",
            "temperature": 20 + i * 0.1,
            "pressure": 1015,
            "humidity": 50,
            "wind": {"speed": 2.0, "deg": 180},
            "rain": 0,
            "clouds": 10,
            "country": "US",
            "is_anomaly": False
        }
        send_event(normal_event)
        time.sleep(0.2)

    # 3 anomaly events with same attribute (weather="ANOMALY")
    for i in range(3):
        anomaly_event = {
            "location_name": "San Francisco",
            "weather": "Clear",
            "weather_description": "forced anomaly",
            "temperature": 500,
            "pressure": 990,
            "humidity": 90,
            "wind": {"speed": 6.0, "deg": 270},
            "rain": 100,
            "clouds": 100,
            "country": "US",
            "is_anomaly": True
        }
        send_event(anomaly_event)
        time.sleep(0.2)

    print("Done sending events")
