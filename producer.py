import requests
from confluent_kafka import Producer
import json
import time
from config import KAFKA_BROKER,KAFKA_TOPIC

# Kafka Producer configuration
config = {
    'bootstrap.servers': KAFKA_BROKER,  # Kafka broker address
    'acks': 'all',
}
producer = Producer(config)
PRODUCER_STATS = {
    "msg_count": 0,
    "last_print": time.time(),
    "start_time": time.time()
}
# Delivery callback to confirm message delivery
def delivery_callback(err, msg):
    if err:
        print(f"Message failed delivery: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]: {msg.value().decode('utf-8')}")

# OpenWeather API configuration
API_KEY = 'bbca1cbe33fadd24c3841f6a8940f27c'
API_URL = 'https://api.openweathermap.org/data/2.5/weather'

# Location with latitude and longitude
locations = [
    # Mediterranean
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194, "country": "US"},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964, "country": "IT"},
    {"name": "Santiago", "lat": -33.4489, "lon": -70.6693, "country": "CL"},
    {"name": "Cape Town", "lat": -33.9249, "lon": 18.4241, "country": "ZA"},

]

# Kafka topic
topic = KAFKA_TOPIC

def fetch_and_send_weather_data(location):
    """Fetch weather data for a single location and send it to Kafka."""
    try:
        params = {
            'lat': location['lat'],
            'lon': location['lon'],
            'appid': API_KEY,
            'units': 'metric'  # 'metric' = Celsius 
        }
        # request data based on parameters
        response = requests.get(API_URL, params=params)
        weather_data = response.json()

        if response.status_code == 200:
            # add metadata
            weather_data["location_name"]=location["name"]
            weather_data["is_anomaly"]=False
            
            # Serialize and send to Kafka 
            producer.produce(
                topic,
                key=location['name'],
                value=json.dumps(weather_data),
                # callback=delivery_callback
            )
            producer.flush()
            PRODUCER_STATS["msg_count"] += 1
            now = time.time()
            if now - PRODUCER_STATS["last_print"] >= 5.0:  # Κάθε 5 δευτερόλεπτα
                rate = PRODUCER_STATS["msg_count"] / (now - PRODUCER_STATS["start_time"])
                print(f"[PRODUCER] Total: {PRODUCER_STATS['msg_count']} | Rate: {rate:.1f} msg/sec")
                PRODUCER_STATS["last_print"] = now

        else:
            print(f"Failed to fetch data for {location['name']}: {weather_data}")
    except Exception as e:
        print(f"Error fetching weather data for {location['name']}: {e}")

if __name__ == "__main__":
    print("Starting real-time weather data producer...")
    try:
        while True:
            # Fetch weather data for all locations in rapid succession
            for location in locations:
                fetch_and_send_weather_data(location)
                # Minimal delay to manage rate limits
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Ensure all messages are delivered
        producer.flush()
