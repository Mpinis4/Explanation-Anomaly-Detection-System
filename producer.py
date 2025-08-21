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
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194},  # Mild Mediterranean climate
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
        response = requests.get(API_URL, params=params)
        weather_data = response.json()

        if response.status_code == 200:
            # extract and add metadata
            data = {
                "location_name": location['name'],
                "weather": weather_data["weather"][0]["main"],  
                "weather_description": weather_data["weather"][0]["description"],
                "temperature": weather_data["main"]["temp"],
                "pressure": weather_data["main"]["pressure"],
                "humidity": weather_data["main"]["humidity"],
                "wind": weather_data["wind"],
                "rain": weather_data.get("rain", {}).get("1h", 0.0),
                "clouds": weather_data["clouds"]["all"],
                "country": weather_data["sys"]["country"],
                "is_anomaly":False # metadata
            }
            
            # Serialize and send to Kafka
            producer.produce(
                topic,
                key=location['name'],
                value=json.dumps(data),
                callback=delivery_callback
            )
            producer.flush()
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
