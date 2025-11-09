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
    # Mediterranean
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194, "country": "US"},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964, "country": "IT"},
    {"name": "Santiago", "lat": -33.4489, "lon": -70.6693, "country": "CL"},
    {"name": "Cape Town", "lat": -33.9249, "lon": 18.4241, "country": "ZA"},

    # # Humid Subtropical
    # {"name": "New York", "lat": 40.7128, "lon": -74.0060, "country": "US"},
    # {"name": "Buenos Aires", "lat": -34.6037, "lon": -58.3816, "country": "AR"},
    # {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917, "country": "JP"},
    # {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "country": "IN"},

    # # Tropical
    # {"name": "Miami", "lat": 25.7617, "lon": -80.1918, "country": "US"},
    # {"name": "Singapore", "lat": 1.3521, "lon": 103.8198, "country": "SG"},
    # {"name": "Nairobi", "lat": -1.2864, "lon": 36.8172, "country": "KE"},

    # # Marine/Temperate
    # {"name": "Seattle", "lat": 47.6062, "lon": -122.3321, "country": "US"},
    # {"name": "London", "lat": 51.5074, "lon": -0.1278, "country": "GB"},
    # {"name": "Berlin", "lat": 52.5200, "lon": 13.4050, "country": "DE"},
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
