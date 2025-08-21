from bytewax.dataflow import Dataflow
from kafka_consumer import kafka_input
from river_anomaly import detect_anomaly
from shap_explainer import compute_shap
import json

from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax import operators as op
from config import KAFKA_BROKER, KAFKA_TOPIC
import numpy as np

def json_serialize(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.float32, np.float64, np.int32, np.int64)):
        return obj.item()  # Convert NumPy scalar to native Python type
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")



def process_data(data):
  
    weather_data = json.loads(data.value)

    # Extract features
    features = {
        "temperature": weather_data["temperature"]/30,
        "pressure": weather_data["pressure"]/1100,
        "humidity": weather_data["humidity"]/100,
        "wind_speed": weather_data["wind"]["speed"]/7,
        "cloud_coverage": weather_data["clouds"]/100,
        "rain": weather_data["rain"]/500,
        "is_anomaly":weather_data["is_anomaly"]
    }

    # Detect anomaly
    anomaly_score, is_anomaly = detect_anomaly(features)

    # # Compute SHAP explanation
    # try:
    #     shap_values = compute_shap(features)
    #     if isinstance(shap_values, np.ndarray):
    #         shap_values = shap_values.tolist()
    # except Exception as e:
    #     print(f"Error computing SHAP: {e}")
    #     shap_values = None

    # Add results to weather data
    weather_data["anomaly_score"] = anomaly_score
    weather_data["anomaly"] = is_anomaly
    # weather_data["shap_explanation"] = shap_values

    return KafkaSinkMessage(data.key, json.dumps(weather_data, default=json_serialize))

def output_data(data):
    # Output the processed data
    print("Processed:", data)

try:
    # Bytewax dataflow pipeline.

    flow = Dataflow("weather")

    # Clarify the input for the bytewax dataflow
    kinp=op.input("input",flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))

    # Log each consumed message
    logged = op.map("log", kinp, lambda x: (print(f"Consumed: Key={x.key}, Value={x.value}"), x)[1])

    # Call the process_data function on the consumed data
    processed = op.map("map", logged,process_data)

    op.output("kafka-out", processed, KafkaSink([KAFKA_BROKER], "out-topic"))
    
except KeyboardInterrupt:
    print("Stopping consumption...")