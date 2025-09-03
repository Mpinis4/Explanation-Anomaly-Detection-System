from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage

from config import KAFKA_BROKER, KAFKA_TOPIC
from river_anomaly import detect_anomaly
import json

# -------------------------
# Helper function για flat_map step
# -------------------------
def step(state, kv):
    """
    kv: tuple (key, value) από upstream keyed stream
    """
    key, record_value = kv

    # record_value μπορεί να είναι bytes ή str
    if isinstance(record_value, bytes):
        record_value = record_value.decode()
    
    data = json.loads(record_value)

    # Normalize features
    features = {
        "temperature": data["temperature"] / 30,
        "pressure": data["pressure"] / 1100,
        "humidity": data["humidity"] / 100,
        "wind_speed": data["wind"]["speed"] / 7,
        "cloud_coverage": data["clouds"] / 100,
        "rain": data.get("rain", 0.0) / 500,
    }

    # Anomaly detection
    score, is_anomaly = detect_anomaly(features)
    data["anomaly_score"] = score
    data["is_anomaly"] = is_anomaly

    # Δημιουργία Kafka messages
    key_bytes = data.get("location_name").encode() if data.get("location_name") else None
    value_bytes = json.dumps(data).encode()

    # Message προς Bytewax anomalies topic
    msg_anomaly = KafkaSinkMessage(
        key=key_bytes,
        value=value_bytes,
        topic="anomalies"
    )

    # Message προς MacroBase input topic
    msg_macrobase = KafkaSinkMessage(
        key=key_bytes,
        value=value_bytes,
        topic="macrobase_input"
    )

    return None, [msg_anomaly, msg_macrobase]

# -------------------------
# Build Bytewax dataflow
# -------------------------
flow = Dataflow("anomaly_pipeline")

# Input από Kafka topic
kinp = op.input("in", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))

# Keyed stream για flat_map
keyed = op.map("to_kv", kinp, lambda msg: (
    msg.key.decode() if msg.key else None,
    msg.value
))

# Flat map για anomaly detection (state όχι απαραίτητο)
ex_out = op.flat_map("detect", keyed, step)

# Output στο Kafka
op.output("out", ex_out, KafkaSink([KAFKA_BROKER], "anomalies"))  # Το KafkaSink θα γράψει όλα τα messages, overrides per-message
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage

from config import KAFKA_BROKER, KAFKA_TOPIC
from river_anomaly import detect_anomaly
import json

# -------------------------
# Helper function για flat_map step
# -------------------------
def step(state, kv):
    """
    kv: tuple (key, value) από upstream keyed stream
    """
    key, record_value = kv

    # record_value μπορεί να είναι bytes ή str
    if isinstance(record_value, bytes):
        record_value = record_value.decode()
    
    data = json.loads(record_value)

    # Normalize features
    features = {
        "temperature": data["temperature"] / 30,
        "pressure": data["pressure"] / 1100,
        "humidity": data["humidity"] / 100,
        "wind_speed": data["wind"]["speed"] / 7,
        "cloud_coverage": data["clouds"] / 100,
        "rain": data.get("rain", 0.0) / 500,
    }

    # Anomaly detection
    score, is_anomaly = detect_anomaly(features)
    data["anomaly_score"] = score
    data["is_anomaly"] = is_anomaly

    # Δημιουργία Kafka messages
    key_bytes = data.get("location_name").encode() if data.get("location_name") else None
    value_bytes = json.dumps(data).encode()

    # Message προς Bytewax anomalies topic
    msg_anomaly = KafkaSinkMessage(
        key=key_bytes,
        value=value_bytes,
        topic="anomalies"
    )

    # Message προς MacroBase input topic
    msg_macrobase = KafkaSinkMessage(
        key=key_bytes,
        value=value_bytes,
        topic="macrobase_input"
    )

    return None, [msg_anomaly, msg_macrobase]

# -------------------------
# Build Bytewax dataflow
# -------------------------
flow = Dataflow("anomaly_pipeline")

# Input από Kafka topic
kinp = op.input("in", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))

# Keyed stream για flat_map
keyed = op.map("to_kv", kinp, lambda msg: (
    msg.key.decode() if msg.key else None,
    msg.value
))

# Flat map για anomaly detection (state όχι απαραίτητο)
ex_out = op.flat_map("detect", keyed, step)

# Output στο Kafka
op.output("out", ex_out, KafkaSink([KAFKA_BROKER], "anomalies"))  # Το KafkaSink θα γράψει όλα τα messages, overrides per-message
