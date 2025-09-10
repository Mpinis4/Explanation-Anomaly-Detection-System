from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage

from config import (
    KAFKA_BROKER, KAFKA_TOPIC, OUT_TOPIC, EXPLANATIONS_TOPIC,
    MDP_MIN_SUPPORT, MDP_MIN_RR, MDP_MAX_K,
    MDP_WINDOW_MAX_EVENTS, MDP_WINDOW_MAX_SECONDS,
    MDP_AMC_EPSILON, MDP_AMC_STABLE_SIZE, MDP_DECAY_RATE,
    MDP_NUM_BINS,
)
from river_anomaly import detect_anomaly
from mdp_explainer import MDPStreamExplainer, Explanation
import json
import numpy as np


# ---------- Helpers ----------

def json_serialize(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.float32, np.float64, np.int32, np.int64)):
        return obj.item()
    if isinstance(obj, Explanation):
        return obj.__dict__
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def build_explainer():
    return MDPStreamExplainer(
        min_outlier_support=MDP_MIN_SUPPORT,
        min_risk_ratio=MDP_MIN_RR,
        max_len=MDP_MAX_K,
        epsilon_amc=MDP_AMC_EPSILON,
        decay_rate=MDP_DECAY_RATE,
        amc_stable_size=MDP_AMC_STABLE_SIZE,
        window_max_events=MDP_WINDOW_MAX_EVENTS,
        window_max_seconds=MDP_WINDOW_MAX_SECONDS,
    )


# ---------- Stateful step ----------

def step(state: MDPStreamExplainer, value):
    if state is None:
        state = build_explainer()

    # decode -> dict
    if isinstance(value, bytes):
        value = value.decode()
    data = json.loads(value)

    # Normalize features
    features = {
        "temperature": data.get("temperature", 0) / 30,
        "pressure": data.get("pressure", 0) / 1100,
        "humidity": data.get("humidity", 0) / 100,
        "wind_speed": data.get("wind", {}).get("speed", 0) / 7,
        "cloud_coverage": data.get("clouds", 0) / 100,
        "rain": data.get("rain", 0.0) / 500,
        "is_anomaly": data.get("is_anomaly"),
    }

    # Detect anomalies
    anomaly_score, is_anomaly = detect_anomaly(features)
    data["anomaly_score"] = anomaly_score
    data["anomaly"] = is_anomaly

    # Convert to explainer attributes
    raw_numeric = {k: v for k, v in features.items() if k != is_anomaly}
    extra_cats = {
        "weather": data.get("weather"),
        "country": data.get("country"),
        "location_name": data.get("location_name"),
    }
    attrs = state.to_attributes(raw_numeric, bins=MDP_NUM_BINS, extra_cats=extra_cats)
    print(">>> ATTRS:", attrs, "is_anomaly:", is_anomaly)
    # Update explainer state
    state.observe(attrs, data.get("anomaly", False))

    # Build Kafka messages
    msgs = []
    location_name = data.get("location_name", "")

    msgs.append(
        KafkaSinkMessage(
            key=location_name.encode(),
            value=json.dumps(data, default=json_serialize).encode(),
            topic=OUT_TOPIC,
        )
    )

    exps = state.maybe_emit()
    print(">>> maybe_emit returned:", exps)   # <--- DEBUG
    if exps:
        payload = {
            "window": {"size_events": MDP_WINDOW_MAX_EVENTS},
            "explanations": [e.__dict__ for e in exps],
        }
        print(">>> EXPLANATIONS PAYLOAD:", payload)   # <--- DEBUG
        msgs.append(
            KafkaSinkMessage(
                key=location_name.encode(),
                value=json.dumps(payload, default=json_serialize).encode(),
                topic=EXPLANATIONS_TOPIC,
            )
        )

    return state, msgs


# ---------- Flow definition ----------

flow = Dataflow("weather_with_mdp")

# Kafka input -> decode into (key, value)
kinp = op.input("input", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))
keyed = op.map("to_key_value", kinp, lambda msg: (msg.key.decode() if msg.key else None, msg.value))

# Run stateful explainer
ex_out = op.stateful_flat_map("mdp_step", keyed, step)

# Drop the stateful key, keep only KafkaSinkMessages
just_msgs = op.map("drop_key", ex_out, lambda kv: kv[1])

# Debug inspect
# def debug_inspect(step_id, item):
#     print(f"[{step_id}] DEBUG TYPE: {type(item)} -> {item}")
#     return item

# debugged = op.inspect("debug-out", just_msgs, debug_inspect)

# Filter out only the explanations (topic == EXPLANATIONS_TOPIC)
explanations = op.filter("only_explanations", just_msgs, 
                         lambda msg: msg.topic == EXPLANATIONS_TOPIC)

# Inspect them
op.inspect("explanations-debug", explanations, 
           lambda sid, item: print(f"[{sid}] {item.value.decode()}") or item)

# Output to Kafka
op.output("kafka-out", just_msgs, KafkaSink([KAFKA_BROKER], topic=None))
