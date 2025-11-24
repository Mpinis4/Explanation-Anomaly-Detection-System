from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
import time

from config import (
    KAFKA_BROKER, KAFKA_TOPIC, OUT_TOPIC, EXPLANATIONS_TOPIC,
    MDP_MIN_SUPPORT, MDP_MIN_RR, MDP_MAX_K,
    MDP_WINDOW_MAX_EVENTS, MDP_AMC_STABLE_SIZE, MDP_DECAY_RATE,
    MDP_NUM_BINS,MDP_EMIT_AFTER_EVENTS
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
        decay_rate=MDP_DECAY_RATE,
        amc_stable_size=MDP_AMC_STABLE_SIZE,
        window_max_events=MDP_WINDOW_MAX_EVENTS,
        emit_after_events=MDP_EMIT_AFTER_EVENTS
    )


# ---------- Stateful step ----------

def step(state: MDPStreamExplainer, value):
    start_time = time.time()
    if state is None:
        state = build_explainer()

    # decode -> dict
    if isinstance(value, bytes):
        value = value.decode()
    raw = json.loads(value)

    # feature selection
    data = {
        "location_name": raw.get("location_name"),
        "weather": raw.get("weather", [{}])[0].get("main"),
        "weather_description": raw.get("weather", [{}])[0].get("description"),
        "temperature": raw.get("main", {}).get("temp"),
        "pressure": raw.get("main", {}).get("pressure"),
        "humidity": raw.get("main", {}).get("humidity"),
        "wind_speed": raw.get("wind", {}).get("speed"),
        "cloud_coverage": raw.get("clouds", {}).get("all"),
        "rain": raw.get("rain", {}).get("1h"),
        "country": raw.get("sys", {}).get("country"),
        "is_anomaly": raw.get("is_anomaly", False),
    }

    # Normalize features
    features = {
        "temperature": (data.get("temperature") or 0)/30,
        "pressure": (data.get("pressure") or 0)/1100,
        "humidity": (data.get("humidity") or 0)/100,
        "wind_speed": (data.get("wind_speed") or 0)/32,
        "cloud_coverage": (data.get("clouds") or 0)/100,
        "rain": (data.get("rain") or 0)/10,
    }

    is_it_anomaly_label = data.get("is_anomaly", False)
    # Detect anomalies
    t1=time.time()
    anomaly_score, is_anomaly = detect_anomaly(features,true_label=is_it_anomaly_label)
    t2=time.time()
    data["anomaly_score"] = anomaly_score
    data["anomaly"] = is_anomaly
    # Convert to explainer attributes
    extra_cats = {
        "weather": data.get("weather"),
        "country": data.get("country"),
        "location_name": data.get("location_name"),
    }
    attrs = state.to_attributes(features, bins=MDP_NUM_BINS, extra_cats=extra_cats)
    if is_anomaly:
        print(">>> LOCATION NAME:", data.get("location_name"), "ANOMALY:", is_anomaly,"ANOMALY SCORE:",anomaly_score)
    # Update explainer state
    state.observe(attrs, data.get("anomaly", False))
    t3=time.time()
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
    if exps:
        payload = {
            "window": {"size_events": MDP_WINDOW_MAX_EVENTS},
            "explanations": [e.__dict__ for e in exps],
        }
        msgs.append(
            KafkaSinkMessage(
                key=location_name.encode(),
                value=json.dumps(payload, default=json_serialize).encode(),
                topic=EXPLANATIONS_TOPIC,
            )
        )
    end_time = time.time()
    # print(
    #     f"""
    # ---- LATENCY (ms) ----
    # transform : {(t1 - start_time) * 1000:8.2f}
    # anomaly   : {(t2 - t1) * 1000:8.2f}
    # observe   : {(t3 - t2) * 1000:8.2f}
    # emit      : {(end_time - t3) * 1000:8.2f}
    # -----------------------
    # TOTAL     : {(end_time - start_time) * 1000:8.2f}
    # """
    # )
    return state, msgs


# ---------- Flow definition ----------

flow = Dataflow("weather_with_mdp")

# Kafka input -> decode into (key, value)
kinp = op.input("input", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))
keyed = op.map("to_key_value", kinp, lambda msg: ("__global__", msg.value))

ex_out = op.stateful_flat_map("mdp_step", keyed, step)

# Drop the stateful key, keep only KafkaSinkMessages
just_msgs = op.map("drop_key", ex_out, lambda kv: kv[1])

# Filter out only the explanations (topic == EXPLANATIONS_TOPIC)
explanations = op.filter("only_explanations", just_msgs, lambda msg: msg.topic == EXPLANATIONS_TOPIC)

# Output to Kafka
op.output("kafka-out", just_msgs, KafkaSink([KAFKA_BROKER], topic=None))
