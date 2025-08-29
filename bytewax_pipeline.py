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
from mdp_explainer import MDPStreamExplainer
import json
import numpy as np


def json_serialize(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.float32, np.float64, np.int32, np.int64)):
        return obj.item()
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


def step(state: MDPStreamExplainer, record):
    # record is a ConsumerRecord; value is bytes/str
    data = json.loads(record.value)

    # features normalized 0..1 (as στο υπάρχον σου pipeline)
    features = {
        "temperature": data["temperature"] / 30,
        "pressure": data["pressure"] / 1100,
        "humidity": data["humidity"] / 100,
        "wind_speed": data["wind"]["speed"] / 7,
        "cloud_coverage": data["clouds"] / 100,
        "rain": data.get("rain", 0.0) / 500,
        "is_anomaly": data.get("is_anomaly", False)
    }

    # anomaly detection (River HST)
    anomaly_score, is_anomaly = detect_anomaly(features)
    data["anomaly_score"] = anomaly_score
    data["anomaly"] = is_anomaly

    # --- build attributes for MDP (discretize numeric + add categorical context) ---
    raw_numeric = {k: v for k, v in features.items() if k != "is_anomaly"}
    extra_cats = {
        "weather": data.get("weather"),
        "country": data.get("country"),
        "location_name": data.get("location_name"),
    }
    attrs = state.to_attributes(raw_numeric, bins=MDP_NUM_BINS, extra_cats=extra_cats)

    # update explainer
    state.observe(attrs, bool(is_anomaly))

    # maybe emit explanations
    msgs = []
    exps = state.maybe_emit()
    if exps:
        payload = {
            "window": {
                "size_events": MDP_WINDOW_MAX_EVENTS,
            },
            "explanations": [
                {
                    "items": list(e.items),
                    "risk_ratio": e.risk_ratio,
                    "support_outlier": e.support_outlier,
                    "support_inlier": e.support_inlier,
                    "ao": e.ao, "ai": e.ai, "bo": e.bo, "bi": e.bi,
                    "k": e.k,
                }
                for e in exps
            ],
        }
        msgs.append(
            KafkaSinkMessage(record.key, json.dumps(payload, default=json_serialize), topic=EXPLANATIONS_TOPIC)
        )

    # always forward enriched anomaly record to OUT_TOPIC
    msgs.append(
        KafkaSinkMessage(record.key, json.dumps(data, default=json_serialize), topic=OUT_TOPIC)
    )

    for m in msgs:
        yield m

    return state


# ---- Build flow ----
flow = Dataflow("weather_with_mdp")
kinp = op.input("input", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))

# optional: log
# logged = op.map("log", kinp, lambda x: (print(f"Consumed: Key={x.key}, Value={x.value}"), x)[1])
# use kinp directly to avoid extra prints

# stateful step so ο explainer κρατάει μετρητές μεταξύ events
ex_out = op.stateful_map("mdp_step", kinp, build_explainer, step)

# Single Kafka sink; per-message topic override via KafkaSinkMessage(topic=...)
op.output("kafka-out", ex_out, KafkaSink([KAFKA_BROKER], OUT_TOPIC))