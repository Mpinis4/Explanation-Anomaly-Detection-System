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


def step(state: MDPStreamExplainer, values):
    if state is None:
        state = build_explainer()

    if isinstance(values, bytes):
        values = values.decode()

    data = json.loads(values)

    features = {
        "temperature": data["temperature"] / 30,
        "pressure": data["pressure"] / 1100,
        "humidity": data["humidity"] / 100,
        "wind_speed": data["wind"]["speed"] / 7,
        "cloud_coverage": data["clouds"] / 100,
        "rain": data.get("rain", 0.0) / 500,
        "is_anomaly": data.get("is_anomaly", False),
    }

    anomaly_score, is_anomaly = detect_anomaly(features)
    data["anomaly_score"] = anomaly_score
    data["anomaly"] = is_anomaly

    raw_numeric = {k: v for k, v in features.items() if k != "is_anomaly"}
    extra_cats = {
        "weather": data.get("weather"),
        "country": data.get("country"),
        "location_name": data.get("location_name"),
    }
    attrs = state.to_attributes(raw_numeric, bins=MDP_NUM_BINS, extra_cats=extra_cats)

    state.observe(attrs, bool(is_anomaly))

    msgs = []
    exps = state.maybe_emit()
    if exps:
        print("----------inside if-------")
        payload = {
            "window": {"size_events": MDP_WINDOW_MAX_EVENTS},
            "explanations": [e.__dict__ for e in exps],
        }
        print("before KAFKA SINK MESSAGE 1")
        msgs.append(
            KafkaSinkMessage(
                key=data.get("location_name", "").encode(),
                value=json.dumps(payload, default=json_serialize).encode(),
                topic=EXPLANATIONS_TOPIC,
            )
        )
        print("----------SURPRISE IT DIDNT BUG-------")
        print("AFTER KAFKA SINK MESSAGE 1")
    print("before KAFKA SINK MESSAGE 2")
    msgs.append(
        KafkaSinkMessage(
            key=data.get("location_name", "").encode(),
            value=json.dumps(data, default=json_serialize).encode(),
            topic=OUT_TOPIC,
        )
    )

    return state, msgs


flow = Dataflow("weather_with_mdp")
kinp = op.input("input", flow, KafkaSource([KAFKA_BROKER], [KAFKA_TOPIC]))

keyed = op.map("to_key_value", kinp, lambda msg: (msg.key.decode() if msg.key else None, msg.value))

ex_out = op.stateful_flat_map("mdp_step", keyed, step)

op.output("kafka-out", ex_out, KafkaSink([KAFKA_BROKER],OUT_TOPIC))
