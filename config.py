import numpy as np
from river_anomaly import FEATURE_LIMITS


KAFKA_BROKER = "localhost:38441"
KAFKA_TOPIC = "weather_data" # input
OUT_TOPIC = "out-topic" # anomalies / enriched
EXPLANATIONS_TOPIC = "explanations-topic" # MDP explanations


# SHAP (unchanged)
SHAP_EXPLAINER_BACKGROUND = np.array([[(low + high) / 2 for low, high in FEATURE_LIMITS.values()]])


# --- MDP (MacroBase-like) configuration ---
# thresholds
MDP_MIN_SUPPORT = 0.05 # min outlier support (relative in a window)
MDP_MIN_RR = 2.0 # min risk ratio
MDP_MAX_K = 3 # max combination size


# streaming windowing
MDP_WINDOW_MAX_EVENTS = 2000 # emit explanations every N events
MDP_WINDOW_MAX_SECONDS = None # or a float in seconds


# AMC sketch (single-attribute counts)
MDP_AMC_EPSILON = 0.001 # ~1/epsilon is stable size
MDP_AMC_STABLE_SIZE = 5000
MDP_DECAY_RATE = 0.0 # 0..1 per window (exponential decay)


# discretization (for numeric features 0..1)
MDP_NUM_BINS = 5