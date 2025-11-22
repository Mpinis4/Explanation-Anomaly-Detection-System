# --- Kafka configuration ---
KAFKA_BROKER = "localhost:38441"
KAFKA_TOPIC = "weather_data" # input
OUT_TOPIC = "out_topic" # anomalies / enriched
EXPLANATIONS_TOPIC = "explanations_topic" # explanations


# --- MDP like configuration ---
# thresholds
MDP_MIN_SUPPORT = 0.05 # min outlier support (relative in a window)
MDP_MIN_RR = 3.0 # min risk ratio
MDP_MAX_K = 9 # max combination size

# streaming windowing
MDP_WINDOW_MAX_EVENTS = 100 # emit explanations every N events
MDP_SLIDE_STEP=20

# AMC sketch (single-attribute counts)
MDP_AMC_STABLE_SIZE = 5000
MDP_DECAY_RATE = 0.5 #  per window (exponential decay)

# discretization 
MDP_NUM_BINS = 5