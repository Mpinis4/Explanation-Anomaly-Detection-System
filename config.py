### **`config.py`**
# Configuration for Kafka and other settings
import numpy as np
from river_anomaly import FEATURE_LIMITS


KAFKA_BROKER = "localhost:38441"
KAFKA_TOPIC = "weather_data"
SHAP_EXPLAINER_BACKGROUND = np.array([[(low + high) / 2 for low, high in FEATURE_LIMITS.values()]])