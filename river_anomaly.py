from river import anomaly
from river import metrics
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RiverAnomaly")

# Define feature limits based on typical weather data ranges
FEATURE_LIMITS = {
    "temperature": (0.0, 1.0), 
    "pressure": (0.0, 1.0),
    "humidity": (0.0, 1.0),
    "wind_speed": (0.0, 1.0),
    "cloud_coverage": (0.0, 1.00),
    "rain": (0.0, 1.0)
}

# Initialize the River model with limits and performance tracker
global river_model, anomaly_metric
river_model = anomaly.HalfSpaceTrees(
    seed=42,
    limits=FEATURE_LIMITS,
    n_trees=50, # the number of trees
    height=10  # tree depth for finer granularity
)
anomaly_metric = metrics.ROCAUC()  # Track model performance

# Default anomaly score threshold
DEFAULT_THRESHOLD = 0.9

def detect_anomaly(features, true_label=False, threshold=DEFAULT_THRESHOLD):
    try:
        # Ensure feature values are valid (numeric)
        features = {key: float(value) for key, value in features.items()}

        #logger.info(f"Input features: {features}")
        # Compute the anomaly score
        anomaly_score = river_model.score_one(features)

        # Update the model with the current feature set
        river_model.learn_one(features)

        # Determine if this is an anomaly
        is_anomaly = anomaly_score > threshold
        
        anomaly_metric.update(true_label, is_anomaly)

        # evaluate_model()

        # logger.info(f"Anomaly Score: {anomaly_score}, Detected: {is_anomaly}")
        return anomaly_score, is_anomaly

    except ValueError as ve:
        logger.error(f"Invalid feature value: {ve}")
        return None, False
    except Exception as e:
        logger.error(f"Error in detect_anomaly: {e}")
        return None, False  # Gracefully handle other unexpected errors

def evaluate_model():
    """
    Get the current performance of the anomaly detection model.

    Returns:
        dict: A dictionary containing model evaluation metrics.
    """
    try:
        logger.info(f"Model Evaluation - ROC AUC: {anomaly_metric}")
        return {"roc_auc": anomaly_metric}
    except Exception as e:
        logger.error(f"Error during model evaluation: {e}")
        return {"roc_auc": None}

def reset_model():
    """
    Reset the anomaly detection model and metrics for re-initialization.
    """
    global river_model, anomaly_metric
    river_model = anomaly.HalfSpaceTrees(
        seed=42,
        limits=FEATURE_LIMITS,
        n_trees=50, # the number of trees
        height=10  # tree depth for finer 
        
    )
    anomaly_metric = metrics.ROCAUC()
    logger.info("Anomaly detection model and metrics have been reset.")
