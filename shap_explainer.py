import shap
import numpy as np
from config import SHAP_EXPLAINER_BACKGROUND

# Ensure SHAP_EXPLAINER_BACKGROUND is a valid 2D NumPy array
if not isinstance(SHAP_EXPLAINER_BACKGROUND, np.ndarray):
    raise ValueError("SHAP_EXPLAINER_BACKGROUND must be a 2D NumPy array.")
if SHAP_EXPLAINER_BACKGROUND.ndim != 2:
    raise ValueError("SHAP_EXPLAINER_BACKGROUND must have 2 dimensions.")

# Initialize SHAP KernelExplainer
try:
    explainer = shap.KernelExplainer(
        model=lambda x: np.zeros(x.shape[0]),  # Dummy model returning zeros
        data=SHAP_EXPLAINER_BACKGROUND
    )
except Exception as e:
    raise RuntimeError(f"Failed to initialize SHAP explainer: {e}")

def compute_shap(features):
    """
    Compute SHAP values for the given feature set.
    
    Parameters:
        features (dict): A dictionary of feature values.
    
    Returns:
        list: SHAP values for each feature.
    """
    try:
        # Ensure feature values are valid and in proper format
        feature_values = np.array([list(features.values())])  # Convert to 2D NumPy array

        if feature_values.ndim != 2 or feature_values.shape[1] != SHAP_EXPLAINER_BACKGROUND.shape[1]:
            raise ValueError(f"Feature set dimensions {feature_values.shape} do not match background {SHAP_EXPLAINER_BACKGROUND.shape}.")

        # Compute SHAP values
        shap_values = explainer.shap_values(feature_values)
        return shap_values[0]  # Return the first row of SHAP values
    except ValueError as ve:
        print(f"ValueError in compute_shap: {ve}")
        return None
    except Exception as e:
        print(f"Error in compute_shap: {e}")
        return None
