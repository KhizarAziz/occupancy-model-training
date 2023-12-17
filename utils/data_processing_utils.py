import numpy as np
from datetime import datetime
from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA


def process_date(date : datetime) -> (int , int):
    """
    Extracts hour and DOW from date.

    Args:
        date (datetime): The input date.

    Returns:
        tuple: extracted hour and DOW.
    """
    # Extract hour
    hour = date.hour

    # Extract day of the week (Monday=0, Sunday=6)
    day_of_week = date.weekday()

    return (hour, day_of_week)

def scale_features(features: np.ndarray, scaler: RobustScaler) -> np.ndarray:
    """
    Scales the given features using the provided scaler.

    Args:
        features (numpy array): The features to be scaled.
        scaler (RobustScaler object): The scaler to use for scaling.

    Returns:
        numpy array: Scaled features.
    """
    return scaler.transform(features.astype(float))

def apply_pca(features: np.ndarray, pca: PCA) -> np.ndarray:
    """
    Applies PCA transformation to the features.

    Args:
        features (numpy array): The features to be transformed.
        pca (PCA object): The PCA object to use for transformation.

    Returns:
        numpy array: PCA-transformed features.
    """
    return pca.transform(features.astype(float))