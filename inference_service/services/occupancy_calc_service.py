import joblib
from config import config
import numpy as np
from utils.data_processing_utils import process_date, scale_features, apply_pca
from data_models.occupancy_input_model import OccupancyServiceInputModel

class Occupancy_Calculation_Service:
    """
    Service class for smart-building related operations.
    """

    def __init__(self):
        """
        Initializes the occupancy detection service with necessary models.
        """
        self.model = joblib.load(config.ML_MODEL)
        self.scaler = joblib.load(config.SCALER)
        self.pca = joblib.load(config.PCA_MODEL)


    def predict(self, occupancy_input: OccupancyServiceInputModel) -> dict:
        """
        Predicts the occupancy of a given room conditions.

        Args:
            occupancy_input (OccupancyServiceInputModel): The input data model for room conditions.

        Returns:
            dict: A dictionary containing the occupancy probability distribution for the room.
        """
        
        hour, day_of_week = process_date(occupancy_input.date)

        # Prepare feature array
        feature_array = np.array([[occupancy_input.temperature, occupancy_input.humidity, occupancy_input.light, occupancy_input.CO2, occupancy_input.humidity_ratio, hour]])

        # Scale features
        scaled_features = scale_features(feature_array, self.scaler)

        # Adding the day_of_week to scaled feature set
        complete_features = np.append(scaled_features, [[day_of_week]], axis=1)

        # Apply PCA
        pca_features = apply_pca(complete_features, self.pca)

        # Make prediction
        prediction_probabilities = self.model.predict_proba(pca_features)

        # Format probabilities as a dictionary
        probability_dict = {
            "not_occupied": prediction_probabilities[0,0],
            "occupied": prediction_probabilities[0,1]
        }

        return probability_dict