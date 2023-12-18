import pandas as pd
import numpy as np
import config
from data_loaders.s3_data_loader import S3DataLoader

class OccupancyDataMonitor:
    def __init__(self):
        self.data_loader = S3DataLoader(config.S3_BUCKET_NAME, config.DATA_FOLDER_IN_S3)

    def analyze_new_data(self):
        # fetch current data stats
        baseline_data_stats = self.data_loader.read_json_to_dict(config.CURRENT_DATA_STATS)

        # fetch new data
        all_data_df = self.data_loader.read_all_txt_to_df()

        # calculate stats
        current_stats = self.calculate_statistics(all_data_df)

        # detect and notify drift
        if self.detect_drift(current_stats,baseline_data_stats):
            self.alert_drift(drift_info=current_stats)

    def calculate_statistics(self, data):
        # Example: Calculate mean and standard deviation for key features
        stats = data[['Temperature', 'Humidity', 'Light', 'CO2', 'HumidityRatio']].agg(['mean', 'std']).to_dict()
        return stats

    def detect_drift(self, current_stats):
        # Basic drift detection: check if current mean/std deviates significantly from historical
        for feature in current_stats:
            current_mean, current_std = current_stats[feature]['mean'], current_stats[feature]['std']
            historical_mean, historical_std = self.historical_stats[feature]['mean'], self.historical_stats[feature]['std']
            
            threshold_mean = (historical_mean * 10/100) # 10% drift from the data's mean
            threshold_std = (historical_std * 10/100) # 10% drift from the data's std

            if np.abs(current_mean - historical_mean) > threshold_mean or np.abs(current_std - historical_std) > threshold_std:
                return True
        return False

    def alert_drift(self, drift_info):
        # Simple print statement for demonstration, replace with actual alerting mechanism
        print(f"Drift detected: {drift_info}")


if __name__ == "__main__":

    monitor = OccupancyDataMonitor()
    monitor.analyze_new_data()
