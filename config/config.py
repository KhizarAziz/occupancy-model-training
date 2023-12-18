import os
ML_MODEL = "ml_models/RandomForest.joblib"
PCA_MODEL = "ml_models/pca_3_components.joblib"
SCALER = "ml_models/robust_scaler.joblib"



DATA_BUCKET = os.getenv('DATA_BUCKET')
DATA_FOLDER_IN_S3 = "occupancy_data"
CURRENT_DATA_STATS = 'current_stats.json'