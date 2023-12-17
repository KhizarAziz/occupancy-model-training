from pydantic import BaseModel
from datetime import datetime

class OccupancyServiceInputModel(BaseModel):
    """
    Pydantic model representing the text input for prediction.
    """
    date : datetime  # Using datetime field type for automatic parsing and validation
    temperature : float
    humidity : float
    light : float
    CO2 : float
    humidity_ratio : float