from fastapi import FastAPI, HTTPException
from data_models.occupancy_input_model import OccupancyServiceInputModel
from services.occupancy_calc_service import Occupancy_Calculation_Service

app = FastAPI()
occu_service = Occupancy_Calculation_Service()

@app.post("/predict/")
async def predict_occupancy(occupancy_input: OccupancyServiceInputModel) -> dict:
    """
    Endpoint to predict the occupancy of the given room data.

    Args:
        occupancy_input (OccupancyServiceInputModel): The input model containing the text to be analyzed.

    Returns:
        dict: A dictionary containing the prediction results.

    Raises:
        HTTPException: If an error occurs during prediction.
    """
    try:
        return occu_service.predict(occupancy_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))