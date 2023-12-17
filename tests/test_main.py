import pytest
from fastapi.testclient import TestClient
from app.main import app
from data_models.occupancy_input_model import OccupancyServiceInputModel

# Utilize a fixture for the TestClient
@pytest.fixture(scope="module")
def test_client():
    with TestClient(app) as client:
        yield client

def test_predict_occupancy(test_client):
    """
    Test the occupancy prediction for known inputs.
    
    Args:
        test_client (TestClient): Fixture for creating a test client for the API.
    """

    # Sample input data
    test_input = {
        "date": "2015-02-02 14:25:00",
        "temperature": 23.7300,
        "humidity": 26.290,
        "light": 536.333333,
        "CO2": 798.000000,
        "humidity_ratio": 0.004776
    }

    # Expected output format (modify as per your model's output)
    expected_output = {
        "not_occupied": 0.1666,  # Replace with expected values
        "occupied": 0.833
    }

    # Make the POST request to the FastAPI endpoint
    response = test_client.post("/predict/", json=test_input)

    # Check if the status code is 200 OK
    assert response.status_code == 200, f"Failed for input: {test_input}"

    # Check if the JSON response structure matches the expected output structure
    response_json = response.json()
    assert all(key in response_json for key in expected_output), f"Response structure mismatch for input: {test_input}"


    # Check specific expected results (modify as per your model's expected behavior)
    absolute_error_margin = 0.1
    assert abs(response_json["not_occupied"] - expected_output['not_occupied']) < absolute_error_margin, "Unexpected probability for not_occupied"
    assert abs(response_json["occupied"] - expected_output['occupied']) < absolute_error_margin, "Unexpected probability for occupied"
