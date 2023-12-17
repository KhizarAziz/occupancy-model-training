# FastApi Boilerplate code to serve smart building ML model
This is a POC FastAPI application that calculates occupancy status of a room based on environmental values (light, temperature, humidty, CO2 etc).

## Setup & Run

To get started, make sure you have Docker installed on your system. Then, build and run the API using the following commands:

First, clone the repository to your local machine:

```bash
git clone https://github.com/KhizarAziz/room-occupancy-detection-service.git
cd room-occupancy-detection-service
```

Build the Docker image using the following command:
```bash
docker build -t occupancy-docker-image .
```

Run the Docker container with:

```bash
docker run -d -p 5000:5000 occupancy-container
```


## Using the API
As, occupancy prediction endpoint is post because it requires a payload, therefor cannot be directly accesssed from the browser using URL. An alternative way to test the service is to utilize fastapi docs. To use this, you can paste the following in your browser and test the service using UI,

```plaintext
http://127.0.0.1:5000/docs
```


## Testing

To run the tests, first identify your container ID using `docker ps`, and then execute pytest within the container:

```bash
docker ps  # Get the container ID.
docker exec -it <container_id> pytest /code/tests
```

Replace `<container_id>` with the actual ID of your container.
