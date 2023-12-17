FROM python:3.8-slim

WORKDIR /code

# Install dependencies
RUN apt-get update && apt-get install -y cron

# Copy the requirements file and install dependencies
COPY ./drift_monitoring/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the application files
COPY ./drift_monitoring/data_monitoring.py /code/data_monitoring.py
COPY ./data_loaders /code/data_loaders
COPY ./config /code/config

# CRON JOB for running the monitoring daily midnight

# Setup cron job | Credits ChatGPT :)
RUN echo "0 0 * * * python /code/data_monitoring.py > /proc/1/fd/1 2>/proc/1/fd/2" | crontab -

# Start cron
CMD ["cron", "-f"]