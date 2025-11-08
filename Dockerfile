FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

# Copy local code to the container image
WORKDIR /app
COPY . ./

# Install production dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the web service on container startup using functions-framework
CMD ["functions-framework", "--target=pubsub_trigger", "--signature-type=cloudevent"]