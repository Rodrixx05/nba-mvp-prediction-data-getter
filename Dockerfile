FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    libgomp1 \
    libpq-dev \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && apt-get clean

WORKDIR /app
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["functions-framework", "--target=pubsub_trigger", "--signature-type=cloudevent"]