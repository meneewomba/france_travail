
FROM python:3.10-slim

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    # Install the dependencies 
    pip install --no-cache-dir -r requirements.txt


COPY ./DatabaseCreator /app/DatabaseCreator

