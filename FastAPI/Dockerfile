FROM python:3.10
WORKDIR /app
COPY FastAPI/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt 

COPY Models /app/Models
COPY utils /app/utils
COPY DatabaseCreator /app/DatabaseCreator


ENV PYTHONPATH="/app"

CMD [ "uvicorn", "FastAPI.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "20"]