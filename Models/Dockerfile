
FROM python:3.10


WORKDIR /app


COPY Models/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt 
   

COPY utils /app/utils
COPY DatabaseCreator /app/DatabaseCreator



ENV PYTHONPATH="/app"


CMD ["mlflow","ui","--host", "0.0.0.0", "--port", "5000"]