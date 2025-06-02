import json
import logging
import time
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
import mlflow
import numpy as np
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from DatabaseCreator.database import get_db, get_Elasticsearch # Import new database function
from elasticsearch import Elasticsearch
import os
import joblib
import pandas as pd
from utils.preprocessing import get_df
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from prometheus_fastapi_instrumentator import Instrumentator





app = FastAPI()

Instrumentator().instrument(app).expose(app)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, "..", "Models")  # remonte d’un dossier

REPORT_DIR = os.path.join(BASE_DIR, "..", "reports")

min_half_low = joblib.load(os.path.join(MODELS_DIR, "model_min_half_low.pckl"))
min_half_high = joblib.load(os.path.join(MODELS_DIR, "model_min_half_high.pckl"))
max_half_low = joblib.load(os.path.join(MODELS_DIR, "model_max_half_low.pckl"))
max_half_high = joblib.load(os.path.join(MODELS_DIR, "model_max_half_high.pckl"))

model_min = joblib.load(os.path.join(MODELS_DIR, "model_min.pckl"))
model_max = joblib.load(os.path.join(MODELS_DIR, "model_max.pckl"))




mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))


dotenv_path = os.path.join(BASE_DIR, ".env")  # Chemin vers le fichier .env
load_dotenv(dotenv_path=dotenv_path)  # Load environment variables from .env file


"""python3 -m uvicorn api:app --reload"""

es = get_Elasticsearch()  # Initialize Elasticsearch client

class SearchPayload(BaseModel):
    must_contain: Optional[List[str]] = None
    contain_any: Optional[List[str]] = None
    not_contain: Optional[List[str]] = None
    exact_match: Optional[bool] = False
    



def search_in_elasticsearch(payload: SearchPayload):
    try:
        if payload.exact_match:
            es_query = {
                "query": {
                    "bool": {
                        "must": [{"term": {"description.keyword": word}} for word in (payload.must_contain or [])],
                        "should": [{"term": {"description.keyword": word}} for word in (payload.contain_any or [])],
                        "must_not": [{"term": {"description.keyword": word}} for word in (payload.not_contain or [])]
                    }
                }
            }
        else:
            es_query = {
                "query": {
                    "bool": {
                        "must": [{"match": {"description": {"query": word, "fuzziness": "AUTO"}}} for word in (payload.must_contain or [])],
                        "should": [{"match": {"description": {"query": word, "fuzziness": "AUTO"}}} for word in (payload.contain_any or [])],
                        "must_not": [{"match": {"description": word}} for word in (payload.not_contain or [])]
                    }
                }
            }
        # Send the search query to Elasticsearch
        es_results = es.search(index="job_index", body=es_query)
        
        # Extract matching IDs from Elasticsearch results
        matching_ids = [hit["_source"]["job_id"] for hit in es_results['hits']['hits']]
        
        # If no matching documents, raise an exception
        if not matching_ids:
            raise HTTPException(status_code=404, detail="No matching jobs found in Elasticsearch")

        return matching_ids
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while searching in Elasticsearch: {str(e)}")


# Method to search in MySQL using the matching IDs
def search_in_mysql(matching_ids: List[str]):
    placeholders = ', '.join(['%s'] * len(matching_ids))
    mysql_query = f"""
        SELECT 
            a.title, 
            CONCAT('https://candidat.francetravail.fr/offres/recherche/detail/', a.internal_id) AS link, 
            b.label AS contract_type, 

            CASE 
                WHEN c.max_monthly_salary IS NULL 
                    THEN CONCAT('à partir de ', c.min_monthly_salary, ' € par mois') 
                ELSE CONCAT('de ', c.min_monthly_salary, ' € à ', c.max_monthly_salary, ' € par mois') 
            END AS salary, 

            CASE 
                WHEN c.min_monthly_salary = 0 AND c.max_monthly_salary = 0 THEN 
                    CASE 
                        WHEN c.avg_max_diff BETWEEN -10 AND 10 AND max_monthly_predicted != 0
                            THEN CONCAT('Salaire prédit entre ', c.min_monthly_predicted, ' et ', c.max_monthly_predicted, ' € par mois')
                        WHEN c.avg_max_diff NOT BETWEEN -10 AND 10 AND c.avg_min_diff BETWEEN -10 AND 10 
                            THEN CONCAT('Salaire prédit à partir de ', c.min_monthly_predicted, ' € par mois')
                        ELSE 'Salaire prédit inconnu'
                    END
                ELSE NULL
            END AS predicted_salary,

            CASE 
                WHEN d.name = '75' THEN 'PARIS' 
                ELSE d.name 
            END AS city

        FROM job a
        JOIN job_contract b ON a.job_id = b.job_id
        JOIN salary c ON a.job_id = c.job_id
        JOIN cities d ON a.insee_code = d.insee_code
        WHERE a.job_id IN ({placeholders});
    """
    try:
        
        with get_db(dictionary=True) as (conn, cursor):  
            cursor.execute(mysql_query, matching_ids)
            result = cursor.fetchall()
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while querying MySQL: {str(e)}")

def get_db_dependency():
    with get_db(dictionary=True) as (_, cursor):
        yield cursor

@app.post("/search/")
def search_jobs(payload: SearchPayload, db=Depends(get_db_dependency)):
    
    # Step 1: Search in Elasticsearch for matching job IDs
    try:
        matching_ids = search_in_elasticsearch(payload)
        
        # Step 2: Search in MySQL using the matching IDs
        result = search_in_mysql(matching_ids)
    
        return {"results": result}
    except HTTPException as e:
        # If an HTTPException is raised, propagate it
        raise e

def get_salary_thresholds():
    with get_db() as (conn, cursor):
        cursor.execute("SELECT max_monthly_salary FROM salary WHERE max_monthly_salary > 75")
        values_max = [row[0] for row in cursor.fetchall() if row[0] > 0]  
        cursor.execute("SELECT min_monthly_salary FROM salary WHERE min_monthly_salary > 75")
        values_min = [row[0] for row in cursor.fetchall() if row[0] > 0] 
   
    thresholds_min = np.percentile(values_min, [ 70])
    thresholds_max = np.percentile(values_max, [70])
    return thresholds_min[0],  thresholds_max[0], 

####  salary prediction


def get_salary_tiers(job_ids: List[int], data: pd.DataFrame) -> pd.DataFrame:
    """
    Prend une liste de job_ids et un DataFrame avec toutes les données,
    retourne le DataFrame avec les colonnes 'min_tier' et 'max_tier' ajoutées.
    """
    df = data[data["job_id"].isin(job_ids)].copy()

    if df.empty:
        raise HTTPException(status_code=404, detail="Aucun des job_id n'a été trouvé.")

    # Appliquer les prédictions des tiers
    df["min_tier"] = model_min.predict(df)
    df["max_tier"] = model_max.predict(df)

    return df
        
    




        
    
def prepare_data(job_id):
    with get_db() as  (conn, cursor):
            
            data = get_df(cursor, conn)
            row = data[data["job_id"] == job_id]
            print(type(job_id))                  # Should be int (or match the column type)
            print(data["job_id"].dtype)
            print(data["job_id"].head())
            logging.info(f"Résultats trouvés pour job_id={job_id}: ")
            return row

            
# Compteur simple : nombre d'appels
requests_counter = Counter('app_requests_total', 'Total des requêtes')

# Histogramme pour mesurer la latence (durée) des appels
request_latency = Histogram('app_request_latency_seconds', 'Latence des requêtes')


REQUEST_COUNT = Counter(
    'api_request_count', 'Nombre de requêtes HTTP',
    ['method', 'endpoint', 'http_status']
)
REQUEST_LATENCY = Histogram(
    'api_request_latency_seconds', 'Latence des requêtes HTTP',
    ['method', 'endpoint']
)
ERROR_COUNT = Counter(
    'api_error_count', 'Nombre d\'erreurs HTTP',
    ['method', 'endpoint', 'http_status']
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time

    endpoint = request.url.path
    method = request.method
    status_code = response.status_code

    REQUEST_COUNT.labels(method=method, endpoint=endpoint, http_status=str(status_code)).inc()
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(process_time)

    if status_code >= 500:
        ERROR_COUNT.labels(method=method, endpoint=endpoint, http_status=str(status_code)).inc()

    return response

@app.get("/metrics")
def metrics():
    # Endpoint scrappé par Prometheus
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)          

class JobIDs(BaseModel):
    job_ids: List[int]

@app.post("/predict")
def predict_salaries(payload: JobIDs) -> Dict[int, Dict[str, Any]]:
    job_ids = payload.job_ids
    results = {}

    try:
        with get_db() as (conn, cursor):
            data = get_df(cursor, conn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur BDD : {e}")

    try:
        df = get_salary_tiers(job_ids, data)
    except HTTPException as e:
        raise e

    # Initialiser les colonnes
    df["min_salary_log"] = np.nan
    df["max_salary_log"] = np.nan

    for tier in [0, 1]:
        min_df = df[df["min_tier"] == tier]
        if not min_df.empty:
            model = min_half_low if tier == 0 else min_half_high
            df.loc[min_df.index, "min_salary_log"] = model.predict(min_df)

        max_df = df[df["max_tier"] == tier]
        if not max_df.empty:
            model = max_half_low if tier == 0 else max_half_high
            df.loc[max_df.index, "max_salary_log"] = model.predict(max_df)

    df["min_salary"] = np.exp(df["min_salary_log"])
    df["max_salary"] = np.exp(df["max_salary_log"])
    df.loc[df["min_salary"] > df["max_salary"], "max_salary"] = 0

    for job_id in job_ids:
        match = df[df["job_id"] == job_id]
        if match.empty:
            results[job_id] = {"error": "Job ID non trouvé."}
        else:
            results[job_id] = {
                "min_monthly_salary": round(float(match["min_salary"].values[0]), 2),
                "max_monthly_salary": round(float(match["max_salary"].values[0]), 2)
            }

    return results



client = mlflow.tracking.MlflowClient()

@app.get("/metrics/classification")
def get_latest_metrics(model_name: str = Query(None, description='model_min or model_max (optional)')):
    experiment = client.get_experiment_by_name("classification_experiment")
    if not experiment:
        return {"error": "Experiment 'classification_experiment' introuvable."}

    

    if model_name:
        # Cherche le dernier run filtré par modèle
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=f"params.model_name = '{model_name}'",
            order_by=["start_time DESC"]
        )
    else:
        # Cherche le dernier run global
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"]
        )

    if not runs:
        return {"error": "Aucun run trouvé"}

    return runs[0].data.metrics

@app.get("/metrics/regression")
def get_latest_metrics(model_name: str = Query(None, description='model_min or model_max (optional)')):
    experiment = client.get_experiment_by_name("regression_experiment")
    if not experiment:
        return {"error": "Experiment 'regression_experiment' introuvable."}

    

    if model_name:
        # Cherche le dernier run filtré par modèle
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=f"params.model_name = '{model_name}'",
            order_by=["start_time DESC"]
        )
    else:
        # Cherche le dernier run global
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"]
        )

    if not runs:
        return {"error": "Aucun run trouvé"}

    return runs[0].data.metrics