import json
import logging
import os
from dotenv import load_dotenv


import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from FastAPI.api import app
from DatabaseCreator.database import DatabaseConfig, get_Elasticsearch
from DatabaseCreator.FranceTravailDataExtractor2 import insert_requirements, insert_cities, insert_job_node, insert_moving, insert_contact, insert_companies, insert_job, insert_contract
from test.database_test import test_insert_offer



client = TestClient(app)

naf_labels = {"87.90B":"toto"}

es = get_Elasticsearch()

@pytest.fixture
def insert_fake_jobs(cursor, connection, job_offer, test_insert_offer):
    try:
        
        job_id, company_id, salary_id= test_insert_offer
        connection.commit()

        # Indexation dans Elasticsearch
        es_doc = {
            "job_id": job_id,
            "title": job_offer.get("title", ""),
            "description": job_offer.get("description", ""),
            # ajoute d'autres champs si besoin
        }

        es.index(index="jobs", id=job_id, document=es_doc)
        es.indices.refresh(index="jobs")

    except Exception as e:
        connection.rollback()
        raise e

    yield job_id

    # Nettoyage après le test

    cursor.execute("DELETE FROM job WHERE internal_id = %s", (job_offer['id'],))
    connection.commit()

    try:
        es.delete(index="jobs", id=job_id)
        es.indices.refresh(index="jobs")
    except Exception as e:
        print(f"ES cleanup error: {e}")

@pytest.fixture(scope="module")
def connection():
    config = DatabaseConfig()
    conn = config.get_connection()
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def cursor(connection):
    cur = connection.cursor(dictionary=True, buffered=True)
    yield cur
    connection.commit()
    cur.close()

@pytest.fixture(scope="module")
def job_offer():
    root_path = os.getenv("PROJECT_PATH")
    if root_path:
        test_file_path = os.path.join(root_path, "test_data.json")
    else:     
        test_file_path = os.path.join(os.path.dirname(__file__), "test_data.json")
    with open(test_file_path, "r", encoding="utf-8") as f:
        job_offer = json.load(f)
        
    return job_offer




def test_search_jobs( job_offer):
    

    search_payload = {
        "must_contain": ["Accueillir", "Accompagner"],
        "contain_any": ["orienter"],
        "not_contain": [" stage "],
        "exact_match": False
    }

    response = client.post("/search/", json=search_payload)
    print(response.status_code)
    print(response.json())
    assert response.status_code == 200
    data = response.json()

    assert "results" in data
    assert (job_offer['internal_id'] in r["link"] for r in data["results"])

def test_search_jobs_no_results( job_offer):
       

    search_payload_2 = {
        "must_contain": ["Python", "Développeur"],  # un mot clé qui ne correspond pas à la description du job
        "contain_any": ["orienter"],
        "not_contain": ["stage"],
        "exact_match": False
    }

    response2 = client.post("/search/", json=search_payload_2)
    assert response2.status_code == 200
    data2 = response2.json()

    assert "results" in data2
    # On vérifie que le job_id n'est PAS dans les résultats
    assert all(str(job_offer['internal_id']) not in str(r["link"]) for r in data2["results"])

def test_predict_salary(insert_fake_jobs):
    job_id = insert_fake_jobs

    predict_payload = {
        "job_ids": [job_id]
       
    }

    response3 = client.post("/predict/", json=predict_payload)
    assert response3.status_code == 200