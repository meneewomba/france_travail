import os


from elasticsearch import Elasticsearch, helpers
from DatabaseCreator.database import get_db_persistent, get_Elasticsearch
from dotenv import load_dotenv



# Get persistent connection (cursor, connection) for MySQL
cursor, connection = get_db_persistent()



# Elasticsearch client setup

env_path = os.path.join(os.path.dirname(__file__), '.env')  # Path to the .env file
es = get_Elasticsearch()  # Initialize Elasticsearch client
load_dotenv(dotenv_path= env_path)

print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_PORT: {os.getenv('DB_PORT')}")
print(f"DB_NAME: {os.getenv('DB_NAME')}")

# Fetch data from MySQL
def fetch_data_from_mysql():
    cursor.execute("SELECT job_id, description FROM job where active = 1")  # Replace with your actual query
    data = cursor.fetchall()
    return data

# Insert data into Elasticsearch
def generate_actions(data):
    for row in data:
        yield {
            "_index": "job_index",
            "_id": row[0],  # job_id
            "_source": {
                "job_id": row[0],
                "description": row[1],
                # Ajoute ici les autres champs nécessaires
            }
        }
        print(f"Document inséré : {row[0]}")

def insert_into_elasticsearch(data, es):
    try:
        response = helpers.bulk(es, generate_actions(data), chunk_size=1000)
        print(f"Bulk insert terminé : {response[0]} documents insérés.")
    except Exception as e:
        print(f"Erreur durant le bulk insert : {e}")

def main():
    cursor, connection = get_db_persistent()



# Elasticsearch client setup
    root_project = os.getenv("PROJECT_ROOT")
    if root_project:
        OUTPUT_DIR = root_project
    else:
        OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

    env_path = os.path.join(root_project, '.env')  # Path to the .env file
    load_dotenv(dotenv_path= env_path)
    es = get_Elasticsearch()  # Initialize Elasticsearch client
    
    data = fetch_data_from_mysql()  # Fetch data from MySQL
    insert_into_elasticsearch(data, es)  # Insert data into Elasticsearch
    print("✅ Data successfully loaded into Elasticsearch!")

    

if __name__ == "__main__":

    main()
