import time
import mysql.connector
import logging
import os

from contextlib import contextmanager
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

project_root = os.getenv('PROJECT_ROOT')
if not project_root:
        # fallback local dev
    project_root = os.path.dirname(__file__)
env_path = os.path.join(project_root, '.env')  # Path to the .env fil
load_dotenv(dotenv_path=env_path) # Load environment variables from .env file

class DatabaseConfig:
    """Database configuration class to store connection parameters."""
    def __init__(self, host=os.getenv("DB_HOST", "localhost"), database=os.getenv("DB_NAME", "mydb"), user=os.getenv("DB_USER", "root"), password=os.getenv("DB_PASSWORD", ""), port=os.getenv("DB_PORT", "3306"), charset='utf8mb4'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.charset = charset
        

    def get_connection(self):
        """Create and return a connection based on the config."""
        return mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            charset=self.charset
        )
    def as_dict(self):
        return {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port,
            "charset": self.charset
        }
    

def get_Elasticsearch():
    """Create and return an Elasticsearch client."""
    
    es_config = {
        "hosts": [{
            "host": os.getenv("ES_HOST", "localhost"),
            "port": int(os.getenv("ES_PORT", 9200)),
            "scheme": "http",
            
        }]
    }
    es = Elasticsearch(**es_config, verify_certs=False, basic_auth=(os.getenv("ES_USER", ""), os.getenv("ELASTIC_PASSWORD", "")))

    # Wait for ES to be ready (max 30 seconds)
    for i in range(30):
        try:
            if es.ping():
                logging.info("Elasticsearch is up!")
                return es
        except Exception as e:
            logging.warning(f"Waiting for Elasticsearch... ({i+1}/30)")
        time.sleep(1)
    
    raise ConnectionError("Could not connect to Elasticsearch after 30 seconds.")
    


# Instantiate the DatabaseConfig
db_config = DatabaseConfig()


@contextmanager
def get_db(dictionary=False):
    connection = None
    cursor = None
    try:
        # Connexion à la base de données
        connection = db_config.get_connection()
        cursor = connection.cursor(dictionary=dictionary, buffered=True)
        # Yield la connexion et le curseur ensemble
        yield connection, cursor
    except mysql.connector.Error as e:
        logging.error(f"Erreur connexion base de données : {e}")
        raise  # Relancer l'exception pour qu'elle soit gérée par FastAPI
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logging.warning(f"Erreur fermeture curseur : {e}")
        if connection:
            try:
                connection.close()
            except Exception as e:
                logging.warning(f"Erreur fermeture connexion : {e}")


def get_db_persistent():
    """Create a persistent connection for database setup."""
    print(mysql.connector.__file__)
    connection = db_config.get_connection()  # Get connection from DatabaseConfig
    cursor = connection.cursor(dictionary=False, buffered=True)
    return cursor, connection

def main():
    # Test the connection
    print(mysql.connector.__file__)

if __name__ == "__main__":
    main()
