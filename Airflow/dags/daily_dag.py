from dotenv import load_dotenv


from airflow import DAG
from airflow.models import DagModel
from airflow.utils.session import provide_session
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pytest

default_args = {
    'start_date': datetime(2025, 5, 27),
    'catchup': True
}










with DAG(
    dag_id='daily_job_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    description='Pipeline quotidien : chargement, ES, ML, MAJ',
    catchup=False,
) as dag:
    


    def clear_test_extraction_index():
        
        from dotenv import load_dotenv
        from DatabaseCreator.database import get_Elasticsearch
        
        load_dotenv(dotenv_path='/opt/airflow/project/.env', override=True)
        
        # Connect to Elasticsearch
        es = get_Elasticsearch()

        if es.indices.exists(index="extraction_logs"):
            es.delete_by_query(
                index="extraction_logs",
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )
    
        
    def run_database_tests():
        load_dotenv(dotenv_path='/opt/airflow/project/.env.test', override=True)
        print(os.getenv("DB_NAME"))
        print(os.getenv("DB_USER"))
        
        retcode = pytest.main(["-q", "--tb=short", "/opt/airflow/project/test/database_test.py"])
        if retcode != 0:
            raise Exception("Les tests Pytest ont échoué !")
        print("Tests Pytest passés avec succès.")



    def run_api_tests():
        load_dotenv(dotenv_path='/opt/airflow/project/.env.test', override=True)
        retcode = pytest.main(["-q", "--tb=short", "/opt/airflow/project/test/api_test.py"])
        if retcode != 0:
            raise Exception("Les tests Pytest ont échoué !")
        print("Tests Pytest passés avec succès.") 

    def load_data():
        load_dotenv(dotenv_path='/opt/airflow/project/.env', override=True)
        from DatabaseCreator.FranceTravailDataExtractor2 import main
        main()
        print("Données chargées avec succès.")

    def load_to_elasticsearch():
        load_dotenv(dotenv_path='/opt/airflow/project/.env',override=True)
        from Elastic_search.sync import main
        main()
        print("Données chargées dans Elasticsearch avec succès.")

    def run_ml_models():
        load_dotenv(dotenv_path='/opt/airflow/project/.env', override=True)
        from Models.Class_Model import main
        main()
        from Models.Final_model import main
        main()
        print("Modèles ML exécutés avec succès.")

    def predict_salaries():
        load_dotenv(dotenv_path='/opt/airflow/project/.env', override=True)
        from DatabaseCreator.update_predicted_salaries import  daily_update_predicted_salary_parallel
        daily_update_predicted_salary_parallel()
        print("Prédictions de salaires mises à jour avec succès.")

    def clean_up():
        load_dotenv(dotenv_path='/opt/airflow/project/.env', override=True)
        from DatabaseCreator.post_load import main
        main()
        print("Nettoyage de la base de données effectué avec succès.")

    def truncate_all_tables():
        load_dotenv(dotenv_path='/opt/airflow/project/.env.test', override=True)
        from DatabaseCreator.database import get_db_persistent
        
        cursor, connection = get_db_persistent()
        
            # Récupère uniquement les tables (pas les vues)
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_type = 'BASE TABLE';
        """)
        tables = cursor.fetchall()

        # Désactive les clés étrangères pour éviter les conflits de dépendances
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        for row in tables:
            table = row[0]
            print(f"Truncating table: {table}")
            cursor.execute(f"TRUNCATE TABLE `{table}`;")

        # Réactive les clés étrangères
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        connection.commit()
        cursor.close()
        connection.close()
    print("Toutes les tables ont été vidées avec succès.")

    


    t_reset_database_1= PythonOperator(
        task_id='reset_database_1',
        python_callable=truncate_all_tables,
        
    )

    t_reset_database_2= PythonOperator(
        task_id='reset_database_2',
        python_callable=truncate_all_tables,
        
    )

    t_load_data = PythonOperator(
        
        task_id='load_data',
        python_callable=load_data
    )

    t_load_to_elasticsearch = PythonOperator(
        task_id='load_to_elasticsearch',
        python_callable=load_to_elasticsearch
    )

    t_run_ml_models = PythonOperator(
        task_id='run_ml_models',
        python_callable=run_ml_models
    )

    t_predict_salaries = PythonOperator(
        task_id='predict_salaries',
        python_callable=predict_salaries
    )
    t_clean_up = PythonOperator(
        task_id='clean_up',
        python_callable=clean_up
    )

    t_run_database_tests = PythonOperator(
    task_id="run_database_tests",
    python_callable=run_database_tests
)

    t_run_api_tests = PythonOperator(
        task_id="run_api_tests",
        python_callable=run_api_tests
    )

  


    t_run_database_tests >>    t_load_data >>  t_load_to_elasticsearch >> t_run_ml_models >> t_reset_database_1 >> t_run_api_tests >> t_reset_database_2 >> t_predict_salaries >> t_clean_up
    

        
