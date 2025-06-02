









from datetime import datetime
import os
from typing import List
from mysql.connector import Error
import requests
from DatabaseCreator.database import get_Elasticsearch, get_db_persistent
import logging
from DatabaseCreator.token_manager import TokenManager
from elasticsearch import Elasticsearch, helpers


def insert_offers_evolution(cursor, connection):
    """
    Insert data into the offers_evolution table.
    """
    try:
        query = """
            INSERT INTO offers_evolution (date, job_offers, dept)
            SELECT %s, COUNT(job_id), LEFT(insee_code, 2) AS dpt
            FROM job where active = 1
            group by dpt
            ON DUPLICATE KEY UPDATE job_offers = VALUES(job_offers)
            """
        cursor.execute(query,(datetime.today(),))
            
        
        connection.commit()
        logging.info("Data inserted into offers_evolution table successfully.")
    except Error as e:
        logging.exception(f"Error inserting data into offers_evolution: {e}")
        connection.rollback()
        logging.error(f"Transaction rolled back due to error: {e}")


def clean_up_job_table(cursor, connection, token_manager):
    """
    Clean up the job table by removing entries that are not updated.
    """
    cursor.execute("SELECT internal_id FROM job where active = 1 and update_date < DATE_SUB(NOW(), INTERVAL 30 DAY)")
    
    logging.info("Fetched all internal IDs from job table.")
    
    ids_in_db = set(row[0] for row in cursor.fetchall())
    
    logging.info(f"IDs to check: {len(ids_in_db)}")
    remaining_ids =  len(ids_in_db)
    for id in ids_in_db:
        try:
            
            logging.info(f"Remaining IDs to check: {remaining_ids}")
            
            token, token_type = token_manager.get_token()
            headers = {"Accept": "application/json",
                       "Authorization": f"{token_type} {token}"}
            response = requests.get(f"https://api.francetravail.io/partenaire/offresdemploi/v2/offres/{id}",
                                     headers=headers, timeout=30)
            if response.status_code == 204:
                cursor.execute("UPDATE job SET active = 0 WHERE internal_id = %s", (id,))
                logging.info(f"Job with internal ID {id} marked as inactive.")
                connection.commit()
                
            else:
                logging.info(f"Job with internal ID {id} is still available.")
            remaining_ids -= 1
    
        
        except Error as e:
            logging.exception(f"Error deleting job with internal ID {id}: {e}")
            connection.rollback()
            logging.error(f"Transaction rolled back due to error: {e}") 

def delete_jobs(cursor, connection, es):
    """
    Delete job offers that are inactive 
    """
    try:
        cursor.execute("""
            select count(*) FROM job
            WHERE active = 0
        """)
        count = cursor.fetchone()[0]
        if count >= 80000:
            cursor.execute("""  
                select job_id FROM job
                WHERE active = 0
                order by update_date
                LIMIT 2000
            """)
            
            ids = cursor.fetchall()
            ids = [row[0] for row in ids]
            placeholders = ','.join(['%s'] * len(ids))
            delete_query = f"DELETE FROM job WHERE job_id IN ({placeholders})"
            cursor.execute(delete_query, ids)
            connection.commit()
            logging.info("Inactive job offers deleted successfully.")
            delete_jobs_from_es(ids, es)
            logging.info(f"Deleted {len(ids)} inactive job offers from the es database.")
            
        else:
            logging.info("No inactive job offers to delete.")
            return
    except Error as e:
        logging.exception(f"Failed to delete inactive job offers: {e}")
        connection.rollback()
        logging.error(f"Transaction rolled back due to error: {e}")


def delete_jobs_from_es(job_ids: List[int], es):
    actions = [
        {
            "_op_type": "delete",
            "_index": "job_index",
            "_id": str(job_id)  # Match the ID format in ES (string)
        }
        for job_id in job_ids
    ]

    if actions:
        helpers.bulk(es, actions)
        logging.info(f"Deleted {len(actions)} jobs from Elasticsearch.")
    else:
        logging.info("No jobs to delete in Elasticsearch.")

def main():
    """
    Main function to execute the post-load operations.
    """
    
    cursor, connection = get_db_persistent()
    es = get_Elasticsearch()
    project_root = os.getenv("DB_CREATOR_PATH")
    if project_root:
        OUTPUT_DIR = project_root
    else:
        
         OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
    token_manager = TokenManager(os.path.join(OUTPUT_DIR))
    
    logging.info("Starting post-loading operations.")
    
    
    
    clean_up_job_table(cursor, connection, token_manager)
    logging.info("Job table cleaned up successfully.")

    insert_offers_evolution(cursor, connection)
    logging.info("Offers evolution data inserted successfully.")
    
    delete_jobs(cursor, connection, es)
    
    logging.info("Post-load operations completed successfully.")
    cursor.close()
    connection.close()
    logging.info("Database connection closed.")
