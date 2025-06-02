from dotenv import load_dotenv   


from DatabaseCreator.FranceTravailDataExtractor2 import insert_requirements, insert_cities, insert_job_node, insert_moving, insert_companies, insert_job, insert_contract, insert_salary, insert_benefits, insert_competencies, insert_job_competency, insert_driver_license, insert_job_driver_license, insert_formation, insert_professional_qualities, insert_languages
import pytest 

import logging
from DatabaseCreator.database import DatabaseConfig, get_db_persistent
import json
import os




naf_labels = {"87.90B":"toto"}

@pytest.fixture(scope="module")
def connection():
   config = DatabaseConfig()
   conn = config.get_connection()
   yield conn
   conn.close()


@pytest.fixture(scope="module")
def cursor(connection):
    cursor = connection.cursor(dictionary=False, buffered=True)
    yield cursor
    connection.rollback() # clean DB after each test
    cursor.close()

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




@pytest.fixture(scope="module")
def test_insert_offer(cursor, connection, job_offer):

    
   
    try:
        insert_requirements(cursor, connection)
        insert_cities(cursor, connection,job_offer)
        insert_job_node(cursor, connection,job_offer, naf_labels)
        
        insert_moving(cursor, connection,job_offer)
        
            
        company_id = insert_companies(cursor, connection, job_offer)
        if not company_id:
            logging.warning(f"Failed to insert company: {job_offer}")
            company_id = None
            
        job_id = insert_job(cursor, connection, job_offer, company_id)
        if not job_id:
            logging.warning(f"Failed to insert job: {job_offer}")
            job_id = None
            
        insert_contract(cursor, connection, job_id, job_offer)
        try:
            salary_id = insert_salary(cursor, connection, job_id, job_offer)
            if not salary_id:
                logging.warning(f"Failed to insert salary: {job_offer}")
        except Exception as e:
            logging.error(f"Error inserting salary: {e}")
            salary_id = None
        insert_benefits(cursor, connection, salary_id, job_offer)
        
            
        
        
        insert_competencies(cursor, connection, job_offer)
        insert_job_competency(cursor, connection, job_id, job_offer)
        
        driver_license_id = insert_driver_license(cursor, connection, job_offer)
        if not driver_license_id:
            logging.warning(f"Failed to insert driver_license: {job_offer}")
            
        insert_job_driver_license(cursor, connection, job_id, driver_license_id, job_offer)
        
        insert_formation(cursor, connection, job_id, job_offer)
        insert_professional_qualities(cursor, connection, job_id, job_offer)
        insert_languages(cursor, connection, job_id, job_offer)
        print(f"Inserted job offer with ID: {job_id}")
        return job_id, company_id, salary_id
    except Exception as e:  
        logging.error(f"Error during test_insert_offer: {e}")
        pytest.fail(f"Failed to insert job offer: {str(e)}")
        return None, None, None

def test_database_connection(connection):
    try:
        # Attempt to execute a simple query to check the connection
        connection.ping()  # This will check if the connection is alive
        assert connection is not None, "Failed to connect to the database"
        print("Database connection successful")
    except Exception as e:
        pytest.fail(f"Database connection failed: {str(e)}")
        
# job_id
def test_job_id(cursor, test_insert_offer, job_offer):
    job_id,_,_ = test_insert_offer
    cursor.execute("SELECT internal_id FROM job WHERE job_id = %s", (job_id,))
    result = cursor.fetchone()
    assert result[0] == job_offer["id"], f"Expected internal_id {job_offer['id']}, got {result[0]}"


# job_title
def test_job_title(cursor, test_insert_offer, job_offer):
    job_id, company_id, salary_id = test_insert_offer   
    cursor.execute("SELECT title  FROM job WHERE job_id = %s", (job_id,))
    result = cursor.fetchone()
    assert result[0] == job_offer["intitule"], f"Expected title {job_offer['title']}, got {result[0]}"

# job_description
def test_job_description(cursor, test_insert_offer, job_offer):
    job_id,_,_= test_insert_offer
    cursor.execute("SELECT description  FROM job WHERE job_id = %s", (job_id,))
    result = cursor.fetchone()
    assert result[0] == job_offer["description"], f"Expected description {job_offer['description']}, got {result[0]}"

# rome_code
def test_job_rome_code(cursor, test_insert_offer, job_offer):
    job_id, company_id, salary_id = test_insert_offer
    cursor.execute("SELECT rome_code  FROM job WHERE job_id = %s", (job_id,))
    result = cursor.fetchone()
    assert result[0] == job_offer["romeCode"], f"Expected rome_code {job_offer['romeCode']}, got {result[0]}"


# insee_code
def test_job_insee_code(cursor, test_insert_offer, job_offer):
    job_id,_,_ = test_insert_offer
    cursor.execute("SELECT insee_code  FROM job WHERE job_id = %s", (job_id,))
    result = cursor.fetchone()
    assert result[0]== int(job_offer["lieuTravail"]["commune"]), f"Expected insee_code {job_offer['lieuTravail']['commune']}, got {result[0]}"

# entreprise
def test_job_company(cursor, test_insert_offer, job_offer):
    _,company_id,_ = test_insert_offer
    cursor.execute("SELECT name  FROM companies WHERE company_id = %s", (company_id,))
    result = cursor.fetchone()
    assert result[0] == job_offer["entreprise"]["nom"], f"Expected name {job_offer['entreprise']['nom']}, got {result[0]}"

    # competency
def test_job_competency(cursor, test_insert_offer, job_offer):
    job_id,_,_ = test_insert_offer
    cursor.execute("SELECT c.label  FROM competencies c join job_competency b on c.competency_code = b.competency_code  WHERE b.job_id = %s", (job_id,))
    results = cursor.fetchall()
    results = {row[0] for row in results}

    actual_libelles = {c["libelle"] for c in job_offer["competences"]}
    assert results == actual_libelles, f"Expected competencies {actual_libelles}, got {results}"


    # benefits
def test_job_benefits(cursor, test_insert_offer, job_offer):
    _,_,salary_id = test_insert_offer
    cursor.execute("""SELECT a.label  FROM benefits a join salary_benefits b on a.benefits_id = b.benefits_id 
                    WHERE b.salary_id = %s""", (salary_id,))
    results = cursor.fetchall()
    results = {row[0] for row in results}
    actual_libelles = {job_offer["salaire"]["complement1"] + '' , '' +  job_offer["salaire"]["complement2"]}
    assert results == actual_libelles, f"Expected benefits {actual_libelles}, got {results}"

# salary
def test_job_salary(cursor, test_insert_offer, job_offer):
    _,_,salary_id = test_insert_offer
    cursor.execute("SELECT min_monthly_salary, max_monthly_salary  FROM salary WHERE salary_id = %s", (salary_id,))
    result = cursor.fetchone()
    salary_str = job_offer["salaire"]["libelle"]
    assert str(result[0]) in salary_str, f"Expected min_monthly_salary {job_offer['salaire']['libelle']}, got {result[0]}"
    assert str(result[1]) in salary_str, f"Expected max_monthly_salary {job_offer['salaire']['libelle']}, got {result[1]}"
    