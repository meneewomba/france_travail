from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dateutil.relativedelta import relativedelta
import requests 
import json
import os
import time
import csv
import re
from mysql.connector import Error, pooling
import sys
from datetime import datetime, timedelta, timezone
import logging
from DatabaseCreator.token_manager import TokenManager
from DatabaseCreator.database import get_Elasticsearch, get_db_persistent, DatabaseConfig

import queue

from Elastic_search.es_writer import ElasticsearchHandler, ExcludeElasticsearchLogs

es = get_Elasticsearch()


already_queued_uris = set()
uri_retry_count = {}

id_inserted = set()


MAX_RETRIES = 3
retry_queue = queue.Queue()

db_config = DatabaseConfig().as_dict()

connection_pool = pooling.MySQLConnectionPool(
    pool_name="job_pool",
    pool_size=26,  # Tu peux adapter selon le nombre de threads
    **db_config
)
   

# Global semaphore to limit to 10 concurrent API calls
semaphore = threading.Semaphore(10)



lock = threading.Lock()
last_call_times = {}    
MAX_CALLS_PER_SECOND = 5  # Base rate limit
MAX_THREADS = 4  # Max threads allowed to run concurrently
lock = threading.Lock()
last_call_times = defaultdict(list)  # Suivi des appels par thread
active_threads_count = 0  # Nombre total de threads actifs
thread_lock = threading.Lock()  # Protéger l'accès au compteur de threads actifs

def generate_uris_by_date_range(department_code, token_manager, db_is_loaded):
    page_size = 150
    
    
    base_url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
    uris = set()

    current_end = datetime.now(timezone.utc) # Maintenant en UTC
    if db_is_loaded:
        start_date = current_end - relativedelta(days=2) # 2  jours avant
    else: 
        start_date = current_end - relativedelta(months=6)  # 6 mois avant
    step_hours = 24  # Intervalle initial de 24h
    
    
    while current_end > start_date:

        token,token_type = token_manager.get_token()

        headers = {
        "Accept": "application/json",
        "Authorization": f"{token_type} {token}"
    }
        
        logging.info(f"Début de la génération d'URIs. current_end: {current_end}, start_date: {start_date}")
        current_start = current_end - timedelta(hours=step_hours)
        if current_start < start_date:
            current_start = start_date

        min_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        max_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        test_url = f"{base_url}?departement={department_code}&minCreationDate={min_str}&maxCreationDate={max_str}&range=0-{page_size - 1}"
        response = requests.get(test_url, headers=headers)
        content_range = response.headers.get("Content-Range")

        logging.info(f"Content-Range : {content_range}")

        if content_range and "/" in content_range:
            try:
                total = int(content_range.split("/")[-1])

            except ValueError:
                total = 0
        else:
            total = 0
        logging.info(f"Total d'offres : {total}")
        if total > 3000:
            if step_hours > 1:
                step_hours = max(1, step_hours // 2)
                continue
            else:
                logging.warning(f"Plage trop dense même à 1h ({min_str} à {max_str}) — on avance quand même.")

        for first_index in range(0, total, page_size):
            last_index = min(first_index + page_size - 1, total - 1)
            range_str = f"{first_index}-{last_index}"
            uri = (
                f"{base_url}?departement={department_code}"
                f"&minCreationDate={min_str}&maxCreationDate={max_str}&range={range_str}"
            )
            uris.add(uri)

        # Toujours avancer dans le temps
        current_end = current_start
        step_hours = 24  # Réinitialiser l'intervalle à 24h pour la prochaine itération

        time.sleep(0.1)
    logging.info(f"Nombre d'URIs générés : {len(uris)}")
    return list(uris)

def rate_limit():
    with lock:
        now = time.time()
        last_call_times.append(now)
        if len(last_call_times) > MAX_CALLS_PER_SECOND:
            oldest = last_call_times.popleft()
            sleep_time = 1 - (now - oldest)
            if sleep_time > 0:
                time.sleep(sleep_time)


HOURS_PER_MONTH = 151.67
WEEKS_PER_MONTH = 4.33

def get_credentials(OUTPUT_DIR: str) -> dict[str, str]:
    """
    Récupère les accréditations à partir d'un fichier JSON.
    """
    with open(os.path.join(OUTPUT_DIR, "clientCredentials.json"), "r") as idFile:
        logging.debug(os.path.join(OUTPUT_DIR, "clientCredentials.json"))
        return json.load(idFile)

def get_access_token(client_id: str, client_secret: str) -> tuple[str, str]:
    """
    Création des accès via les accréditations clients.
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "o2dsoffre api_offresdemploiv2"
    }

    try:
        req=requests.post(url= "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire",
                headers= headers,params=params)
        req.raise_for_status()
        token_data = req.json()
        return token_data["access_token"], token_data["token_type"]
    except requests.exceptions.HTTPError as errh: 
        logging.exception("HTTP Error") 
        logging.exception(errh.args[0]) 

# def requete_api(token_type:str, token:str):
    # """
    # Requête l'API de France Travail.
    # """
    # headers={
        # "Accept": "application/json",
        # "Authorization": f"{token_type} {token}"
    # }

    # try :
        # query = requests.get(url="https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search",
                              # headers= headers)
        # query.raise_for_status()
        # return query.json(), query.headers
    # except requests.exceptions.HTTPError as errh:
        # print('HTTP Error:', errh)
        
def years_to_months(years):
    months = years * 12
    return months
    
def days_to_months(days):
    months = days / 30
    return round(months,2)

def get_naf_labels(token_type: str, token: str):
    """
    Requête le référentiel NAF de l'API de France Travail pour récupérer les libellés de chaque code.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"{token_type} {token}"
    }
    

    
    
    try:
        query = requests.get(
            url="https://api.francetravail.io/partenaire/offresdemploi/v2/referentiel/nafs",
            headers=headers,
            timeout=10  # 10 seconds timeout
        )
        
        # Check if the response was successful
        query.raise_for_status()
        
        # Check if the response is JSON
        if query.headers.get('Content-Type') == 'application/json':
            data = query.json()  # Parse the response to JSON
            
            # Now you have the JSON data, which is a list of dictionaries.
            # For example, you can extract the `code` and `libelle` from each item in the list:
            naf_dict = {item['code']: item['libelle'] for item in data}
            return naf_dict
        else:
            logging.error(f"Unexpected response format: {query.headers.get('Content-Type')}")
            return None
    
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh}")
        return None
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
        return None
    except requests.exceptions.RequestException as err:
        logging.error(f"Request Error: {err}")
        return None

""" def get_rome_codes(token_type: str, token: str):
    
    Requête le référentiel ROME de l'API de France Travail pour récupérer tous les codes.
    
    headers = {
        "Accept": "application/json",
        "Authorization": f"{token_type} {token}"
    }
    

    
    
    try:
        query = requests.get(
            url="https://api.francetravail.io/partenaire/offresdemploi/v2/referentiel/metiers",
            headers=headers,
            timeout=10  # 10 seconds timeout
        )
        
        # Check if the response was successful
        query.raise_for_status()
        
        # Check if the response is JSON
        if query.headers.get('Content-Type') == 'application/json':
            data = query.json()  # Parse the response to JSON
            
            # Now you have the JSON data, which is a list of dictionaries.
            # For example, you can extract the `code` and `libelle` from each item in the list:
            rome_codes = [item['code'] for item in data]
            return rome_codes
        else:
            logging.error(f"Unexpected response format: {query.headers.get('Content-Type')}")
            return None
    
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh}")
        return None
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
        return None
    except requests.exceptions.RequestException as err:
        logging.error(f"Request Error: {err}")
        return None """

    
def convert_to_mysql_datetime(iso_datetime: str) -> str:
    # Remove the 'Z' and convert the string to the MySQL format
    if iso_datetime.endswith('Z'):
        iso_datetime = iso_datetime[:-1]
    
    # Convert to MySQL format (without Z) and handle fractional seconds
    datetime_obj = datetime.fromisoformat(iso_datetime)
    
    # Convert back to string in MySQL-compatible format
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
    
def insert_job(cursor, connection, job_offer: dict, company_id) -> int:
   
    moving = job_offer.get("deplacementCode",1)
    try:
        
        
        def extract_experience_length(experience_label):
            experience_label = job_offer.get("experienceLibelle")
             # Check for months in experience_label
            contract_match = re.search(r"(\d+)\s*Mois$", experience_label)
            # Check for years (with optional "(s)") in experience_label
            contract_match2 = re.search(r"(\d+)\s*(An(?:\(s\)))?", experience_label)
            
            if contract_match:
                experience_length = int(contract_match.group(1))  # Extract the number of months
                return experience_length
    
            # If experience_label is in years(and optional "An(s)" is present)
            elif contract_match2:
                experience_length = int(contract_match2.group(1))  # Extract the number of years
                experience_length = years_to_months(experience_length)  # Convert days to months
                return experience_length
    
            # Default case when no valid work duration is found
            else:
                experience_length = 0
                return experience_length
                
        
            
        # Insert or Update query
        insert_query = """
        INSERT INTO job ( title, description, creation_date, update_date, rome_code, 
                          experience_required, experience_length_months, is_alternance, 
                         is_disabled_friendly, naf_code, qualification_code, candidates_missing, 
                         activity_sector_code, moving_code, experience_detail, insee_code, company_id, internal_id, active)
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,1 )
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            description = VALUES(description),
            creation_date = VALUES(creation_date),
            update_date = VALUES(update_date),
            experience_required = VALUES(experience_required),
            experience_length_months = VALUES(experience_length_months),
            is_alternance = VALUES(is_alternance),
            is_disabled_friendly = VALUES(is_disabled_friendly),
            candidates_missing = VALUES(candidates_missing),
            moving_code = VALUES(moving_code),
            experience_detail = VALUES(experience_detail),
            insee_code = VALUES(insee_code),
            active = VALUES(active)

            
        """
        
        # Execute the query
        cursor.execute(insert_query, (
            
            job_offer.get("intitule"),
            job_offer.get("description"),
            convert_to_mysql_datetime(job_offer.get("dateCreation")),
            convert_to_mysql_datetime(job_offer.get("dateActualisation")),
            job_offer.get("romeCode"),
            job_offer.get("experienceExige"),
            extract_experience_length(job_offer.get("experienceLibelle")),
            job_offer.get("alternance"),
            job_offer.get("accessibleTH"),
            job_offer.get("codeNAF"),
            job_offer.get("qualificationCode"),
            job_offer.get("offresManqueCandidats",0),
            job_offer.get("secteurActivite"),
            moving,
            job_offer.get("experienceCommentaire"),
            job_offer.get("lieuTravail", {}).get("commune", "75056"),
            company_id,
            job_offer.get("id")
            
            
        ))
        logging.debug(f"Executed query: {cursor._executed}")

        # Retrieve the job_id of the inserted/updated record
        if cursor.lastrowid:
            logging.info(f"Inserted into `job`: {cursor.lastrowid}")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT job_id FROM job WHERE internal_id = %s", 
                           (job_offer.get("id"),))
            logging.debug(f"Executed query: {cursor._executed}")
            result = cursor.fetchone()
            logging.info(f"Duplicate ignored in table `job`: {result}")
            return result[0] if result else None  
       
        logging.info(f"Job updated with ID: {result}")
        
    except Error as e:
        logging.exception(f"Error inserting/updating job: {e} , {job_offer}")
        return None
    


    
    
def convert_to_float(value: str) -> float:
    """
    Converts a string with comma as decimal separator into a float.
    Example: '1800,00' -> 1800.0
    """
    try:
        # Replace comma with a dot and convert to float
        return float(value.replace(',', '.'))
    except ValueError:
        raise ValueError(f"Unable to convert '{value}' to float.")
        
def add_space_around_numbers(input_str):
    """
    Adds a space:
    1. After numbers (integers and floats) if not already followed by a space or digit.
    2. Before numbers if they are immediately preceded by a letter.
    Does not break floating-point numbers (e.g., 1800.00).
    
    Args:
        input_str (str): The input string to process.
        
    Returns:
        str: The processed string with spaces added around numbers where necessary.
    """
    # Match whole numbers followed by non-space non-digit characters
    pattern_after = r'(\d+)([^\s\d\.])'
    # Match letters immediately followed by numbers, ensuring no space is already present
    pattern_before = r'([a-zA-Z])(\d+(\.\d+)?)'
    
    # Add space after numbers (but not floats)
    result = re.sub(pattern_after, r'\1 \2', input_str)
    # Add space before numbers if needed
    result = re.sub(pattern_before, r'\1 \2', result)
    
    return result



def convert_salary_to_monthly(salary_str, job_offer : dict): 
    """
    Convertit une chaîne de caractères représentant un salaire en un montant mensuel.
    
    Args:
        salary_str (str): Chaîne de caractères représentant le salaire (ex: "Mensuel de 2000.0 Euros sur 12.0 mois").
        
    Returns:
        dict: {'min_salary': float, 'max_salary': float} en Euros par mois.
    """
    # retrieve hours_per_week to calculate monthly salary from an hourly rate
    hpw, work_condition = hours_per_week(job_offer.get("dureeTravailLibelle"))
    if not hpw:
        hpw=35
        
    
    
    # Normalize the input (replace non-breaking space and remove extra spaces)
    salary_str = salary_str.replace('\xa0', ' ').replace(',', '.').replace('€','').strip()
    salary_str = re.sub(r'\s+', ' ', salary_str)  # Ensure consistent spacing
    
    logging.debug(f"Normalized salary string: {repr(salary_str)}")
    
    # Split the string into tokens based on spaces
    tokens = salary_str.split()
    logging.debug(f"Tokens: {tokens}")
    pattern = r'\b\d+(?:\.\d+)?\b'
    match = re.search(pattern, salary_str)
    if not match:
        logging.warning(f"Invalid salary format, salary not found: {salary_str}")
        return {'min_salary': 0, 'max_salary': 0}
    
            
    # Check if "de" exists and extract min amount
    if "de" in salary_str or "De" in salary_str :
        salary_str= salary_str.replace("De","de")
        tokens = salary_str.split()
        logging.debug(f"{tokens}")
        min_amount_index = tokens.index("de") + 1 
        min_amount = round(float(tokens[min_amount_index]),1)
        max_amount = min_amount
    
        
    else:
        logging.warning(f"Invalid salary format, 'de' not found: {salary_str}")
        return {'min_salary': 0, 'max_salary': 0}
    
    
    # Check for the presence of "à" and extract max_amount if it exists
    if "à" in tokens:
        try:
            max_amount_index = tokens.index("à") + 1
            max_amount = round(float(tokens[max_amount_index]),1)
        except ValueError:
            logging.exception(f"Error extracting max amount after 'à' in: {salary_str}")
            return {'min_salary': 0, 'max_salary': 0}
    
    # Extract the period (months)
    if "sur" in tokens:
        period_index = tokens.index("sur") + 1
        period_in_months = float(tokens[period_index])
        if period_in_months > 14.0:
            period_in_months = 12.0

        if period_in_months == 0.0:
            period_in_months = 12.0
    else:
        period_in_months = 12.0
        
    
    # Convert amounts to monthly salaries
    if salary_str.startswith("de") or salary_str.startswith("Autre")  or salary_str.startswith("Mensuel") or salary_str.startswith("Annuel") or salary_str.startswith("Horaire"):
        
        if salary_str.startswith("Annuel") and max_amount > 70:
            if max_amount < 5000:
                min_monthly = min_amount * period_in_months / 12
                max_monthly = max_amount * period_in_months / 12
            else:    
                min_monthly = min_amount / period_in_months
                max_monthly = max_amount / period_in_months
        elif salary_str.startswith("Mensuel") and max_amount > 70:
            if  max_amount > 20000:
                min_monthly = min_amount / period_in_months
                max_monthly = max_amount / period_in_months
            else:
                min_monthly = min_amount * period_in_months / 12
                max_monthly = max_amount * period_in_months / 12
        
        elif max_amount < 70:
            min_monthly = min_amount * hpw * WEEKS_PER_MONTH * period_in_months/12
            max_monthly = max_amount * hpw * WEEKS_PER_MONTH * period_in_months/12
        elif max_amount > 15000:
            min_monthly = min_amount / period_in_months
            max_monthly = max_amount / period_in_months
        
        else:
            min_monthly = min_amount * period_in_months /12
            max_monthly = max_amount * period_in_months /12

    
    
    else:
        logging.warning(f"Unknown salary type: {salary_str}")
        return {'min_salary': 0, 'max_salary': 0}
    
    
    logging.debug(f"Parsed salary details: Min={min_monthly}, Max={max_monthly}, Period={period_in_months}")
    
    return {
        'min_salary': round(min_monthly, 2),
        'max_salary': round(max_monthly, 2)
    }
def replace_space_between_numbers(input_str):
    """
    Replaces a space between two numbers with a dot (.)
    
    Args:
        input_str (str): The input string to process.
        
    Returns:
        str: The processed string with spaces between numbers replaced by dots.
    """
    # Regular expression to match a digit, followed by a space, followed by another digit
    pattern = r'(\d)\s+(\d)'
    # Replace the space with a dot
    return re.sub(pattern, r'\1\2', input_str)
  
def add_space_before_slash(input_str):
    """
    Adds a space between a number (integer or float) and a '/' if not already present.
    
    Args:
        input_str (str): The input string to process.
        
    Returns:
        str: The processed string with spaces added before '/' where necessary.
    """
    # Regular expression to match a number followed by a '/'
    pattern = r'(\d+(\.\d+)?)/'
    # Replace with the number followed by a space and the '/'
    return re.sub(pattern, r'\1 /', input_str)

  
def insert_salary(cursor, connection, job_id, job_offer: dict):
    """Insert a record into the `salary` table."""
    hpw, work_condition = hours_per_week(job_offer.get("dureeTravailLibelle"))
    if not hpw:
        hpw=35
    
    
    
    def format_salaries(salary_str):
            tokens = salary_str.split()
            try:
                if "à" in salary_str or "-" or " et " in salary_str:
                    if salary_str.startswith("-"):
                        salary_str=salary_str[1:]
                        
                    salary_str = salary_str.replace("-","à").replace("et","à")
                    tokens = salary_str.split()
                    index1= tokens.index("à") -1
                    index2= tokens.index("à") + 1
                    logging.debug(f"{tokens}")
                    min_monthly_salary = float(tokens[index1])
                    max_monthly_salary = float(tokens[index2])
                    if max_monthly_salary < 700:
                        min_monthly_salary *= hpw * WEEKS_PER_MONTH
                        max_monthly_salary *= hpw * WEEKS_PER_MONTH
                        salary_description = salary_str
                    elif max_monthly_salary > 15000 and min_monthly_salary < 200:
                        min_monthly_salary *=  1000 / 12
                        max_monthly_salary /=  12
                        salary_description = salary_str
                    elif max_monthly_salary > 15000:
                        min_monthly_salary /=  12
                        max_monthly_salary /=  12
                        salary_description = salary_str
                    else:
                        min_monthly_salary = min_monthly_salary
                        max_monthly_salary = max_monthly_salary
                        salary_description = salary_str

                    
                    return min_monthly_salary, max_monthly_salary, salary_description
            except Error as e:
                logging.warning(f"conversion error : {salary_str}")
            
    try:
        # Extract salary information from the job_offer
        salary_str = job_offer.get("salaire", {}).get("libelle")
        logging.debug(f"Extracted salary_str from libelle: {salary_str}")
        if not salary_str:
            try:
                salary_str = job_offer.get("salaire", {}).get("commentaire")
                
                if not salary_str:
                    min_monthly_salary = 0
                    max_monthly_salary = 0
                    salary_description = "Invalid salary format (no string)"
                    logging.warning(f"no salary found for job_id {job_id}")
                    
                    
                    
                logging.debug(f"Extracted salary_str from commentaire: {salary_str}")
                salary_str = re.sub(r'\s+', ' ', salary_str)  # Ensure consistent spacing
                salary_str = (
                    salary_str.replace("'","")
                    .replace("K", "k")
                    .replace(" k","k")
                    .replace("k","000")
                    .replace(",",".")
                    .replace(" 000","000")
                    .replace("?","")
                    .replace("€","")
                    .replace("Euros","")
                    .replace("E","")
                    .replace("euro","")
                    .replace("brut","")
                    .replace("BRUT","")
                    .replace("Brut","")
                    .replace("(","")
                    .replace(")","")
                    .replace("*","")
                    .replace(":","")

                )
                
                salary_str = add_space_around_numbers(salary_str)
                salary_str=replace_space_between_numbers(salary_str)
                logging.debug(f"Cleaned string : {salary_str}")
                pattern = r'\b\d+(?:[.]\d+)?\b' # capture any float or int
                match = re.search(pattern, salary_str)
                pattern2 = r'\d+(\.\d+)?\s+(à|-|et)\s+\d+(\.\d+)?' #capture strings  like "18 à 25", "18,5 - 25,4" "18 et 25",
                                                                                               
                                                                                                 
                                                                                              
                match2 = re.search(pattern2, salary_str)
                if match2:
                    
                        logging.info(f"match 2 reached")
                        logging.debug(f"salary_str : {salary_str}")
                        pattern3= r'\d+\.\d+\.\d+'
                        match3 = re.search(pattern3, salary_str)
                        if match3:
                            def modify_match(match):
                                matched_str = match.group()  # Get the full match
                                # Retain only the first two groups of digits
                                return '.'.join(matched_str.split('.')[:2])

                            # Replace the matched pattern with the modified result
                            salary_str = re.sub(pattern3, modify_match, salary_str)
                        
                        #salary_str=add_space_before_slash(salary_str)
                        salary_str = add_space_around_numbers(salary_str)
                        if "%" in salary_str:
                            min_monthly_salary = 0
                            max_monthly_salary = 0
                            salary_description = salary_str
                        elif "à" in salary_str or "-" in salary_str or " et " in salary_str:
                            logging.debug(f"Match 2, à/-et found")
                            min_monthly_salary, max_monthly_salary, salary_description = format_salaries(salary_str)  
                        else:
                            logging.debug(f"Match 2, exit")
                            min_monthly_salary = 0
                            max_monthly_salary = 0
                            salary_description = salary_str
                            
                            
                elif match:
                    min_monthly_salary = match.group()
                    max_monthly_salary = None
                    if any(keyword in salary_str for keyword in ["%", "CN", "CCN", "CC", "convention", "Convention", "RTT", "Cnn", "13 mois", "Coef", "coef", "grille", "Grille", " 66", "ségur", "Ségur"]) or ( "13 " in salary_str and "mois" in salary_str):
                        logging.warning("Found %/convention/13e mois")
                        min_monthly_salary = 0
                        max_monthly_salary = 0
                        salary_description = salary_str
                    if isinstance(min_monthly_salary, str) and min_monthly_salary.endswith('.'):
    
                            logging.info(f"salary ends with '.'")
                            min_monthly_salary = min_monthly_salary[:-1]
                    min_monthly_salary = round(float(min_monthly_salary),1)
                    salary_description = salary_str
                    
                    logging.debug(f"salary found : {min_monthly_salary}")
                          
                    if min_monthly_salary > 9999:
                    
                            min_monthly_salary /= 12
                    elif min_monthly_salary < 70:
                            min_monthly_salary *= hpw * WEEKS_PER_MONTH
                    
                else:
                    logging.warning(f"No salary found")
                    min_monthly_salary = 0
                    max_monthly_salary = 0
                    salary_description = salary_str
            except ValueError:
                logging.warning(f"Invalid salary format: {min_monthly_salary}")
                min_monthly_salary = 0
                max_monthly_salary = 0
                salary_description = 'invalid format'
            except Exception as e:
                logging.warning(f"Unexpected error during salary conversion: {e}")
                min_monthly_salary = 0
                max_monthly_salary = 0
                salary_description = 'conversion errror'
                
                
                   
                    
        else:
            # Convert the salary string to monthly min and max values
            salary_str = salary_str.replace("Euros","")
            salary_data = convert_salary_to_monthly(salary_str, job_offer)
            
            if not salary_data:
                min_monthly_salary = 0.0
                max_monthly_salary = 0.0
                salary_description = "Invalid salary format"
                logging.debug(f" salary_data failure")
            else:
                # Extract min and max salary from the converted data
                min_monthly_salary = salary_data.get('min_salary')
                max_monthly_salary = salary_data.get('max_salary')
                salary_description = salary_str
                
        
    except Error as e:
        logging.exception(f"Error inserting into salary: {e}")
        min_monthly_salary = 0.0
        max_monthly_salary = 0.0
        salary_description = "Invalid salary format"   
            
    try:
        # Insert data into the salary table
        insert_query = """
        INSERT  INTO salary (job_id, min_monthly_salary, max_monthly_salary, salary_description)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            min_monthly_salary = VALUES(min_monthly_salary),
            max_monthly_salary = VALUES(max_monthly_salary),
            salary_description = VALUES(salary_description)
            
        """
        cursor.execute(insert_query, (job_id, min_monthly_salary, max_monthly_salary, salary_description))
        logging.debug(f"Executed query: {cursor._executed}")
        
        
        # Return the ID of the inserted or existing record
        if cursor.lastrowid:
            logging.info(f"Inserted into `salary`: {cursor.lastrowid}")
            return cursor.lastrowid
        else:
            cursor.execute("SELECT salary_id FROM salary WHERE min_monthly_salary = %s AND max_monthly_salary = %s AND salary_description = %s AND job_id = %s", 
                           (min_monthly_salary, max_monthly_salary, salary_description, job_id))
            logging.debug(f"Executed query: {cursor._executed}")
            result = cursor.fetchone()
            logging.info(f"Duplicate ignored in table `salary`: {result}")
            return result[0] if result else None
    except Error as e:  
        logging.exception(f"Error inserting into salary: {e}")
        return None
    
        
def hours_per_week(length_work_label):
            if length_work_label is None or length_work_label == "":
                return None, None
           
                
            elif "\n" in length_work_label and "H" in length_work_label :
                parts = length_work_label.split("\n")
                hpw = parts[0].strip()
                work_condition = parts[1].strip()
                
            elif "temps partiel -" in length_work_label:
                tokens = length_work_label.split()
                index = tokens.index("-") +1
                hpw = tokens[index]
                work_condition= "Temps partiel"
            elif  "\n" not in length_work_label and "H" in length_work_label:
                if "Travail" in length_work_label:
                    parts = length_work_label.split()
                    hpw=parts[0].strip()
                    work_condition= parts[1].strip()
                else:
                    hpw = length_work_label
                    work_condition = None
            elif "temps partiel" in length_work_label:
                hpw = "24"
                work_condition = "Temps partiel"
            elif "H Autre" in length_work_label:
                tokens = length_work_label.split()
                index = tokens.index("Autre") -1
                hpw = tokens[index]
                work_condition= "Autre"
            else: 
                hpw= 0
                work_condition = length_work_label
                
                
            if hpw:
                hpw = hpw.replace("H", ".00").replace(".00",".").replace(".30", ".5 ").replace(".15",".25 ").replace('.45',".75 ")
                hpw = hpw.split()[0]
                try:
                    hpw = float(hpw)  # Convert to float for duration
                except ValueError:
                # Handle cases where conversion fails (invalid format)
                    logging.exception(f"float conversion failed for hpw: {hpw} from string {length_work_label}")
                    work_condition = length_work_label
                    hpw=0
            
            return hpw, work_condition
            
            
def insert_contract(cursor, connection, job_id, job_offer: dict):
    
    
    
    try:
        
        def insert_contract_type(contract_type):
            try:
                
                insert_query="""
                INSERT IGNORE INTO contract_type (contract_type)
                VALUES (%s)
                """
                cursor.execute(insert_query, (contract_type,))
                logging.debug(f"Executed query: {cursor._executed}")
        
                if cursor.lastrowid:
                    logging.info(f"Inserted into `contract_type`: {cursor.lastrowid}")
                    return cursor.lastrowid
                else:
                    cursor.execute("SELECT contract_type_id FROM contract_type WHERE contract_type = %s", 
                           (contract_type,))
                    logging.debug(f"Executed query: {cursor._executed}")
                    result = cursor.fetchone()
                    logging.info(f"Duplicate ignored in table `contract_type`: {result}")
                    return result[0] if result else None
            except Error as e:
                logging.exception(f"Error inserting into contract_type: {e}")
                return None
        def insert_contract_nature(contract_nature):
            try:
                insert_query="""
                INSERT IGNORE INTO contract_nature (contract_nature)
                VALUES (%s)
                """
                cursor.execute(insert_query, (contract_nature,))
                logging.debug(f"Executed query: {cursor._executed}")
                
        
                if cursor.lastrowid:
                    logging.info(f"Inserted into `contract_type`: {cursor.lastrowid}")
                    return cursor.lastrowid
                else:
                    cursor.execute("SELECT contract_nature_id FROM contract_nature WHERE contract_nature = %s", 
                           (contract_nature,))
                    logging.debug(f"Executed query: {cursor._executed}")
                    result = cursor.fetchone()
                    logging.info(f"Duplicate ignored in table `contract_type`: {result}")
                    return result[0] if result else None 
                    
            except Error as e:
                logging.exception(f"Error inserting into contract_nature: {e}")
                return None    
        
        
        
        
        def extract_contract_label(contract_label):
            
            # Split contract label into label and work duration
            label = contract_label.split("-")[0].strip()
            work_duration = contract_label.split("-")[-1].strip()
    
            # Check for months in work_duration
            contract_match = re.search(r"(\d+)\s*Mois$", work_duration)
            # Check for days (with optional "(s)") in work_duration
            contract_match2 = re.search(r"(\d+)\s*(Jour(?:\(s\)))?", work_duration)
    
            # If work duration is in months
            if contract_match:
                logging.debug(f"contract_match found: {contract_match.group(1)}")
                work_duration = int(contract_match.group(1))  # Extract the number of months
                return label, work_duration
    
            # If work duration is in days (and optional "Jour(s)" is present)
            elif contract_match2:
                logging.debug(f"contract_match2 found: {contract_match2.group(1)}")
                work_duration = int(contract_match2.group(1))  # Extract the number of days
                work_duration = days_to_months(work_duration)  # Convert days to months
                return label, work_duration
    
            
           
            else:
                logging.debug(f"Default case for contract_label: {contract_label}")
                work_duration = 0
                return label, work_duration
                
        def is_partial(partial_time):
            
                 
            #convert "dureeTravailLibelleConverti" to a boolean
            if partial_time and "Temps partiel" in partial_time :
                return True
            else:
                return False
                
        
        
            
            
        insert_query="""
        INSERT  INTO job_contract(job_id, contract_type_id,
        contract_nature_id, work_duration, hours_per_week, work_condition,
        additional_condition, partial, label)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )
        ON DUPLICATE KEY UPDATE
        contract_type_id = VALUES(contract_type_id),
        contract_nature_id = VALUES(contract_nature_id),
        work_duration = VALUES(work_duration),
        hours_per_week = VALUES(hours_per_week),
        work_condition = VALUES(work_condition),
        additional_condition = VALUES(additional_condition),
        partial = VALUES(partial),
        label = VALUES(label)
        """


    
        
        hpw, work_condition = hours_per_week(job_offer.get("dureeTravailLibelle"))
        partial =  is_partial(job_offer.get("dureeTravailLibelleConverti"))     
        contract_label= job_offer.get("typeContratLibelle")
        label, work_duration = extract_contract_label(contract_label)
        contract_nature_id=insert_contract_nature(job_offer.get("natureContrat"))
        contract_type_id=insert_contract_type(job_offer.get("typeContrat"))
        cursor.execute(insert_query, (job_id, contract_type_id, contract_nature_id, work_duration, hpw, work_condition,
        job_offer.get("experienceCommentaire"), partial, label))
        logging.debug(f"Executed query: {cursor._executed}")
        logging.info(f"Inserted into `job_contract`: {cursor.lastrowid}")
    
    except Error as e:
        logging.exception(f"Error inserting into job_contract: {e}")
        
        
        

        
def insert_benefits(cursor, connection, salary_id, job_offer: dict):
   
    """ insert a record inside the benefits tables"""
    
    complements= job_offer.get("salaire", {})
    try:
        # Extract complements
        complement1 = complements.get("complement1", None)
        complement2 = complements.get("complement2", None)
        
        # Prepare the insert query for insert_benefits
        insert_query = """
        INSERT IGNORE INTO benefits (label)
        VALUES (%s)
        """
        insert_query2= """
        INSERT IGNORE INTO salary_benefits (benefits_id, salary_id)
        VALUES (%s, %s)
        """
        
        
        # Insert complement1 if it exists
        def insert_and_link_benefit(benefit_label, salary_id):
            if benefit_label:
                # Insert the benefit into the `benefits` table
                cursor.execute(insert_query, (benefit_label,))
                logging.debug(f"Executed query: {cursor._executed}")
                 
                # Retrieve the benefit_id of the inserted or existing benefit
                if cursor.lastrowid:
                    benefit_id = cursor.lastrowid
                    logging.info(f"Inserted into `benefits`: {cursor.lastrowid}")
                else:
                    cursor.execute("SELECT benefits_id FROM benefits WHERE label = %s", (benefit_label,))
                    logging.debug(f"Executed query: {cursor._executed}")
                    result = cursor.fetchone()
                    benefit_id = result[0] if result else None
                    logging.info(f"Duplicate ignored in `benefits`: {result}")
                # Link the benefit to the job in the `job_benefit` table
                if benefit_id:
                    cursor.execute(insert_query2, (benefit_id, salary_id))
                    logging.debug(f"Executed query: {cursor._executed}")
                    logging.info(f"Inserted into `salary_benefit`: {cursor.lastrowid}")
                
        
        # Insert and link complement1 if it exists
        insert_and_link_benefit(complement1, salary_id)
        
        # Insert and link complement2 if it exists
        insert_and_link_benefit(complement2, salary_id)
        
        
        
    except Error as e:
        logging.warning(f"Error inserting into benefits: {e}")


        
def insert_salary_benefits(cursor, connection, benefit_id, salary_id):
    
    """
    Insert a record into the `salary_benefits` table.
    If a record with the same salary_id and salary_benefit_id exists, update the existing record.
    """
    try:
        
        insert_query = """
        INSERT IGNORE INTO salary_benefits (salary_benefits_id, salary_id)
        VALUES (%s, %s)
        """
        cursor.execute(insert_query, (benefit_id, salary_id))
        logging.debug(f"Executed query: {cursor._executed}")
        
        logging.info(f"Inserted salary_benefit for salary_id: {salary_id} and benefit_id: {benefit_id}") 
    except Error as e:
        logging.exception(f"Error inserting salary benefit: {e}")

def insert_companies(cursor, connection, job_offer: dict ):
    """Insert a record into the `companies` table."""
    
    try:
        
        insert_query = """
        INSERT IGNORE INTO companies ( name, is_adapted)
        VALUES ( %s, %s)
        """
        cursor.execute(insert_query, (job_offer.get("entreprise", {}).get("nom",'null'), job_offer.get("entreprise", {}).get("entrepriseAdaptee",0))
        )
        logging.debug(f"Executed query: {cursor._executed}")
        if job_offer.get("entreprise", {}).get("nom",'null') == 'null':
            return None
        
        if cursor.lastrowid:
            company_id = cursor.lastrowid
            logging.info(f"Inserted into `companies`: {cursor.lastrowid}")
            return company_id
        else:
                cursor.execute("SELECT company_id FROM companies WHERE name = %s", (job_offer.get("entreprise", {}).get("nom"),))
                logging.debug(f"Executed query: {cursor._executed}")
                result = cursor.fetchone()
                company_id = result[0] if result else None
                logging.info(f"Duplicate ignored in `companies`: {result}")
                return company_id
    except Error as e:
        logging.exception(f"Error inserting into companies: {e}")
        
def insert_contact(cursor, connection, job_offer: dict ):
    """Insert a record into the `contact` table."""
    
    coordonnees1=job_offer.get("contact", {}).get("coordonnees1", "")
    coordonnees2=job_offer.get("contact", {}).get("coordonnees2", "")
    coordonnees3=job_offer.get("contact", {}).get("coordonnees3", "")
    if coordonnees1 == coordonnees2 == coordonnees3:
        coordonnees2 = ''
        coordonnees3 = ''
        address=''
    elif coordonnees1 == coordonnees2:
        coordonnees2 = ''
    elif coordonnees2 == coordonnees3:
        coordonnees3 = ''
    address = " ".join(filter(None, [coordonnees1, coordonnees2, coordonnees3])).strip()
    try:
        
        insert_query = """
        INSERT IGNORE INTO contact (name, email, address)
        VALUES (%s, %s, %s)
        
        """
        if not job_offer.get("contact", {}).get("nom")  and not job_offer.get("contact", {}).get("courriel") and  not address.strip():
            return None
        cursor.execute(insert_query, (job_offer.get("contact", {}).get("nom"), job_offer.get("contact", {}).get("courriel"), address))
        logging.debug(f"Executed query: {cursor._executed}")
        logging.info(f"Inserted into `contact`: {cursor.lastrowid}")
        if cursor.lastrowid:
            contact_id = cursor.lastrowid
            logging.info(f"Inserted into `contact`: {cursor.lastrowid}")
            return contact_id
        else:
                cursor.execute("SELECT contact_id FROM contact WHERE  name = %s AND email = %s AND address = %s", (job_offer.get("contact", {}).get("nom"), job_offer.get("contact", {}).get("courriel"), address ))
                logging.debug(f"Executed query: {cursor._executed}")
                result = cursor.fetchone()
                contact_id = result[0] if result else 0
                logging.info(f"Duplicate ignored in `contact`: {result}")
                return  contact_id
    except Error as e:
        logging.exception(f"Error inserting into contact: {e}")

def insert_competencies(cursor, connection, job_offer: dict):
    """Insert a record into the `competencies` table."""
   
    
    try:
        
        insert_query = """
        INSERT IGNORE INTO competencies (competency_code, label)
        VALUES (%s, %s)

        
        """
        
        
        competencies = job_offer.get("competences", [])
        for competence in competencies:
                cursor.execute(insert_query, (
                competence.get("code"),
                competence.get("libelle")
                
            ))
                logging.debug(f"Executed query: {cursor._executed}")    
        
        
        logging.info(f"Inserted into `competencies`: {cursor.lastrowid}")
        
    except Error as e:
        logging.exception(f"Error inserting into competencies: {e}")
        
def insert_job_competency(cursor, connection, job_id, job_offer: dict):
    """Insert a record into the `job_competencies` table."""
   
    try:
        
        insert_query = """
        INSERT IGNORE INTO job_competency (competency_code, job_id, required)
        VALUES (%s, %s, %s)
        """
        competencies = job_offer.get("competences", [])
        for competence in competencies:
                cursor.execute(insert_query, (
                competence.get("code"),
                job_id,
                competence.get("exigence")))
                logging.debug(f"Executed query: {cursor._executed}")
                 
        
        
        logging.info(f"Inserted into `job_competencies`: {cursor.lastrowid}")
    except Error as e:
        logging.exception(f"Error inserting into job_competencies: {e}")   
def insert_driver_license(cursor, connection, job_offer: dict):
    
    try:
        
       libelle = job_offer.get("permis", [{}])[0].get("libelle")
       if libelle:
            insert_query = """
            INSERT IGNORE INTO driver_license (label)
            VALUES (%s)
            """
            cursor.execute(insert_query, (libelle,))
            logging.debug(f"Executed query: {cursor._executed}")
            
       if cursor.lastrowid:
            logging.info(f"Inserted into `driver_license`: {cursor.lastrowid}")
            return cursor.lastrowid
       else:
            cursor.execute("SELECT driver_license_id FROM driver_license WHERE label = %s", (libelle,))
            logging.debug(f"Executed query: {cursor._executed}")
            result = cursor.fetchone()
            logging.info(f"Duplicate found in `driver_license`: {result}")
            return result[0] if result else None
            
    except Error as e:
        logging.exception(f"Error inserting into driver_license: {e}")
        
def insert_job_driver_license(cursor, connection, job_id, driver_license_id, job_offer: dict ):
    
    try:
        req= job_offer.get("permis", [{}])[0].get("exigence")
        if req:
            insert_query= """
            INSERT IGNORE INTO job_driver_license(job_id, driver_license_id, requirement )
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (job_id, driver_license_id, req))
            logging.debug(f"Executed query: {cursor._executed}")
            logging.info(f"Inserted into `job_driver_license`:driver_licence_id: {driver_license_id} job_id {job_id}")
    except Error as e:
        logging.exception(f"Error inserting into job_driver_license: {e}")
        
def insert_job_node(cursor, connection, job_offer: dict, naf_label: dict):
     """Insert a record into the `job_node` table and its children naf, rome, actuvity sector & qualification."""
     
     def get_naf_libelle(code):
        return naf_label.get(code, None)
     try:
        
       
        
        insert_query_naf= """
        INSERT IGNORE INTO naf (naf_code, label)
        VALUES (%s, %s)
        """
        
        insert_query_rome= """
        INSERT IGNORE INTO rome (rome_code, label)
        VALUES (%s, %s)
        """
        
        insert_query_activity= """
        INSERT IGNORE INTO activity_sector (activity_sector_code, label)
        VALUES (%s, %s)
        """
        
        insert_query_qualification= """
        INSERT IGNORE INTO qualification (qualification_code, label)
        VALUES (%s, %s)
        """
       
        cursor.execute(insert_query_rome, (job_offer.get("romeCode"),job_offer.get("romeLibelle")))
        logging.info(f"Inserted into `rome`: {job_offer.get('romeCode')}")
        logging.debug(f"Executed query: {cursor._executed}")
        cursor.execute(insert_query_naf, (job_offer.get('codeNAF'),get_naf_libelle(job_offer.get('codeNAF'))))
        logging.info(f"Inserted into `naf`: {job_offer.get('codeNAF')}")
        logging.debug(f"Executed query: {cursor._executed}")
        cursor.execute(insert_query_activity, (job_offer.get("secteurActivite"),job_offer.get("secteurActiviteLibelle")))
        logging.info(f"Inserted into `sector_activity`: {job_offer.get('secteurActivite')}")
        logging.debug(f"Executed query: {cursor._executed}")
        cursor.execute(insert_query_qualification, (job_offer.get("qualificationCode"),job_offer.get("qualificationLibelle")))
        logging.info(f"Inserted into `qualification`: {job_offer.get('qualificationCode')}")
        logging.debug(f"Executed query: {cursor._executed}")
        connection.commit()
     except Error as e:
        logging.exception(f"Error inserting into job_node: {e}")
        connection.rollback()
        logging.error(f"Transaction rolled back due to error: {e}")
        
def insert_formation(cursor, connection, job_id, job_offer: dict ):
    
    
    
    try:
        formations= job_offer.get("formations", [])
        
            
        def insert_and_link_formations(formation_level, formation_label, job_id, requirement):
            formation_id = None
            nonlocal insert_query, insert_query2
            if formation_label:
                try:
                    # Insert the formation into the `formation` table
                    cursor.execute(insert_query, (formation_level, formation_label))
                    logging.debug(f"Executed query: {cursor._executed}")
                    
                    # Retrieve the formation_id of the inserted or existing formation
                    if cursor.lastrowid:
                        logging.info(f"Inserted into `formation`: {cursor.lastrowid}")
                        formation_id = cursor.lastrowid
                    else:
                        cursor.execute("SELECT formation_id FROM formation WHERE label = %s AND level = %s", (formation_label, formation_level))
                        logging.debug(f"Executed query: {cursor._executed}")
                        result = cursor.fetchone()
                        formation_id = result[0] if result else None
                        logging.info(f"Duplicate found in `formation`: {result}")
                except Error as e:
                    logging.exception(f"Error inserting into formaiton: {e}")
                # Link the formation to the job in the `job_formation` table
                if formation_id:
                    try:
                        cursor.execute(insert_query2, (job_id, formation_id, requirement))
                        logging.debug(f"Executed query: {cursor._executed}")
                        logging.info(f"Inserted into `job_formation`: {cursor.lastrowid}")
                    except Error as e:
                        logging.exception(f"Error inserting into job_formation: {e}")
               
    
        
        insert_query ="""
        INSERT IGNORE INTO formation (level, label)
        VALUES (%s, %s)
        """
        
        insert_query2="""
        INSERT IGNORE INTO job_formation (job_id, formation_id, requirement)
        VALUES (%s, %s, %s)
        """
        for formation in formations:
            insert_and_link_formations(formation.get("niveauLibelle"),
                                    formation.get("domaineLibelle"),
                                    job_id, formation.get("exigence"))
            logging.debug(f"Executed query: {cursor._executed}")
        
        
      
        
    except Error as e:
        logging.exception(f"Error inserting into job_formation: {e}")
        
def insert_professional_qualities(cursor, connection, job_id, job_offer: dict ):
    
    qualities= job_offer.get("qualitesProfessionnelles", [])
    def insert_and_link_qualities( quality_label,quality_desc, job_id):
            if quality_label:
                # Insert the quality into the `professional_quality` table
                cursor.execute(insert_query, (quality_label, quality_desc))
                logging.debug(f"Executed query: {cursor._executed}")
                
                # Retrieve the formation_id of the inserted or existing formation
                if cursor.lastrowid:
                    logging.info(f"Inserted into `professional_quality`: {cursor.lastrowid}")
                    professional_quality_id = cursor.lastrowid
                else:
                    cursor.execute("SELECT professional_quality_id FROM professional_qualities WHERE label = %s AND description = %s", (quality_label, quality_desc))
                    logging.debug(f"Executed query: job_id {job_id}")
                    result = cursor.fetchone()
                    professional_quality_id = result[0] if result else None
                    logging.info(f"Duplicate found`job_formation`: {result}")
                # Link the formation to the job in the `job_professional_qualities` table
                if  professional_quality_id:
                    cursor.execute(insert_query2, (professional_quality_id, job_id))
                    logging.debug(f"Executed query: {cursor._executed}")
                    logging.info(f"Inserted into `job_professional_qualities`: job_id {job_id}, job_professional_quality_id {professional_quality_id}")
                
    try:
        
        insert_query ="""
        INSERT IGNORE INTO professional_qualities(label, description)
        VALUES (%s, %s)
        """
        
        insert_query2="""
        INSERT IGNORE INTO job_professional_qualities(professional_quality_id, job_id)
        VALUES (%s, %s)
        
        """
        for quality in qualities:
            insert_and_link_qualities(quality.get("libelle"),
                                    quality.get("description"),
                                    job_id)
            logging.debug(f"Executed query: {cursor._executed}")
        
        
        
        
    except Error as e:
        logging.warning(f"Error inserting into professional_qualities: {e}")

def insert_languages(cursor, connection, job_id, job_offer: dict):
    
    languages= job_offer.get("langues", [])
    def insert_and_link_languages( label, job_id, requirement):
            if label:
                # Insert the quality into the `languages` table
                cursor.execute(insert_query, (label,))
                logging.debug(f"Executed query: {cursor._executed}")
                
                # Retrieve the language_id of the inserted or existing formation
                if cursor.lastrowid:
                    logging.info(f"Inserted into `language`: {cursor.lastrowid}")
                    language_id = cursor.lastrowid
                else:
                    cursor.execute("SELECT language_id FROM language WHERE label = %s ", (label,))
                    logging.debug(f"Executed query: {cursor._executed}")
                    result = cursor.fetchone()
                    language_id = result[0] if result else None
                    logging.info(f"Duplicate found in `languages`: {result}")
                # Link the language to the job in the `job_languages` table
                if  language_id:
                    cursor.execute(insert_query2, (job_id, language_id, requirement))
                    logging.debug(f"Executed query: {cursor._executed}")
                    logging.info(f"Inserted into `job_language`: {cursor.lastrowid}")
                
    try:
        
        insert_query ="""
        INSERT IGNORE INTO language(label)
        VALUES (%s)
        """
        
        insert_query2="""
        INSERT IGNORE INTO job_language( job_id, language_id, requirement)
        VALUES (%s, %s, %s)
        
        """
        for language in languages:
            insert_and_link_languages(language.get("libelle"),job_id,language.get("exigence"))
        
        
        
        
    except Error as e:
        logging.exception(f"Error inserting into languages: {e}")
    
    
def insert_moving(cursor, connection, job_offer: dict):
    
    try:
      
        insert_query="""
        INSERT IGNORE INTO moving( moving_code, label)
        VALUES (%s, %s)
        
        """
        cursor.execute(insert_query, (job_offer.get("deplacementCode",1),job_offer.get("deplacementLibelle","Jamais")))
        logging.debug(f"Executed query: {cursor._executed}")
        logging.info(f"Inserted into `moving`: {cursor.lastrowid}")
       
        
    except Error as e:
        logging.exception(f"Error inserting into moving: {e}")
        
def insert_cities(cursor, connection, job_offer: dict):
    
    travail=job_offer.get("lieuTravail", {})
    
    def process_location(location_string):
        """
        Processes the location string to extract the city and arrondissement (if present).
        Returns:
        - city: The name of the city.
        """
        # Extract the part after the hyphen
        location_part = location_string.split('-')[-1].strip()

        
            
        return location_part
        
        
        
    
        
    try:

        insert_query="""
        INSERT IGNORE INTO cities( insee_code, name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        
        """
        city = process_location(travail.get("libelle"))
        
        cursor.execute(insert_query, (travail.get("commune","75056"), city, travail.get("latitude"), 
        travail.get("longitude")))
        logging.debug(f"Executed query: {cursor._executed}")
        
        
        logging.info(f"Inserted into `cities`: {city}")
       
    except Error as e:
         logging.exception(f"Error inserting into cities: {e}")
        
def insert_requirements(cursor, connection):
    
    insert_query="""
    INSERT IGNORE INTO requirements (requirements, label)
    VALUES("E", "Exige"),("S", "Souhaite"),("D", "Debutants acceptes")
    """
    
    cursor.execute(insert_query)
    logging.debug(f"Executed query: {cursor._executed}")
    logging.info(f"Inserted into `requirements`: ")

    
""" def generate_uris(department_code, rome_code, token_type, token):
    all_job_offers = []
    first_index = 0
    last_index = 149
    page_size = 150
    range_param = f"{first_index}-{last_index}"
    uris = set()

    
    headers = {"Accept": "application/json",
        "Authorization": f"{token_type} {token}"}
    
    
    
    base_uri = f"https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search?departement={department_code}&romeCode={rome_code}"
    response = requests.get(f"{base_uri}?range={range_param}", headers=headers)
    content_range = response.headers.get("Content-Range")  # ex: "offres 0-149/18194"

    if content_range and "/" in content_range:
        try:
            total_items = int(content_range.split("/")[-1])
        except ValueError:
            total_items = 0
    else:
        total_items = 0 

    for first_index in range(0, total_items, page_size):
        last_index = min(first_index + page_size - 1, total_items - 1)
        range_param = f"{first_index}-{last_index}"
        uri = f"{base_uri}&range={range_param}"
        uris.add(uri)  # Add the URI to the set to avoid duplicates
    return uris """


    
    

    

def fetch_all_job_offers(uris, token_manager):

   
    all_job_offers = []


    for uri in uris:
        token, token_type = token_manager.get_token()
        headers = {"Accept": "application/json",
        "Authorization": f"{token_type} {token}"}
        
        # Initialize retry counter for the URI if not already
        if uri not in uri_retry_count:
            uri_retry_count[uri] = 0

    for uri in uris:
        
        # Make the request with the current range
        
        response = requests.get(uri,
        headers=headers,
        timeout=30)
        
        if response.status_code in [ 200, 206 ]:
            data = response.json()
            job_offers = data.get("resultats", [])
            if not job_offers:
                continue  # Skip if no job offers are found

            all_job_offers.extend(job_offers)

            

            
            

        elif response.status_code == 429:
            # Retry limit check
            if uri_retry_count[uri] < MAX_RETRIES:  # Try 3 times max
                if uri not in already_queued_uris:
                    retry_queue.put(uri)
                    already_queued_uris.add(uri)
                    uri_retry_count[uri] += 1  # Increment retry count
            else:
                logging.warning(f"URI {uri} reached retry limit (3 attempts).") # Add the uri to the queue for retrying later


        else:
            logging.warning(f"Warning {response.status_code}: {response.text}")
            break
    
    return all_job_offers


def fetch_job_offers_retries():
    uris = set()
    while not retry_queue.empty():
        uri = retry_queue.get()
        uris.add(uri) 
    return uris # Add the URI to the set to avoid duplicates
       

def establish_connection():
    
    
    
    logging.basicConfig(
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
            logging.StreamHandler()      # Log to the console
    ]
    )
    
    es_client = get_Elasticsearch()
    es_handler = ElasticsearchHandler(es_client, index_name="extraction_logs")
    es_handler.addFilter(ExcludeElasticsearchLogs())
    
    
    
    

    try:
        
        # Use the get_db function as a context manager to get the cursor and connection
        cursor, connection = get_db_persistent()
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return cursor, connection
    except Error as e:
        logging.exception(f"Error while connecting to MySQL: {e}")
        return None, None
    
def close_connection(cursor, connection):
    """Close the global connection and cursor."""
    
    if cursor:
        cursor.close()  # Close the cursor if it exists
    if connection and connection.is_connected():
        connection.close()
        logging.info("Connection closed")

def load_department_codes(csv_file_path: str) -> list[str]:
    """
    Loads a list of department codes from a CSV file.
    
    :param csv_file_path: Path to the CSV file containing department codes.
    :return: A list of department codes.
    """
    department_codes = []
    
    try:
        with open(csv_file_path, mode="r", newline="", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)  # Read the CSV as a dictionary (for better column access)
            
            # Iterate through each row and append the department code to the list
            for row in csv_reader:
                department_codes.append(row['Department_Code'])  
        
        return department_codes
    except FileNotFoundError:
        logging.exception(f"File not found: {csv_file_path}")
        raise
    except KeyError:
        logging.exception("Error: CSV file does not contain 'department_code' column.")
        raise
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        raise

def throttled_fetch(department_code,  token_manager, db_is_loaded):
    with semaphore:
        
        result = generate_uris_by_date_range(department_code, token_manager, db_is_loaded)
        time.sleep(0.1)  # to keep average rate under 10/sec
        return result


def process_department(department_code, token_manager, naf_labels, connection_pool, db_is_loaded):
    logging.info(f"Processing department: {department_code}")
    total_inserted = 0

    
    try:
        
        uris = throttled_fetch(department_code, token_manager, db_is_loaded)
        if not uris:
            logging.info(f"Aucune URI trouvée pour le département {department_code}.")
            return 0
        job_offers = fetch_all_job_offers(uris, token_manager)

        if job_offers:
            inserted = parallel_insert_all(job_offers, connection_pool, naf_labels, max_threads=5)
            nb_inserted = sum(1 for success in inserted if success)
            total_inserted += nb_inserted
            logging.info(f"{nb_inserted} offres insérées pour  / {department_code}")
        else:
            logging.info(f"Aucune offre récupérée pour / {department_code}")

    except Exception as e:
        logging.exception(f"Erreur pendant le traitement de {department_code}  : {e}")

    return total_inserted

def retry_failed_requests(token_manager, naf_labels, connection_pool):
    
    retry_uris = fetch_job_offers_retries()
    logging.info(f"Nombre total d'URIs à réessayer : {len(retry_uris)}")

    if not retry_uris:
        return 0

    job_offers = fetch_all_job_offers(retry_uris, token_manager)
    if not job_offers:
        return 0

    inserted = parallel_insert_all(job_offers, connection_pool, naf_labels, max_threads=5)
    nb_inserted = sum(1 for success in inserted if success)
    logging.info(f"{nb_inserted} offres insérées après retry.")
                   


    
def get_naf_rome_dept(token, token_type, csv_file_path):
    naf_labels = get_naf_labels(token_type, token)
    """rome_codes = get_rome_codes(token_type, token)"""
    department_codes = load_department_codes(csv_file_path)
    return naf_labels, department_codes 
    
def load_data_to_db( csv_file_path, token_manager, db_is_loaded):
    
    

    # Authentification
    
    token, token_type = token_manager.get_token()


    naf_labels, department_codes = get_naf_rome_dept(token, token_type, csv_file_path)

    logging.info(f"{len(department_codes)} départements à traiter")

    # Thread pool
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [
            executor.submit(
                process_department,
                department_code,
                token_manager,
                
                naf_labels,
                connection_pool,
                db_is_loaded
            )
            for department_code in department_codes
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Erreur dans un thread département : {e}")

def insert_all_data(cursor, connection, job_offer: dict, naf_labels: dict):
    """
    Insert all data into the database.
    """
    try:
        insert_cities(cursor, connection, job_offer)
        
        
        insert_job_node(cursor, connection, job_offer, naf_labels)
       
        
        
        insert_moving(cursor, connection, job_offer)
      
        company_id = insert_companies(cursor, connection, job_offer)
       

        job_id = insert_job(cursor, connection, job_offer, company_id)
        if not job_id:
            return False
       

        insert_contract(cursor, connection, job_id, job_offer)
 
        salary_id = insert_salary(cursor, connection, job_id, job_offer)
       
        if salary_id:
            insert_benefits(cursor, connection, salary_id, job_offer)
      

        insert_competencies(cursor, connection, job_offer)
       
        insert_job_competency(cursor, connection, job_id, job_offer)
        
        driver_license_id = insert_driver_license(cursor, connection, job_offer)
        
        if driver_license_id:
            insert_job_driver_license(cursor, connection, job_id, driver_license_id, job_offer)
        
        insert_formation(cursor, connection, job_id, job_offer)
        
        insert_professional_qualities(cursor, connection, job_id, job_offer)
        
        insert_languages(cursor, connection, job_id, job_offer)
        
        connection.commit()
        logging.info(f"Data inserted successfully for job offer ID: {job_offer.get('id')}, job ID: {job_id}")
        id_inserted.add(job_offer.get("id"))
        return True
    except Error as e:
        logging.exception(f"Error inserting data: {e}")
        connection.rollback()
        return False

def insert_job_offer_threadsafe(job_offer: dict, connection_pool, naf_labels) -> bool:
    connection = None
    cursor = None
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()

        success = insert_all_data(cursor, connection, job_offer, naf_labels)
        return success

    except Exception as e:
        logging.error(f"Erreur lors de l'insertion: {e}")
        return False

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def parallel_insert_all(job_offers, connection_pool, naf_labels, max_threads=5):
    results = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [
            executor.submit(insert_job_offer_threadsafe, offer, connection_pool, naf_labels)
            for offer in job_offers
        ]
        for future in as_completed(futures):
            results.append(future.result())
    return results



# def Extract_data(OUTPUT_DIR):
    # credentials = get_credentials(OUTPUT_DIR=OUTPUT_DIR)
    # client_id = credentials["clientID"]
    # client_secret = credentials["key"]

    # try : 
        # token, token_type = get_access_token(client_id, client_secret)
        # data, headers = requete_api(token_type= token_type,
                                    # token=token)
        # #print(json.dumps(data, indent=4))
        # #print(headers)
        # file_name=str(time.gmtime().tm_year*10000+time.gmtime().tm_mon*100+time.gmtime().tm_mday)+"_offre_demplois.json"
        # #data["resultats"]
        # print(json.dumps(data["resultats"][0],indent=4))
        # for doc in data["resultats"]:
            # with open(OUTPUT_DIR+"/Elasticsearch/requirements/logstash/data/to_ingest/"+file_name,"+a") as idFile:
                # json.dump(doc,idFile)
                # idFile.write("\n")
                # idFile.close()
    # except Exception as e:
        # print("Erreur :", e) 
        
def fill_missing_salaries(cursor, connection):
    """
    Fill missing salary values in four phases:
    Phase 1: Use average salary by rome_code and experience.
    Phase 2: Use average salary by rome_code.
    Phase 3: Use overall average salary.
    Phase 4: Fix very low salaries by multiplying by 1000.
    """
    try:
        # Fetch all rome_codes
        cursor.execute("SELECT rome_code FROM rome")
        rome_codes = [row[0] for row in cursor.fetchall()]
        experiences = ["D", "S", "E"]

        # Create a temporary table for performance optimization
        cursor.execute("""
            CREATE TABLE if not exists temp_avg_salaries AS
            SELECT b.rome_code, b.experience_required, 
                   ROUND(AVG(a.max_monthly_salary), 2) AS avg_max_salary,
                   ROUND(AVG(a.min_monthly_salary), 2) AS avg_min_salary
            FROM salary a
            JOIN job b ON a.job_id = b.job_id
            WHERE a.max_monthly_salary IS NOT NULL 
                  AND a.max_monthly_salary > 0
                  AND a.min_monthly_salary IS NOT NULL
                  AND a.min_monthly_salary > 0
            GROUP BY b.rome_code, b.experience_required
        """)
        connection.commit()

        # Phase 1: Update salary using experience-specific averages
        update_query = """
            UPDATE salary s
            JOIN temp_avg_salaries t ON s.job_id IN (
                SELECT job_id FROM job 
                WHERE rome_code = t.rome_code AND experience_required = t.experience_required
            )
            SET s.max_monthly_salary = t.avg_max_salary,
                s.min_monthly_salary = t.avg_min_salary
            WHERE (s.max_monthly_salary = 0 OR s.max_monthly_salary IS NULL)
              AND (s.min_monthly_salary = 0 OR s.min_monthly_salary IS NULL)
        """
        cursor.execute(update_query)
        connection.commit()
        logging.info("Update salaries phase 1 complete")

        # Phase 2: Use rome_code-level average salary
        cursor.execute("""
            UPDATE salary s
            JOIN (
                SELECT rome_code,
                       ROUND(AVG(avg_max_salary), 2) AS avg_max_salary,
                       ROUND(AVG(avg_min_salary), 2) AS avg_min_salary
                FROM temp_avg_salaries
                GROUP BY rome_code
            ) t ON s.job_id IN (SELECT job_id FROM job WHERE rome_code = t.rome_code)
            SET s.max_monthly_salary = t.avg_max_salary,
                s.min_monthly_salary = t.avg_min_salary
            WHERE (s.max_monthly_salary = 0 OR s.max_monthly_salary IS NULL)
              AND (s.min_monthly_salary = 0 OR s.min_monthly_salary IS NULL)
        """)
        connection.commit()
        logging.info("Update salaries phase 2 complete")

        # Phase 3: Use overall average salary if still missing
        cursor.execute("""
            UPDATE salary
            SET max_monthly_salary = (SELECT ROUND(AVG(avg_max_salary), 2) FROM temp_avg_salaries),
                min_monthly_salary = (SELECT ROUND(AVG(avg_min_salary), 2) FROM temp_avg_salaries)
            WHERE (max_monthly_salary = 0 OR max_monthly_salary IS NULL)
              AND (min_monthly_salary = 0 OR min_monthly_salary IS NULL)
        """)
        connection.commit()
        logging.info("Update salaries phase 3 complete")

        # Phase 4: Fix salaries lower than 10
        cursor.execute("""
            UPDATE salary
            SET min_monthly_salary = min_monthly_salary * 1000
            WHERE min_monthly_salary < 10
        """)
        connection.commit()
        logging.info("Update salaries phase 4 complete")

        # Drop the temporary table
        cursor.execute("DROP TABLE temp_avg_salaries")
        connection.commit()
        
    except Error as e:
        logging.exception(f"Error while updating salary: {e}")
        connection.rollback()
        logging.error(f"Transaction rolled back due to error: {e}")















def fix_salary_inconsistencies(cursor, connection):
    
    try:
        cursor.execute("UPDATE salary SET  max_monthly_salary = 0, min_monthly_salary = 0 WHERE max_monthly_salary >= 80000 or min_monthly_salary >= 80000")
        connection.commit()

        cursor.execute("UPDATE salary SET min_monthly_salary = 0, max_monthly_salary = 0  WHERE min_monthly_salary > max_monthly_salary")
        connection.commit()

        cursor.execute("""UPDATE salary SET max_monthly_salary = max_monthly_salary /10 
                        WHERE max_monthly_salary  > min_monthly_salary * 9 AND min_monthly_salary != 0
                    """)
        connection.commit()
        
        cursor.execute("""UPDATE salary SET max_monthly_salary = max_monthly_salary /10, 
                    min_monthly_salary = min_monthly_salary /10 where 
                    max_monthly_salary > 25000 AND min_monthly_salary > 10000
                    OR (min_monthly_salary > 20000 AND min_monthly_salary = max_monthly_salary)""")
        connection.commit()

       

        cursor.execute("UPDATE salary SET  max_monthly_salary = 0, min_monthly_salary =0 WHERE max_monthly_salary > 25000")
        connection.commit()

       
        logging.info("Salary inconsistencies fixed successfully.")
    except:
        logging.exception("Error while fixing salary inconsistencies")
        
        
        

    

    

def fix_missing_hours_per_week(cursor, connection):
    # fix missing partial_time boolean
    try:
        cursor.execute("UPDATE job_contract SET partial = 1 where work_condition ='Temps partiel'")
        connection.commit()
        # set default hours_per_week to 35 for full-time jobs
        cursor.execute("""UPDATE job_contract SET hours_per_week = 35 where 
                        (hours_per_week is null or hours_per_week = 0) AND 
                        (work_condition != 'Temps partiel' OR work_condition IS NULL)""")

        connection.commit()

        # set default hours_per_week to avg part-time for part-time jobs

        cursor.execute("""WITH partial_avg AS (
            SELECT
                ROUND(AVG(CASE WHEN b.contract_type = 'CDD' AND a.work_condition = 'Temps partiel' AND a.hours_per_week > 0 THEN a.hours_per_week END), 2) AS partial_cdd,
                ROUND(AVG(CASE WHEN b.contract_type = 'SAI' AND a.work_condition = 'Temps partiel' AND a.hours_per_week > 0 THEN a.hours_per_week END), 2) AS partial_sai,
                ROUND(AVG(CASE WHEN b.contract_type = 'DDI' AND a.work_condition = 'Temps partiel' AND a.hours_per_week > 0 THEN a.hours_per_week END), 2) AS partial_ddi,
                ROUND(AVG(CASE WHEN b.contract_type = 'MIS' AND a.work_condition = 'Temps partiel' AND a.hours_per_week > 0 THEN a.hours_per_week END), 2) AS partial_mis,
                ROUND(AVG(CASE WHEN b.contract_type = 'CDI' AND a.work_condition = 'Temps partiel' AND a.hours_per_week > 0 THEN a.hours_per_week END), 2) AS partial_cdi
            FROM job_contract a
            JOIN contract_type b ON a.contract_type_id = b.contract_type_id
        )
        UPDATE job_contract a
        JOIN contract_type b ON a.contract_type_id = b.contract_type_id
        JOIN partial_avg c ON TRUE
        SET a.hours_per_week = CASE
            WHEN b.contract_type = 'CDD' THEN c.partial_cdd
            WHEN b.contract_type = 'SAI' THEN c.partial_sai
            WHEN b.contract_type = 'DDI' THEN c.partial_ddi
            WHEN b.contract_type = 'MIS' THEN c.partial_mis
            WHEN b.contract_type = 'CDI' THEN c.partial_cdi
            ELSE a.hours_per_week
        END
        WHERE a.hours_per_week = 0
        AND a.work_condition = 'Temps partiel'
        AND b.contract_type IN ('CDD', 'SAI', 'DDI', 'MIS', 'CDI')""")
        connection.commit()
        logging.info("Missing hours_per_week fixed successfully.")
    except Error as e:
        logging.exception(f"Error while fixing missing hours_per_week: {e}")

def fix_missing_work_duration(cursor, connection): 
    try:
        cursor.execute("""WITH work_duration_avg AS (
            SELECT
                ROUND(AVG(CASE WHEN b.contract_type = 'CDD'  THEN a.work_duration END), 2) AS avg_cdd,
                ROUND(AVG(CASE WHEN b.contract_type = 'SAI'  THEN a.work_duration END), 2) AS avg_sai,
                ROUND(AVG(CASE WHEN b.contract_type = 'DDI'  THEN a.work_duration END), 2) AS avg_ddi,
                ROUND(AVG(CASE WHEN b.contract_type = 'MIS'  THEN a.work_duration END), 2) AS avg_mis
            FROM job_contract a
            JOIN contract_type b ON a.contract_type_id = b.contract_type_id
        )
        UPDATE job_contract a
        JOIN contract_type b ON a.contract_type_id = b.contract_type_id
        JOIN work_duration_avg c ON TRUE
        SET a.work_duration = CASE
            WHEN b.contract_type = 'CDD' THEN c.avg_cdd
            WHEN b.contract_type = 'SAI' THEN c.avg_sai
            WHEN b.contract_type = 'DDI' THEN c.avg_ddi
            WHEN b.contract_type = 'MIS' THEN c.avg_mis
            ELSE a.work_duration
        END
        WHERE a.hours_per_week = 0
        AND a.work_condition = 'Temps partiel'
        AND b.contract_type IN ('CDD', 'SAI', 'DDI', 'MIS')""")
        connection.commit()
        logging.info(" work_duration fixed successfully.")
    except Error as e:
        logging.exception(f"Error while fixing missing hours_per_week: {e}")

def log_to_es(index_name, log_message, level="INFO"):
    doc = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "level": level,
        "message": log_message
    }
    res = es.index(index=index_name, document=doc)
    return res



def main():
    project_root =os.getenv("DB_CREATOR_PATH")
    if project_root:
        
        OUTPUT_DIR = project_root
    else:
        log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_log.txt")
        OUTPUT_DIR = os.path.abspath(project_root)
    CSV_FILE_PATH = os.path.join(OUTPUT_DIR, 'french_departments.csv')  # relative path of the csv file
    
    # Redirect stdout to the log file
    try:
        
        cursor, connection = establish_connection()
        

        token_manager = TokenManager(OUTPUT_DIR)
        token, token_type = token_manager.get_token()
        cursor.execute("SELECT COUNT(*) FROM job")
        result = cursor.fetchone()
        if result[0] < 70000:
            logging.info("Table job is empty or has less than 70000 records. Proceeding with data loading.")
            db_is_loaded = False
        else:
            db_is_loaded = True
        
        naf_labels, _= get_naf_rome_dept(token, token_type, CSV_FILE_PATH)
        if not token:
            logging.error("Échec de l'obtention du token")
            return

        
        insert_requirements(cursor, connection)
        connection.commit()
        
        # departments list
        departments = load_department_codes(CSV_FILE_PATH)
        logging.info(f"Loaded {len(departments)} departments from {CSV_FILE_PATH}")

        
        load_data_to_db( CSV_FILE_PATH, token_manager, db_is_loaded)

        retry_failed_requests(token_manager, naf_labels, connection_pool)

        fix_salary_inconsistencies(cursor, connection)
        
        

        fix_missing_hours_per_week(cursor, connection)

        fix_missing_work_duration(cursor, connection)
        
        # clean up taken jobs
        
        
        
        connection.commit()

    finally:
        logging.info(f"Program completed. Logs are saved in 'output_log.txt'.")
        close_connection(cursor, connection)
        

if __name__ == "__main__":
   
    OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
    CSV_FILE_PATH = os.path.join(OUTPUT_DIR, 'french_departments.csv')  # relative path of the csv file
    main()
    
    

 