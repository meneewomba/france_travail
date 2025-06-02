





import os
from dotenv import load_dotenv
import requests
from DatabaseCreator.database import get_db_persistent
from concurrent.futures import ThreadPoolExecutor, as_completed
from more_itertools import chunked


def process_job_batch(job_ids):
    try:
        cursor, conn = get_db_persistent()
        root_project = os.getenv("PROJECT_ROOT")
        if  root_project:
            env_path= os.path.join(root_project, ".env")
        else:
            env_path= os.path.join(os.path.dirname(__file__), ".env")
            
        api = os.getenv("API", "localhost")
        
        load_dotenv(dotenv_path= env_path)
        response = requests.post(f"http://{api}:8000/predict",  json={"job_ids": job_ids})
        if response.status_code == 200:
            data = response.json()
            for job_id in job_ids:
                result = data.get(str(job_id)) or data.get(job_id)
                if not isinstance(result, dict) or "error" in result:
                    error_msg = result.get('error') if isinstance(result, dict) else str(result)
                    print(f"Erreur job_id {job_id}: {error_msg}")
                    continue

                min_salary_prediction = result.get('min_monthly_salary', 0)
                max_salary_prediction = result.get('max_monthly_salary', 0)

                cursor.execute("SELECT min_monthly_predicted, max_monthly_predicted FROM salary WHERE job_id = %s", (job_id,))
                result = cursor.fetchone() or (0, 0)
                min_monthly_predicted = result[0] or 0
                max_monthly_predicted = result[1] or 0

                if min_monthly_predicted == 0 and max_monthly_predicted == 0:
                    cursor.execute("""UPDATE salary SET min_monthly_predicted= %s, max_monthly_predicted = %s, 
                                    min_salary_pred_diff = 0, max_salary_pred_diff = 0, avg_min_diff = 0, avg_max_diff = 0  WHERE job_id = %s""",
                                (min_salary_prediction, max_salary_prediction, job_id))
                    print(f"Job_id {job_id} : min_salary_prediction = {min_salary_prediction}, max_salary_prediction = {max_salary_prediction}")
                else:
                    cursor.execute("SELECT avg_min_diff, avg_max_diff from salary where job_id = %s", (job_id,))
                    avg_min_diff_row = cursor.fetchone()
                    avg_min_diff = avg_min_diff_row[0] or 0 if avg_min_diff_row else 0
                    avg_max_diff = avg_min_diff_row[1] or 0 if avg_min_diff_row else 0

                    # Always calculate new_min_diff and new_max_diff
                    new_min_diff = round((min_salary_prediction - min_monthly_predicted) / min_monthly_predicted * 100, 2) if min_monthly_predicted != 0 else 0
                    new_max_diff = round((max_salary_prediction - max_monthly_predicted) / max_monthly_predicted * 100, 2) if max_monthly_predicted != 0 else 0

                    if avg_min_diff == 0 and avg_max_diff == 0:
                        cursor.execute("SELECT min_salary_pred_diff, max_salary_pred_diff from salary where job_id = %s", (job_id,))
                        min_max_diff_row = cursor.fetchone()
                        current_min_diff = min_max_diff_row[0] if min_max_diff_row and min_max_diff_row[0] is not None else 0.0
                        current_max_diff = min_max_diff_row[1] if min_max_diff_row and min_max_diff_row[1] is not None else 0.0            
                        avg_min_diff = round((new_min_diff + current_min_diff) / 2, 2)
                        avg_max_diff = round((new_max_diff + current_max_diff) / 2, 2)
                        cursor.execute("""UPDATE salary SET min_monthly_predicted= %s, max_monthly_predicted = %s,
                                        min_salary_pred_diff = %s, max_salary_pred_diff = %s, avg_min_diff = %s, avg_max_diff = %s WHERE job_id = %s""",
                                    (min_salary_prediction, max_salary_prediction, new_min_diff, new_max_diff, avg_min_diff, avg_max_diff, job_id))
                        print(f"Job_id {job_id} : min_salary_prediction = {min_salary_prediction}, max_salary_prediction = {max_salary_prediction}, min_diff ={new_min_diff} %', max_diff = {new_max_diff} %")
                    else:
                        cursor.execute("SELECT avg_min_diff, avg_max_diff from salary where job_id = %s", (job_id,))
                        current_avg_diff_row = cursor.fetchone()
                        current_avg_min_diff = current_avg_diff_row[0] or 0 if current_avg_diff_row else 0
                        current_avg_max_diff = current_avg_diff_row[1] or 0 if current_avg_diff_row else 0
                        avg_min_diff = round((new_min_diff + current_avg_min_diff) / 2, 2)
                        avg_max_diff = round((new_max_diff + current_avg_max_diff) / 2, 2)
                        cursor.execute("""UPDATE salary SET min_monthly_predicted= %s, max_monthly_predicted = %s,
                                        min_salary_pred_diff = %s, max_salary_pred_diff = %s, avg_min_diff = %s, avg_max_diff = %s WHERE job_id = %s""",
                                    (min_salary_prediction, max_salary_prediction, new_min_diff, new_max_diff, avg_min_diff, avg_max_diff, job_id))
                        print(f"Job_id {job_id} : min_salary_prediction = {min_salary_prediction}, max_salary_prediction = {max_salary_prediction}, min_diff ={new_min_diff} %', max_diff = {new_max_diff} %")
                        
            conn.commit()
    except Exception as e:
        print(f"Erreur traitement batch : {e}")
    finally:
        cursor.close()
        conn.close()


def update_predicted_salary_parallel_batch():
    cursor, conn = get_db_persistent()
    cursor.execute("SELECT job_id from salary where min_monthly_salary = 0 and max_monthly_salary = 0")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    job_ids = [row[0] for row in rows]
    batch_size = 1000

    with ThreadPoolExecutor(max_workers=20) as executor:  # exemple 5 threads max
        futures = []
        for batch in chunked(job_ids, batch_size):
            futures.append(executor.submit(process_job_batch, batch))

        for future in as_completed(futures):
            try:
                future.result()  # bloque et propage les exceptions éventuelles
            except Exception as e:
                print(f"Erreur dans un batch : {e}")

def daily_update_predicted_salary_parallel():
    cursor, conn = get_db_persistent()
    cursor.execute("SELECT job_id from salary where ((min_monthly_salary =0 and max_monthly_salary = 0) or (min_monthly_salary =0 and max_monthly_salary is null))")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    job_ids = [row[0] for row in rows]
    batch_size = 500
    remaining = len(job_ids)
    with ThreadPoolExecutor(max_workers=5) as executor:  # exemple 5 threads max
        futures = []
        for batch in chunked(job_ids, batch_size):
            remaining -= len(batch)
            print(f"Jobs restants : {remaining} - Traitement de {len(batch)} jobs")
            futures.append(executor.submit(process_job_batch, batch))

        for future in as_completed(futures):
            try:
                future.result()  # bloque et propage les exceptions éventuelles
            except Exception as e:
                print(f"Erreur dans un batch : {e}")


if __name__ == "__main__":
    daily_update_predicted_salary_parallel()
