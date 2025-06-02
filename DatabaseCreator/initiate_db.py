




import os
import subprocess
import sys
from dotenv import load_dotenv
from DatabaseCreator.database import get_db_persistent
from mysql.connector import Error




def execute_sql_file(cursor, sql_file_path):
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        sql_commands = file.read()

    for command in sql_commands.split(';'):
        command = command.strip()
        if command:
            try:
                cursor.execute(command)
            except Error as e:
                print(f"[ERREUR] Commande SQL échouée : {command[:50]}... \n{e}")


def initiate_db(env_file_path: str, sql_file_path: str):
    if not os.path.isfile(env_file_path):
        raise FileNotFoundError(f"Fichier .env introuvable : {env_file_path}")

    if not os.path.isfile(sql_file_path):
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_file_path}")

    load_dotenv(dotenv_path=env_file_path)

    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME")

    if not db_name:
        raise ValueError("La variable d'environnement DB_NAME doit être définie.")

    # Vérifie si la base est déjà initialisée (optionnel, tu peux garder ta méthode existante)
    try:
        cursor, connection = get_db_persistent()
        cursor.execute("SELECT count(*) FROM job LIMIT 1")
        cursor.fetchall()
        if cursor.rowcount > 50000:
            print("La base de données est déjà initialisée. {cursor.rowcount} lignes trouvées.")
        cursor.close()
        connection.close()
        return
    except Exception:
        # Si erreur, on part sur l'init
        pass

    # Commande mysql à exécuter
    # Attention : passer le mot de passe en clair dans la commande est un risque en prod,
    # ici pour un usage local/test
    cmd = [
        "mysql",
        f"-h{db_host}",
        f"-P{db_port}",
        f"-u{db_user}",
        f"-p{db_password}" if db_password else f"-p",  # si vide, -p sans mdp (invite)
        db_name
    ]

    try:
        # Lancer la commande en redirigeant le fichier sql en stdin
        with open(sql_file_path, 'r') as sql_file:
            result = subprocess.run(cmd, stdin=sql_file, capture_output=True, text=True)

        if result.returncode != 0:
            print("[ERREUR] Import SQL échoué :", result.stderr)
            raise RuntimeError("Échec de l'import du fichier SQL")
        else:
            print("Base de données initialisée avec succès via la commande mysql.")
    except Exception as e:
        print("[ERREUR] Échec de l'initialisation de la base de données :", e)
        raise e