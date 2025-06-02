# Mode d'emploi

## Pré requis
Avoir docker et wsl 2 installés
mysql pour accéder à la base de données 



## Configuration

créer un ficher clientCredentials.json dans le dossier DatabaseCreator avec le contenu suivant
{
    "clientID": "xxxxxxxxxxxxxxx",
    "key": "xxxxxxxxxxxxx"
}

Se connecter sur https://francetravail.io/
Créer un compte et une application
ajouter l'api offres d'emploi v2

récupérer les identifiants et les copier dans le fichier json à la place des xxxxxxxxx.

Créer un fichier .env à la racine avec les variables suivantes :

DB_HOST=mysql
DB_NAME=mydb

DB_USER=xxxxx
DB_PASSWORD=xxxxxx

DB_PORT=3306
DB_CHARSET=utf8mb4

MYSQL_ROOT_PASSWORD=xxxxxxx

MYSQL_ALLOW_EMPTY_PASSWORD=yes
MLFLOW_TRACKING_URI=http://mlflow:5000
ES_HOST=elasticsearch
ES_PORT=9200

ES_USER=xxxxxx
ELASTIC_PASSWORD=xxxxxx

AIRFLOW_UID=50000
AIRFLOW_GID=0
PROJECT_ROOT=/opt/airflow/project
DB_CREATOR_PATH=/opt/airflow/project/DatabaseCreator
API=fastapi

remplacer les champs xxxxxx par les identifiants de votre choix

Dupliquer ce fichier .env en le renommant .env.test et remplacer
DB_NAME=mydb
par 
DB_NAME=fr_test

Savegarder le tout.




## Lancement 
Lancer le déploiement en se positionnant à la racine du dossier avec docker compose up 

## Acces

### accès à l'api 

localhost:8000
#### endpoints : 
/search/   [POST]
##### payload 

{"must_contain": [],
        "contain_any": [],
        "not_contain": []
        }

/predict [POST]
##### payload

{"job_ids": []}

### accès base de données 

dans cmd
mysql -u xxxx -pxxxxx mydb

### accès airflow
localhost:8080 
admin/admin

### accès grafana
localhost:3000


