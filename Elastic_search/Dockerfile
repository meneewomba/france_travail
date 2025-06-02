# Utilise une image Python officielle
FROM python:3.10-slim

# Définir le dossier de travail dans le conteneur
WORKDIR /app

# Copier les fichiers nécessaires
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copier tous les scripts dans le conteneur
COPY ./Elastic_search /app/Elastic_search

# Point d'entrée par défaut (modifiable via docker-compose)
CMD ["python"]