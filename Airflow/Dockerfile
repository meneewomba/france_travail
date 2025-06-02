FROM apache/airflow:2.9.1-python3.11

# Installer les dépendances système nécessaires (optionnel)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow


COPY . /opt/airflow/project

# Ajouter le dossier projet au PYTHONPATH
ENV PYTHONPATH="/opt/airflow/project"
ENV AIRFLOW_VERSION=2.9.1



# Installer les dépendances Python
COPY Airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

RUN pip uninstall -y mysql-connector mysql-connector-python mysql-connector-repackaged && \
    pip cache purge && \
    pip install --no-cache-dir mysql-connector-python==8.0.33
