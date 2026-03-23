# On part de l'image officielle Airflow (adapte la version si besoin)
FROM apache/airflow:2.7.3

# On passe en root pour installer des dépendances système si nécessaire
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# On repasse sur l'utilisateur airflow pour installer les paquets Python
USER airflow

# Installation des librairies requises pour ton projet GoodAir
RUN pip install --no-cache-dir \
    requests \
    psycopg2-binary \
    pandas