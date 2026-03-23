from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ==========================================
# CONFIGURATION ET VARIABLES
# ==========================================
# Clés API
WEATHER_API_KEY = "15b0f132c45b9e3a4958a43dc27b4d97"
AQI_API_KEY = "23ea76a0b52cff545173e8e8f6ed86bc51861b73"

CITIES = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille"]

# --- CONFIGURATION EMAIL D'ALERTE (PYTHON) ---
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "wahabmounirou38@gmail.com"
# REMPLACE par ton mot de passe d'application Google (16 lettres, sans espaces)
SENDER_PASSWORD = "dngh fmvl nerw mhyt" 
RECEIVER_EMAIL = "wahabmounirou38@gmail.com"

# Configuration par défaut du DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['alerte@goodair.fr'], 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Paramètres de connexion à la base de données
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

# ==========================================
# FONCTIONS D'EXTRACTION ET CHARGEMENT (ETL)
# ==========================================

def get_city_id(cursor, city_name):
    """Fonction utilitaire pour récupérer l'ID de la ville depuis la table dim_city"""
    cursor.execute("SELECT city_id FROM dim_city WHERE name = %s;", (city_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"La ville {city_name} n'existe pas dans la base de données.")

def extract_and_load_weather():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    for city in CITIES:
        logging.info(f"Récupération de la météo pour {city}...")
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_API_KEY}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            city_id = get_city_id(cursor, city)
            
            # Extraction des variables
            temp = data['main']['temp']
            humidity = data['main']['humidity']
            wind_speed = data['wind']['speed']
            weather_desc = data['weather'][0]['description']
            
            # Insertion dans la base
            insert_query = """
                INSERT INTO fact_weather (city_id, timestamp, temperature, humidity, wind_speed, weather_condition)
                VALUES (%s, NOW(), %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (city_id, temp, humidity, wind_speed, weather_desc))
        else:
            logging.error(f"Erreur API Météo pour {city}: {response.text}")
            raise Exception(f"L'API OpenWeatherMap a échoué pour {city}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Données météo chargées avec succès.")

def extract_and_load_air_quality():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    for city in CITIES:
        logging.info(f"Récupération de la qualité de l'air pour {city}...")
        url = f"https://api.waqi.info/feed/{city}/?token={AQI_API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'ok':
                city_id = get_city_id(cursor, city)
                iaqi = data['data']['iaqi']
                
                # Extraction avec gestion des valeurs manquantes (.get)
                aqi = data['data']['aqi']
                pm25 = iaqi.get('pm25', {}).get('v', None)
                pm10 = iaqi.get('pm10', {}).get('v', None)
                no2 = iaqi.get('no2', {}).get('v', None)
                o3 = iaqi.get('o3', {}).get('v', None)
                
                insert_query = """
                    INSERT INTO fact_air_quality (city_id, timestamp, aqi, pm25, pm10, no2, o3)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (city_id, aqi, pm25, pm10, no2, o3))
            else:
                logging.error(f"Erreur dans les données AQI pour {city}: {data.get('data')}")
                raise Exception(f"Données invalides retournées par l'API AQI pour {city}")
        else:
            logging.error(f"Erreur HTTP API AQI pour {city}")
            raise Exception(f"L'API AQI a échoué pour {city}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Données de qualité de l'air chargées avec succès.")

# ==========================================
# FONCTION D'ALERTE (LOGS + EMAIL)
# ==========================================

def check_pollution_and_alert():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # On cherche les villes avec un AQI > 150 sur la dernière heure insérée : on met à >=50 pour tester les alertes même avec des données normales
    query = """
        SELECT c.name, a.aqi  
        FROM fact_air_quality a 
        JOIN dim_city c ON a.city_id = c.city_id 
        WHERE a.aqi >= 0 
        AND a.timestamp >= NOW() - INTERVAL '1 hour';
    """
    cursor.execute(query)
    anomalies = cursor.fetchall()
    
    if anomalies:
        # 1. Construction du corps de l'email
        corps_email = "Bonjour l'équipe GoodAir,\n\nVoici les alertes de pollution pour la dernière heure :\n\n"
        
        for ville, aqi in anomalies:
            message_alerte = f"🚨 La ville de {ville} a enregistré un AQI critique de {aqi} !"
            logging.warning(message_alerte) # Garde une trace dans Airflow
            corps_email += f"- {message_alerte}\n"
            
        corps_email += "\nMerci de vérifier le tableau de bord Metabase pour plus de détails.\n"
        
        # 2. Préparation du message SMTP
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = "🚨 GoodAir - Alerte Pic de Pollution"
        msg.attach(MIMEText(corps_email, 'plain'))
        
        # 3. Envoi de l'email via les serveurs de Google
        try:
            logging.info("Tentative d'envoi de l'email d'alerte...")
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls() # Sécurise la connexion (STARTTLS)
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
            server.quit()
            logging.info(f"✅ Email d'alerte envoyé avec succès à {RECEIVER_EMAIL}.")
        except Exception as e:
            logging.error(f"❌ Échec de l'envoi de l'email : {e}")
            
    else:
        logging.info("✅ Qualité de l'air normale pour toutes les villes. Aucune alerte déclenchée.")

    cursor.close()
    conn.close()

# ==========================================
# DEFINITION DU DAG AIRFLOW
# ==========================================

with DAG(
    'goodair_hourly_pipeline',
    default_args=default_args,
    description='Pipeline ETL horaire pour le projet GoodAir (Météo et Qualité Air)',
    schedule_interval='@hourly', 
    start_date=datetime(2023, 10, 1), 
    catchup=False, 
    tags=['goodair', 'etl'],
) as dag:

    # Définition des tâches
    task_fetch_weather = PythonOperator(
        task_id='extract_and_load_weather',
        python_callable=extract_and_load_weather,
    )

    task_fetch_air_quality = PythonOperator(
        task_id='extract_and_load_air_quality',
        python_callable=extract_and_load_air_quality,
    )

    task_check_alerts = PythonOperator(
        task_id='check_pollution_and_alert',
        python_callable=check_pollution_and_alert,
    )

    # Ordre d'exécution : On extrait d'abord les données (en parallèle), PUIS on vérifie les alertes
    [task_fetch_weather, task_fetch_air_quality] >> task_check_alerts