"""
Exercice 3 : Alertes météo Kafka

Créer un DAG exécuté toutes les minutes qui :
1. Récupère les données météo actuelles
2. Analyse la température et la vitesse du vent
3. Produit des alertes Kafka selon les règles :
   - Cold alert si : température < 5°C ET vitesse du vent > 20 km/h
   - Hot alert si : température > 35°C
4. Chaque message contient : ville, timestamp, température, vitesse du vent, code météo, type d'alerte
"""

from datetime import datetime, timedelta
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)


# ==============================================================================
# TÂCHE 1 : Récupérer les données météo actuelles
# ==============================================================================
@task
def fetch_current_weather(city='Paris'):
    """
    Récupère les données météo actuelles pour une ville.
    Utilise les coordonnées fixes de Paris.
    """
    print(f"Récupération de la météo actuelle pour {city}")
    
    # Coordonnées de Paris
    latitude = 48.8566
    longitude = 2.3522
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': 'temperature_2m,wind_speed_10m,weather_code',
        'timezone': 'Europe/Paris',
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        current = data['current']
        
        weather_info = {
            'city': city,
            'timestamp': datetime.now().isoformat(),
            'latitude': latitude,
            'longitude': longitude,
            'temperature_celsius': current['temperature_2m'],
            'wind_speed_kmh': current['wind_speed_10m'],
            'weather_code': current['weather_code'],
            'timezone': data['timezone'],
        }
        
        print(f"Météo reçue pour {city}:")
        print(f"  Température : {weather_info['temperature_celsius']}°C")
        print(f"  Vitesse du vent : {weather_info['wind_speed_kmh']} km/h")
        print(f"  Code météo : {weather_info['weather_code']}")
        
        return weather_info
    
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération météo : {e}")
        raise AirflowException(f"Impossible de récupérer les données météo : {e}")


# ==============================================================================
# TÂCHE 2 : Analyser les données et générer les alertes
# ==============================================================================
@task
def generate_alerts(weather_info):
    """
    Analyse la météo et génère les alertes selon les règles.
    
    Règles :
    - Cold alert : température < 5°C ET vitesse du vent > 20 km/h
    - Hot alert : température > 35°C
    """
    alerts = []
    
    temperature = weather_info['temperature_celsius']
    wind_speed = weather_info['wind_speed_kmh']
    
    # Vérifier condition de froid
    if temperature < 5 and wind_speed > 20:
        alert = {
            'city': weather_info['city'],
            'timestamp': weather_info['timestamp'],
            'temperature': temperature,
            'wind_speed': wind_speed,
            'weather_code': weather_info['weather_code'],
            'alert_type': 'cold_alert',
            'alert_level': 'warning',
            'message': f"Attention : températures très froides ({temperature}°C) avec vent fort ({wind_speed} km/h)",
        }
        alerts.append(alert)
        print(f"Alerte FROID générée : {alert['message']}")
    
    # Vérifier condition de chaleur
    if temperature > 35:
        alert = {
            'city': weather_info['city'],
            'timestamp': weather_info['timestamp'],
            'temperature': temperature,
            'wind_speed': wind_speed,
            'weather_code': weather_info['weather_code'],
            'alert_type': 'hot_alert',
            'alert_level': 'warning',
            'message': f"Attention : températures très chaudes ({temperature}°C)",
        }
        alerts.append(alert)
        print(f"Alerte CHALEUR générée : {alert['message']}")
    
    if not alerts:
        print("Aucune alerte générée - conditions normales")
    
    return alerts


# ==============================================================================
# TÂCHE 3 : Envoyer les alertes à Kafka
# ==============================================================================
@task
def send_alerts_to_kafka(alerts):
    """
    Envoie les alertes au topic Kafka 'weather-alerts'.
    """
    if not alerts:
        print("Aucune alerte à envoyer")
        return {'messages_sent': 0, 'status': 'success'}
    
    print(f"Envoi de {len(alerts)} alerte(s) à Kafka")
    
    try:
        # Créer le producteur Kafka
        # kafka:29092 est le listener PLAINTEXT interne du conteneur
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=10000,
        )
        
        messages_sent = 0
        
        # Envoyer chaque alerte
        for alert in alerts:
            try:
                # Envoyer le message au topic
                future = producer.send(
                    'weather-alerts',
                    value=alert,
                    key=alert['city'].encode('utf-8'),
                )
                
                # Attendre la confirmation
                record_metadata = future.get(timeout=10)
                
                print(f"Message envoyé - Topic: {record_metadata.topic}, "
                      f"Partition: {record_metadata.partition}, "
                      f"Offset: {record_metadata.offset}")
                
                messages_sent += 1
            
            except KafkaError as e:
                print(f"Erreur lors de l'envoi du message : {e}")
                raise
        
        # Fermer le producteur
        producer.flush()
        producer.close()
        
        result = {
            'messages_sent': messages_sent,
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
        }
        
        print(f"Alertes envoyées avec succès : {messages_sent} message(s)")
        return result
    
    except Exception as e:
        print(f"Erreur Kafka : {e}")
        raise AirflowException(f"Impossible d'envoyer les alertes à Kafka : {e}")


# ==============================================================================
# DAG PRINCIPAL
# ==============================================================================
@dag(
    dag_id='weather_alert_stream',
    description='Flux temps réel d\'alertes météo via Kafka',
    schedule='* * * * *',  # Toutes les minutes
    start_date=datetime(2026, 5, 20),
    catchup=False,
    tags=['weather', 'exercice3', 'kafka', 'alertes'],
)
def weather_alert_stream_dag():
    """
    DAG pour générer et publier des alertes météo sur Kafka.
    Exécution toutes les minutes.
    """
    
    # Pipeline d'exécution
    weather_data = fetch_current_weather(city='Paris')
    alerts = generate_alerts(weather_data)
    send_alerts_to_kafka(alerts)


# Instancier le DAG
dag = weather_alert_stream_dag()
