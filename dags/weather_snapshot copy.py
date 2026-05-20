"""
Exercice 1: Weather Snapshot
Créer un DAG exécuté une fois par jour qui :
1. Récupère les coordonnées d'une ville via l'API Open-Meteo Geocoding
2. Récupère les données météo via l'API Open-Meteo Forecast
3. Extrait température moyenne, vitesse maximale du vent, précipitations
4. Génère un JSON et le sauvegarde
"""

from datetime import datetime, timedelta
import json
import requests
from pathlib import Path

from airflow.decorators import dag, task


# ==============================================================================
# TÂCHE 1 : Récupérer les coordonnées de la ville
# ==============================================================================
@task
def get_city_coordinates():
    """
    Récupère les coordonnées (latitude, longitude) d'une ville
    via l'API Open-Meteo Geocoding
    """
    city_name = 'Paris'  # Vous pouvez changer la ville
    
    print(f"Récupération des coordonnées pour : {city_name}")
    
    # Appel à l'API Open-Meteo Geocoding
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        'name': city_name,
        'count': 1,
        'language': 'en',
        'format': 'json'
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()  # Lève une exception si erreur HTTP
    
    data = response.json()
    
    # Extraire les coordonnées du premier résultat
    if 'results' in data and len(data['results']) > 0:
        result = data['results'][0]
        coordinates = {
            'city': result['name'],
            'country': result.get('country', 'Unknown'),
            'latitude': result['latitude'],
            'longitude': result['longitude'],
        }
        print(f"Coordonnées trouvées : {coordinates}")
        return coordinates
    else:
        raise ValueError(f"Ville '{city_name}' non trouvée")


# ==============================================================================
# TÂCHE 2 : Récupérer les données météo
# ==============================================================================
@task
def get_weather_data(coordinates):
    """
    Récupère les données météo pour 24h via l'API Open-Meteo Forecast
    """
    latitude = coordinates['latitude']
    longitude = coordinates['longitude']
    city = coordinates['city']
    
    print(f"Récupération météo pour {city} ({latitude}, {longitude})")
    
    # Appel à l'API Open-Meteo Forecast
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'hourly': 'temperature_2m,wind_speed_10m,precipitation,weather_code',
        'timezone': 'auto',
        'forecast_days': 1,
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    weather_data = response.json()
    print(f"Données météo reçues: {len(weather_data['hourly']['time'])} mesures horaires")
    
    return weather_data


# ==============================================================================
# TÂCHE 3 : Extraire et agréger les données
# ==============================================================================
@task
def process_weather_data(coordinates, weather_data):
    """
    Extrait les métriques clés des données météo
    """
    # Extraire les données horaires
    hourly_temps = weather_data['hourly']['temperature_2m']
    hourly_wind_speed = weather_data['hourly']['wind_speed_10m']
    hourly_precipitation = weather_data['hourly']['precipitation']
    
    # Calculer les agrégations
    avg_temperature = sum(hourly_temps) / len(hourly_temps)
    max_wind_speed = max(hourly_wind_speed)
    total_precipitation = sum(hourly_precipitation)
    
    # Créer le snapshot
    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'city': coordinates['city'],
        'country': coordinates['country'],
        'coordinates': {
            'latitude': coordinates['latitude'],
            'longitude': coordinates['longitude'],
        },
        'metrics': {
            'avg_temperature_celsius': round(avg_temperature, 2),
            'max_wind_speed_kmh': round(max_wind_speed, 2),
            'total_precipitation_mm': round(total_precipitation, 2),
        },
        'hourly_count': len(hourly_temps),
    }
    
    print(f"Snapshot créé : {json.dumps(snapshot, indent=2)}")
    
    return snapshot


# ==============================================================================
# TÂCHE 4 : Sauvegarder le snapshot
# ==============================================================================
@task
def save_snapshot(snapshot):
    """
    Sauvegarde le snapshot JSON dans le dossier partagé data/weather_snapshots/
    """
    # Chemin du dossier de sortie
    output_dir = Path('/opt/airflow/data/weather_snapshots')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Créer le nom du fichier avec timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_dir / f"snapshot_{snapshot['city'].lower()}_{timestamp}.json"
    
    # Sauvegarder le fichier
    with open(filename, 'w') as f:
        json.dump(snapshot, f, indent=2)
    
    print(f"Snapshot sauvegardé : {filename}")
    return str(filename)


# ==============================================================================
# Définir le DAG avec TaskFlow
# ==============================================================================
@dag(
    dag_id='weather_snapshot',
    description='Daily weather snapshot from Open-Meteo API',
    schedule='@daily',
    start_date=datetime(2026, 5, 19),
    catchup=False,
    tags=['weather', 'exercice1'],
)
def weather_snapshot_dag():
    """DAG principal pour le snapshot météo"""
    
    # Exécuter les tâches dans l'ordre
    coordinates = get_city_coordinates()
    weather_data = get_weather_data(coordinates)
    snapshot = process_weather_data(coordinates, weather_data)
    save_snapshot(snapshot)


# Instancier le DAG
dag = weather_snapshot_dag()
