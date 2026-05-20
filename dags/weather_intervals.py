"""
Exercice 2 : Scheduling & météo par intervalle temporel

Créer deux DAGs exécutés à différentes fréquences :
- weather_hourly : exécuté toutes les heures
- weather_daily : exécuté une fois par jour

Chaque DAG doit :
1. Utiliser les fenêtres temporelles Airflow (data_interval_start, data_interval_end)
2. Récupérer les weather codes sur l'intervalle du run
3. Compter les occurrences de chaque code
4. Générer un rapport JSON avec distribution et code dominant
5. Sauvegarder dans data/weather_intervals/
"""

from datetime import datetime, timedelta
import json
import requests
from pathlib import Path
from collections import Counter

from airflow.decorators import dag, task


# ==============================================================================
# Fonction utilitaire pour créer le dictionnaire des descriptions de codes
# ==============================================================================
def get_weather_code_descriptions():
    """
    Retourne un dictionnaire des codes météo Open-Meteo et leurs descriptions
    Référence: https://open-meteo.com/en/docs
    """
    return {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail",
    }


# ==============================================================================
# TÂCHE 1 : Récupérer les données météo pour l'intervalle
# ==============================================================================
@task
def fetch_weather_for_interval(data_interval_start, data_interval_end, city='Paris'):
    """
    Récupère les données météo historiques sur l'intervalle du run.
    
    Les données historiques d'Open-Meteo sont disponibles pour les 7 derniers jours.
    """
    print(f"Récupération météo pour {city}")
    print(f"Intervalle : {data_interval_start} à {data_interval_end}")
    
    # Extraire les dates pour l'API
    start_date = data_interval_start.strftime('%Y-%m-%d')
    end_date = data_interval_end.strftime('%Y-%m-%d')
    
    # Coordonnées fixes de Paris
    latitude = 48.8566
    longitude = 2.3522
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': 'weather_code',
        'timezone': 'Europe/Paris',
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        weather_data = response.json()
        
        # Vérifier que nous avons des données
        if 'hourly' not in weather_data or not weather_data['hourly'].get('weather_code'):
            print("Aucune donnée météo disponible pour cette période")
            return {
                'weather_codes': [],
                'count': 0,
                'start_date': start_date,
                'end_date': end_date,
                'city': city,
            }
        
        weather_codes = weather_data['hourly']['weather_code']
        print(f"Données reçues : {len(weather_codes)} mesures horaires")
        
        return {
            'weather_codes': weather_codes,
            'count': len(weather_codes),
            'start_date': start_date,
            'end_date': end_date,
            'city': city,
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return {
            'weather_codes': [],
            'count': 0,
            'start_date': start_date,
            'end_date': end_date,
            'city': city,
        }


# ==============================================================================
# TÂCHE 2 : Analyser et agréger les codes météo
# ==============================================================================
@task
def analyze_weather_codes(weather_data):
    """
    Analyse les codes météo et produit des statistiques.
    """
    weather_codes = weather_data['weather_codes']
    
    if not weather_codes:
        print("Aucun code météo à analyser")
        return {
            'distribution': {},
            'dominant_code': None,
            'dominant_description': 'N/A',
            'total_measurements': 0,
            'interval': {
                'start': weather_data['start_date'],
                'end': weather_data['end_date'],
            },
            'city': weather_data['city'],
        }
    
    # Compter les occurrences de chaque code
    code_counter = Counter(weather_codes)
    code_descriptions = get_weather_code_descriptions()
    
    # Créer la distribution en pourcentage
    total = len(weather_codes)
    distribution = {}
    for code, count in sorted(code_counter.items()):
        description = code_descriptions.get(code, "Code inconnu")
        distribution[str(code)] = {
            'count': count,
            'percentage': round((count / total) * 100, 2),
            'description': description,
        }
    
    # Identifier le code dominant
    dominant_code = code_counter.most_common(1)[0][0]
    dominant_description = code_descriptions.get(dominant_code, "Code inconnu")
    
    print(f"Distribution des codes météo analysée")
    print(f"Code dominant : {dominant_code} ({dominant_description})")
    print(f"Occurrences : {code_counter[dominant_code]}/{total}")
    
    return {
        'distribution': distribution,
        'dominant_code': dominant_code,
        'dominant_description': dominant_description,
        'total_measurements': total,
        'interval': {
            'start': weather_data['start_date'],
            'end': weather_data['end_date'],
        },
        'city': weather_data['city'],
    }


# ==============================================================================
# TÂCHE 3 : Sauvegarder le rapport
# ==============================================================================
@task
def save_report(analysis, report_type='daily'):
    """
    Sauvegarde le rapport d'analyse en JSON.
    """
    output_dir = Path('/opt/airflow/data/weather_intervals')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Créer le rapport complet
    report = {
        'timestamp': datetime.now().isoformat(),
        'report_type': report_type,
        'interval': analysis['interval'],
        'city': analysis['city'],
        'metrics': {
            'total_measurements': analysis['total_measurements'],
            'distribution': analysis['distribution'],
            'dominant_code': analysis['dominant_code'],
            'dominant_description': analysis['dominant_description'],
        }
    }
    
    # Nom du fichier avec timestamp et type
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_dir / f"weather_report_{report_type}_{timestamp}.json"
    
    # Sauvegarder
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Rapport sauvegardé : {filename}")
    return str(filename)


# ==============================================================================
# DAG HORAIRE
# ==============================================================================
@dag(
    dag_id='weather_hourly',
    description='Analyse des codes météo sur intervalle horaire',
    schedule='@hourly',
    start_date=datetime(2026, 5, 19),
    catchup=False,
    tags=['weather', 'exercice2', 'horaire'],
)
def weather_hourly_dag():
    """DAG exécuté toutes les heures pour analyser les conditions météo"""
    
    # Récupérer les variables d'exécution du contexte
    from airflow.models import Variable
    
    # Les intervalles sont automatiquement passés par Airflow
    weather_data = fetch_weather_for_interval(
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        city='Paris',
    )
    
    analysis = analyze_weather_codes(weather_data)
    save_report(analysis, report_type='hourly')


# ==============================================================================
# DAG QUOTIDIEN
# ==============================================================================
@dag(
    dag_id='weather_daily',
    description='Analyse des codes météo sur intervalle quotidien',
    schedule='@daily',
    start_date=datetime(2026, 5, 19),
    catchup=False,
    tags=['weather', 'exercice2', 'quotidien'],
)
def weather_daily_dag():
    """DAG exécuté une fois par jour pour analyser les conditions météo"""
    
    weather_data = fetch_weather_for_interval(
        data_interval_start="{{ data_interval_start }}",
        data_interval_end="{{ data_interval_end }}",
        city='Paris',
    )
    
    analysis = analyze_weather_codes(weather_data)
    save_report(analysis, report_type='daily')


# ==============================================================================
# Instancier les DAGs
# ==============================================================================
hourly_dag = weather_hourly_dag()
daily_dag = weather_daily_dag()
