# TP Airflow

Exercices pratiques avec Apache Airflow et les APIs Open-Meteo.

## Exercice 1 : Weather Snapshot

DAG qui s'exécute une fois par jour pour récupérer et sauvegarder les conditions météorologiques actuelles.

### Fonctionnalités

- Récupération des coordonnées d'une ville via l'API Geocoding d'Open-Meteo
- Récupération des données météo pour 24 heures
- Extraction des métriques clés :
  - Température moyenne en Celsius
  - Vitesse maximale du vent en km/h
  - Total des précipitations en mm
- Génération d'un snapshot JSON
- Sauvegarde des fichiers dans `data/weather_snapshots/`

### Exécution

Le DAG `weather_snapshot` s'exécute automatiquement chaque jour. Pour tester manuellement :

1. Accéder à http://localhost:8080/
2. Localiser le DAG `weather_snapshot`
3. Cliquer sur le bouton Trigger DAG
4. Vérifier les fichiers générés dans `data/weather_snapshots/`

## Exercice 2 : Scheduling & météo par intervalle temporel

Deux DAGs qui analysent les codes météo sur des intervalles de temps différents.

### DAGs

#### weather_hourly
- Exécuté toutes les heures
- Analyse des conditions météo sur 1 heure
- Génère des rapports toutes les heures

#### weather_daily  
- Exécuté une fois par jour
- Analyse des conditions météo sur 24 heures
- Génère un rapport quotidien

### Fonctionnalités

- Utilisation des fenêtres temporelles Airflow (`data_interval_start`, `data_interval_end`)
- Récupération des données historiques via l'API Archive d'Open-Meteo
- Calcul de la distribution des codes météo
- Identification du code météo dominant
- Génération de rapports JSON avec :
  - Intervalle traité
  - Nombre total de mesures
  - Distribution détaillée des codes météo
  - Code météo dominant et sa description
- Sauvegarde dans `data/weather_intervals/`

### Exécution

Les DAGs s'exécutent automatiquement selon leurs schedules. Pour tester manuellement :

1. Accéder à http://localhost:8080/
2. Localiser `weather_hourly` ou `weather_daily`
3. Cliquer sur Trigger DAG
4. Vérifier les rapports dans `data/weather_intervals/`

## Exercice 3 : Alertes météo Kafka

DAG qui génère et publie des alertes météo sur Kafka en temps quasi-réel.

### Fonctionnalités

- Exécution toutes les minutes
- Récupération des données météo actuelles via Open-Meteo
- Analyse en temps réel :
  - **Cold alert** : température < 5°C ET vitesse du vent > 20 km/h
  - **Hot alert** : température > 35°C
- Publication des alertes sur le topic Kafka `weather-alerts`
- Chaque message contient :
  - Ville
  - Timestamp
  - Température
  - Vitesse du vent
  - Code météo
  - Type d'alerte
  - Message descriptif

### Architecture Kafka

- **Zookeeper** : Coordination et gestion des brokers
- **Kafka Broker** : Topic `weather-alerts` créé automatiquement
- **Producteur** : Le DAG envoie les alertes
- Port Kafka interne : 29092 (pour les conteneurs)
- Port Kafka externe : 9092 (pour l'accès hôte)

### Format des messages Kafka

```json
{
  "city": "Paris",
  "timestamp": "2026-05-20T14:30:45.123456",
  "temperature": -2.5,
  "wind_speed": 25.3,
  "weather_code": 71,
  "alert_type": "cold_alert",
  "alert_level": "warning",
  "message": "Attention : températures très froides (-2.5°C) avec vent fort (25.3 km/h)"
}
```

### Exécution

1. Accéder à http://localhost:8080/
2. Localiser le DAG `weather_alert_stream`
3. Cliquer sur Trigger DAG
4. Les alertes sont publiées sur Kafka si les seuils sont dépassés
5. Pour consommer les messages (optionnel) :

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather-alerts --from-beginning
```

## Infrastructure

### Services Docker

- **Airflow** : Orchestration des workflows
- **PostgreSQL** : Base de données Airflow
- **Redis** : Broker Celery
- **Zookeeper** : Coordination Kafka
- **Kafka** : Streaming d'événements

### Volumes

- `data/` : Dossier partagé pour les fichiers générés
- `logs/` : Logs Airflow
- `config/` : Configuration personnalisée
- `postgres-db-volume/` : Données PostgreSQL

## Structure du projet

```
airflow-project/
├── dags/
│   └── weather_snapshot.py       # DAG de l'exercice 1
├── data/
│   ├── weather_snapshots/        # Snapshots JSON générés
│   ├── weather_intervals/        # Rapports d'intervalles
│   └── weather_reports/          # Rapports agrégés
├── logs/                         # Logs Airflow
├── plugins/                      # Plugins personnalisés
├── config/
│   └── airflow.cfg               # Configuration Airflow
└── docker-compose.yaml           # Orchestration Docker
```

## Configuration Docker

Le volume `data/` est monté pour permettre l'accès aux fichiers depuis la machine hôte.

## Prérequis

- Docker et Docker Compose
- Python 3.11+
- Accès aux APIs Open-Meteo (publiques, sans authentification)

## Mise en route

```bash
docker-compose down
docker-compose up -d
```

Attendre environ 30 secondes que tous les services démarrent correctement.
