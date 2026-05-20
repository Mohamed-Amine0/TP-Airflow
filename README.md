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
