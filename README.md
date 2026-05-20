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
