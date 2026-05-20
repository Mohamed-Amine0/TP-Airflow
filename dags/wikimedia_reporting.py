"""
DAG de reporting automatisé Wikimedia

Ce DAG génère les rapports automatisés:
- Rapport activité globale
- Rapport qualité données
- Rapport trafic
- Rapport système/pipelines
"""

from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Dict, List

from airflow import DAG
from airflow.decorators import dag, task


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='wikimedia_reporting',
    default_args=default_args,
    description='Reporting automatisé Wikimedia',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['wikimedia', 'reporting', 'analytics'],
)
def dag_wikimedia_reporting():
    """
    DAG pour génération de rapports automatisés
    """
    
    @task
    def charger_agregations() -> Dict:
        """
        Charge les données d'agrégations
        
        Returns:
            Dictionnaire avec toutes les agrégations
        """
        data_dir = Path('/tmp/airflow_wikimedia_data')
        agregations = {}
        
        try:
            # Charger activité par heure
            file_activity = data_dir / 'activity_by_hour.json'
            if file_activity.exists():
                with open(file_activity, 'r', encoding='utf-8') as f:
                    agregations['activity_by_hour'] = json.load(f)
            
            # Charger pages principales
            file_pages = data_dir / 'top_pages.json'
            if file_pages.exists():
                with open(file_pages, 'r', encoding='utf-8') as f:
                    agregations['top_pages'] = json.load(f)
            
            # Charger activité utilisateurs
            file_users = data_dir / 'user_activity.json'
            if file_users.exists():
                with open(file_users, 'r', encoding='utf-8') as f:
                    agregations['user_activity'] = json.load(f)
            
            # Charger ratio bots
            file_bots = data_dir / 'bot_ratio.json'
            if file_bots.exists():
                with open(file_bots, 'r', encoding='utf-8') as f:
                    agregations['bot_ratio'] = json.load(f)
            
            # Charger distribution langues
            file_langs = data_dir / 'language_distribution.json'
            if file_langs.exists():
                with open(file_langs, 'r', encoding='utf-8') as f:
                    agregations['language_distribution'] = json.load(f)
            
            logger.info(f"Agrégations chargées: {list(agregations.keys())}")
            
        except Exception as e:
            logger.warning(f"Erreur chargement agrégations: {e}")
        
        return agregations
    
    
    @task
    def charger_anomalies() -> Dict:
        """
        Charge les données d'anomalies
        
        Returns:
            Dictionnaire avec statistiques anomalies
        """
        data_dir = Path('/tmp/airflow_wikimedia_data')
        anomalies_stats = {
            'detected': 0,
            'par_type': {},
            'severity': {}
        }
        
        try:
            anomalies_file = data_dir / 'anomalies.jsonl'
            if anomalies_file.exists():
                anomalies = []
                with open(anomalies_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            anomalies.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
                
                anomalies_stats['detected'] = len(anomalies)
                
                # Compter par type
                for anom in anomalies:
                    anom_type = anom.get('type', 'unknown')
                    anomalies_stats['par_type'][anom_type] = anomalies_stats['par_type'].get(anom_type, 0) + 1
                    
                    severity = anom.get('severity', 'unknown')
                    anomalies_stats['severity'][severity] = anomalies_stats['severity'].get(severity, 0) + 1
            
            logger.info(f"Anomalies chargées: {anomalies_stats['detected']}")
            
        except Exception as e:
            logger.warning(f"Erreur chargement anomalies: {e}")
        
        return anomalies_stats
    
    
    @task
    def generer_rapport_activite(agregations: Dict) -> Dict:
        """
        Génère le rapport d'activité globale
        
        Contient:
        - Nombre total d'événements
        - Top pages
        - Top utilisateurs
        - Ratio bots/humains
        
        Args:
            agregations: Données d'agrégations
            
        Returns:
            Rapport activité
        """
        rapport = {
            'section': 'activite_globale',
            'timestamp': datetime.utcnow().isoformat(),
            'contenu': {
                'nombre_total_evenements': agregations.get('activity_by_hour', {}).get('total_events', 0),
                'top_pages_modifiees': agregations.get('top_pages', {}).get('pages_modifiees', {}),
                'top_contributeurs': agregations.get('user_activity', {}).get('top_contributeurs', {}),
                'ratio_bots_humains': {
                    'bots': agregations.get('user_activity', {}).get('bots', 0),
                    'humains': agregations.get('user_activity', {}).get('humains', 0),
                    'ratio_percent': agregations.get('user_activity', {}).get('ratio_bots', 0)
                }
            }
        }
        
        return rapport
    
    
    @task
    def generer_rapport_qualite(agregations: Dict, anomalies: Dict) -> Dict:
        """
        Génère le rapport qualité données
        
        Contient:
        - Anomalies détectées
        - Erreurs ingestion
        - Taux données invalides
        
        Args:
            agregations: Données d'agrégations
            anomalies: Statistiques anomalies
            
        Returns:
            Rapport qualité
        """
        rapport = {
            'section': 'qualite',
            'timestamp': datetime.utcnow().isoformat(),
            'contenu': {
                'anomalies_detectees': anomalies.get('detected', 0),
                'anomalies_par_type': anomalies.get('par_type', {}),
                'anomalies_par_severite': anomalies.get('severity', {}),
                'data_quality_issues': agregations.get('user_activity', {}).get('anonymes', 0),
                'taux_donnees_invalides': 0
            }
        }
        
        return rapport
    
    
    @task
    def generer_rapport_trafic(agregations: Dict) -> Dict:
        """
        Génère le rapport trafic
        
        Contient:
        - Trafic par heure
        - Distribution par langue
        - Distribution par wiki
        
        Args:
            agregations: Données d'agrégations
            
        Returns:
            Rapport trafic
        """
        rapport = {
            'section': 'trafic',
            'timestamp': datetime.utcnow().isoformat(),
            'contenu': {
                'par_heure': agregations.get('activity_by_hour', {}).get('par_heure', {}),
                'par_langue': agregations.get('language_distribution', {}).get('distribution', {}),
                'par_wiki': agregations.get('language_distribution', {}).get('par_wiki', {})
            }
        }
        
        return rapport
    
    
    @task
    def generer_rapport_systeme() -> Dict:
        """
        Génère le rapport système/pipelines
        
        Contient:
        - Status DAGs
        - Latence pipeline
        - Taux de succès
        
        Returns:
            Rapport système
        """
        rapport = {
            'section': 'systeme',
            'timestamp': datetime.utcnow().isoformat(),
            'contenu': {
                'lag_ingestion_kafka': 'N/A',
                'latence_spark': 'N/A',
                'taux_echec_dags': 0,
                'status': 'operational',
                'notes': 'Simulation en environnement de développement'
            }
        }
        
        return rapport
    
    
    @task
    def assembler_rapport_final(
        rapport_activite: Dict,
        rapport_qualite: Dict,
        rapport_trafic: Dict,
        rapport_systeme: Dict
    ) -> Dict:
        """
        Assemble tous les rapports en un seul fichier
        
        Args:
            rapport_activite: Rapport activité
            rapport_qualite: Rapport qualité
            rapport_trafic: Rapport trafic
            rapport_systeme: Rapport système
            
        Returns:
            Rapport complet
        """
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
        
        rapport_complet = {
            'date': date_str,
            'timestamp': datetime.utcnow().isoformat(),
            'titre': f'Rapport Wikimedia - {date_str}',
            'rapports': {
                'activite': rapport_activite,
                'qualite': rapport_qualite,
                'trafic': rapport_trafic,
                'systeme': rapport_systeme
            }
        }
        
        # Sauvegarder le rapport
        data_dir = Path('/tmp/airflow_wikimedia_data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        rapport_file = data_dir / f"rapport_{date_str}.json"
        with open(rapport_file, 'w', encoding='utf-8') as f:
            json.dump(rapport_complet, f, indent=2, ensure_ascii=False)
        
        logger.info("=" * 60)
        logger.info("RAPPORT FINAL GÉNÉRÉ")
        logger.info("=" * 60)
        logger.info(f"Date: {date_str}")
        logger.info(f"Fichier: {rapport_file}")
        logger.info("=" * 60)
        
        return {
            'fichier': str(rapport_file),
            'rapport': rapport_complet
        }
    
    
    # Orchestration des tâches
    agregations = charger_agregations()
    anomalies = charger_anomalies()
    rap_activite = generer_rapport_activite(agregations)
    rap_qualite = generer_rapport_qualite(agregations, anomalies)
    rap_trafic = generer_rapport_trafic(agregations)
    rap_systeme = generer_rapport_systeme()
    assembler_rapport_final(rap_activite, rap_qualite, rap_trafic, rap_systeme)


# Création de l'instance du DAG
dag_instance = dag_wikimedia_reporting()
