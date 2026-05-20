"""
DAG d'ingestion Wikimedia vers Kafka

Ce DAG consomme le flux d'événements Wikimedia en temps réel et l'envoie vers Kafka.
Fonctionnalités:
- Récupération des événements du flux Wikimedia EventStreams
- Parsing et validation des événements JSON
- Classification: bot/humain, anonymous/connected, action type
- Routage vers les topics Kafka appropriés
- Gestion des erreurs et logging
"""

from datetime import datetime, timedelta
import json
import logging
import requests
from typing import Dict, List

from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


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
    dag_id='wikimedia_ingestion_kafka',
    default_args=default_args,
    description='Ingestion du flux Wikimedia vers Kafka',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['wikimedia', 'ingestion', 'kafka'],
)
def dag_wikimedia_ingestion():
    """
    DAG principal pour ingestion Wikimedia vers Kafka
    """
    
    @task
    def consommer_wikimedia_events(batch_size: int = 100, timeout_seconds: int = 30):
        """
        Consomme les événements du flux Wikimedia EventStreams
        
        Args:
            batch_size: Nombre maximum d'événements à traiter par lot
            timeout_seconds: Timeout en secondes pour la requête
            
        Returns:
            Liste des événements bruts reçus du flux Wikimedia
        """
        url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        
        logger.info(f"Démarrage de la consommation depuis: {url}")
        
        events = []
        try:
            response = requests.get(
                url,
                stream=True,
                timeout=timeout_seconds
            )
            response.raise_for_status()
            
            line_count = 0
            for line in response.iter_lines():
                if line_count >= batch_size:
                    break
                    
                if line:
                    try:
                        event_data = json.loads(line)
                        events.append(event_data)
                        line_count += 1
                    except json.JSONDecodeError as e:
                        logger.warning(f"Impossible de parser JSON: {line[:100]}... - Erreur: {e}")
                        continue
            
            logger.info(f"Événements consommés: {line_count}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la consommation Wikimedia: {e}")
            raise AirflowException(f"Erreur d'ingestion Wikimedia: {e}")
        
        return events
    
    
    @task
    def valider_et_classer_events(events: List[Dict]) -> Dict:
        """
        Valide et classe les événements Wikimedia
        
        Classification:
        - Type bot: bot vs humain
        - Type utilisateur: anonyme vs connecté
        - Type action: création, édition, suppression
        
        Args:
            events: Liste des événements bruts
            
        Returns:
            Dictionnaire avec événements classifiés et erreurs
        """
        events_valides = {
            'wm.recentchange.raw': [],
            'wm.bot.events': [],
            'wm.page.edits': [],
        }
        errors = []
        
        for idx, event in enumerate(events):
            try:
                # Validation des champs obligatoires
                required_fields = ['type', 'wiki', 'page_title', 'timestamp']
                if not all(field in event for field in required_fields):
                    missing = [f for f in required_fields if f not in event]
                    errors.append({
                        'index': idx,
                        'reason': f'champs manquants: {missing}',
                        'event': event
                    })
                    continue
                
                # Enrichissement avec métadonnées
                enriched_event = {
                    **event,
                    'ingestion_timestamp': datetime.utcnow().isoformat(),
                    'source': 'wikimedia',
                    'partition_key': event.get('wiki', 'unknown')
                }
                
                # Classification bot
                is_bot = event.get('bot', False)
                
                # Classification utilisateur
                user = event.get('user', '')
                is_anonymous = user.startswith('192.') or user.startswith('2001:')
                
                # Classification action
                event_type = event.get('type', '').lower()
                
                # Ajout aux topics appropriés
                events_valides['wm.recentchange.raw'].append(enriched_event)
                
                if is_bot:
                    events_valides['wm.bot.events'].append(enriched_event)
                
                if event_type in ['new', 'edit']:
                    events_valides['wm.page.edits'].append(enriched_event)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement événement {idx}: {e}")
                errors.append({
                    'index': idx,
                    'reason': str(e),
                    'event': event
                })
        
        result = {
            'valides': events_valides,
            'erreurs': errors,
            'statistiques': {
                'total_traites': len(events),
                'raw_events': len(events_valides['wm.recentchange.raw']),
                'bot_events': len(events_valides['wm.bot.events']),
                'edit_events': len(events_valides['wm.page.edits']),
                'erreurs': len(errors)
            }
        }
        
        logger.info(f"Validation complétée. Stats: {result['statistiques']}")
        
        return result
    
    
    @task
    def envoyer_vers_kafka(classification_result: Dict) -> Dict:
        """
        Envoie les événements validés vers les topics Kafka appropriés
        
        Cette fonction simule l'envoi vers Kafka. En production, utiliser
        kafka-python ou confluent-kafka pour une vraie implémentation.
        
        Args:
            classification_result: Résultat de la classification des événements
            
        Returns:
            Statistiques d'envoi
        """
        from pathlib import Path
        
        # Créer le répertoire de données s'il n'existe pas
        data_dir = Path('/tmp/airflow_wikimedia_data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        topics_stats = {}
        
        for topic, events in classification_result['valides'].items():
            if events:
                # Sauvegarder les événements dans des fichiers
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                filename = data_dir / f"{topic}_{timestamp}.jsonl"
                
                with open(filename, 'w', encoding='utf-8') as f:
                    for event in events:
                        f.write(json.dumps(event) + '\n')
                
                topics_stats[topic] = {
                    'count': len(events),
                    'fichier': str(filename)
                }
                logger.info(f"Envoyé {len(events)} événements vers {topic}")
            else:
                topics_stats[topic] = {'count': 0}
        
        # Traiter les erreurs
        if classification_result['erreurs']:
            errors_file = data_dir / f"wm.errors_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
            with open(errors_file, 'w', encoding='utf-8') as f:
                for error in classification_result['erreurs']:
                    f.write(json.dumps(error) + '\n')
            
            topics_stats['wm.errors'] = {
                'count': len(classification_result['erreurs']),
                'fichier': str(errors_file)
            }
        
        return topics_stats
    
    
    @task
    def logger_resultats_ingestion(send_stats: Dict) -> Dict:
        """
        Log les résultats finaux de l'ingestion
        
        Args:
            send_stats: Statistiques d'envoi
            
        Returns:
            Résumé de l'exécution
        """
        total_sent = sum(stat.get('count', 0) for stat in send_stats.values())
        
        logger.info("=" * 60)
        logger.info("RÉSUMÉ INGESTION WIKIMEDIA")
        logger.info("=" * 60)
        for topic, stat in send_stats.items():
            logger.info(f"{topic}: {stat['count']} événements")
        logger.info(f"Total: {total_sent} événements traités")
        logger.info("=" * 60)
        
        return {
            'total_sent': total_sent,
            'topics_count': len(send_stats),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    
    # Orchestration des tâches
    raw_events = consommer_wikimedia_events()
    classification = valider_et_classer_events(raw_events)
    send_result = envoyer_vers_kafka(classification)
    logger_resultats_ingestion(send_result)


# Création de l'instance du DAG
dag_instance = dag_wikimedia_ingestion()
