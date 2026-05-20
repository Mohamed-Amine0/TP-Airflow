"""
DAG de détection d'anomalies Wikimedia

Ce DAG détecte les anomalies dans le flux Wikimedia:
- Spikes d'activité (+300% en < 5 min)
- Comportements anormaux de bots
- Spam/vandalisme (éditions répétées, texte court)
- Anomalies données (champs manquants, timestamps incohérents)
"""

from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

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
    dag_id='wikimedia_detection_anomalies',
    default_args=default_args,
    description='Détection d\'anomalies dans les événements Wikimedia',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['wikimedia', 'anomalies', 'data_quality'],
)
def dag_wikimedia_anomalies():
    """
    DAG pour détection d'anomalies Wikimedia
    """
    
    @task
    def charger_donnees_anomalies() -> List[Dict]:
        """
        Charge les données brutes pour analyse d'anomalies
        
        Returns:
            Liste des événements
        """
        data_dir = Path('/tmp/airflow_wikimedia_data')
        events = []
        
        if not data_dir.exists():
            logger.warning("Répertoire de données n'existe pas")
            return []
        
        try:
            for jsonl_file in data_dir.glob('wm.recentchange.raw_*.jsonl'):
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            event = json.loads(line.strip())
                            events.append(event)
                        except json.JSONDecodeError:
                            continue
            
            logger.info(f"Chargé {len(events)} événements pour analyse anomalies")
            
        except Exception as e:
            logger.error(f"Erreur chargement données: {e}")
            raise AirflowException(f"Impossible de charger les données: {e}")
        
        return events
    
    
    @task
    def detecter_spikes_activite(events: List[Dict]) -> List[Dict]:
        """
        Détecte les spikes d'activité (>300% en < 5 min)
        
        Args:
            events: Liste des événements
            
        Returns:
            Liste des anomalies détectées
        """
        anomalies = []
        
        # Grouper par page et fenêtre de 5 minutes
        pages_timeline = defaultdict(lambda: defaultdict(int))
        
        for event in events:
            try:
                page = event.get('page_title', 'unknown')
                timestamp = event.get('timestamp', '')
                
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    # Arrondir à la fenêtre de 5 minutes la plus proche
                    minute = (dt.minute // 5) * 5
                    window = dt.strftime(f'%Y-%m-%d %H:{minute:02d}')
                    pages_timeline[page][window] += 1
                
            except Exception as e:
                logger.warning(f"Erreur parsing événement spike: {e}")
                continue
        
        # Analyser les spikes
        for page, timeline in pages_timeline.items():
            if len(timeline) < 2:
                continue
            
            windows = sorted(timeline.keys())
            for i in range(len(windows) - 1):
                current_window = timeline[windows[i]]
                next_window = timeline[windows[i + 1]]
                
                # Calculer l'augmentation
                if current_window > 0:
                    increase = ((next_window - current_window) / current_window) * 100
                    
                    if increase >= 300:
                        anomalies.append({
                            'type': 'edit_spike',
                            'page': page,
                            'severity': 'high' if increase >= 500 else 'medium',
                            'timestamp': datetime.utcnow().isoformat(),
                            'details': {
                                'edit_count_prev': current_window,
                                'edit_count_current': next_window,
                                'increase_percent': round(increase, 2),
                                'window': '5min'
                            }
                        })
        
        logger.info(f"Spikes détectés: {len(anomalies)}")
        return anomalies
    
    
    @task
    def detecter_anomalies_bots(events: List[Dict]) -> List[Dict]:
        """
        Détecte les comportements anormaux de bots
        
        - Bot effectuant > X édits/min
        - Bot éditant des pages non autorisées
        
        Args:
            events: Liste des événements
            
        Returns:
            Liste des anomalies bots
        """
        anomalies = []
        bot_activity = defaultdict(lambda: defaultdict(int))
        
        MAX_EDITS_PER_MINUTE = 100
        PROTECTED_PAGES_PATTERNS = ['MediaWiki:', 'Template:']
        
        for event in events:
            try:
                if not event.get('bot', False):
                    continue
                
                bot_name = event.get('user', 'unknown')
                page = event.get('page_title', '')
                timestamp = event.get('timestamp', '')
                
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    minute_key = dt.strftime('%Y-%m-%d %H:%M')
                    bot_activity[bot_name][minute_key] += 1
                
                # Vérifier édition pages protégées
                for pattern in PROTECTED_PAGES_PATTERNS:
                    if page.startswith(pattern):
                        anomalies.append({
                            'type': 'bot_protected_edit',
                            'bot': bot_name,
                            'page': page,
                            'severity': 'high',
                            'timestamp': datetime.utcnow().isoformat(),
                            'details': {
                                'reason': f'Bot éditant page protégée: {pattern}'
                            }
                        })
                
            except Exception as e:
                logger.warning(f"Erreur détection anomalie bot: {e}")
                continue
        
        # Vérifier les taux d'édition
        for bot_name, timeline in bot_activity.items():
            for minute, count in timeline.items():
                if count > MAX_EDITS_PER_MINUTE:
                    anomalies.append({
                        'type': 'bot_excessive_rate',
                        'bot': bot_name,
                        'severity': 'high',
                        'timestamp': datetime.utcnow().isoformat(),
                        'details': {
                            'edits_per_minute': count,
                            'max_allowed': MAX_EDITS_PER_MINUTE,
                            'minute': minute
                        }
                    })
        
        logger.info(f"Anomalies bots détectées: {len(anomalies)}")
        return anomalies
    
    
    @task
    def detecter_spam_vandalisme(events: List[Dict]) -> List[Dict]:
        """
        Détecte le spam et le vandalisme
        
        - Page modifiée 5+ fois en < 2 minutes
        - Édition très courte répétée
        - Reverts massifs
        
        Args:
            events: Liste des événements
            
        Returns:
            Liste des anomalies spam/vandalisme
        """
        anomalies = []
        pages_timeline = defaultdict(list)
        
        for event in events:
            try:
                page = event.get('page_title', '')
                timestamp = event.get('timestamp', '')
                comment = event.get('comment', '')
                
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    pages_timeline[page].append({
                        'timestamp': dt,
                        'comment': comment
                    })
            except Exception as e:
                logger.warning(f"Erreur parsing spam: {e}")
                continue
        
        # Analyser les patterns de vandalisme
        for page, edits in pages_timeline.items():
            if len(edits) < 5:
                continue
            
            # Vérifier éditions rapides
            edits_sorted = sorted(edits, key=lambda x: x['timestamp'])
            
            for i in range(len(edits_sorted) - 4):
                window = edits_sorted[i:i+5]
                time_diff = (window[-1]['timestamp'] - window[0]['timestamp']).total_seconds()
                
                if time_diff < 120:  # < 2 minutes
                    anomalies.append({
                        'type': 'rapid_edits',
                        'page': page,
                        'severity': 'high',
                        'timestamp': datetime.utcnow().isoformat(),
                        'details': {
                            'edit_count': 5,
                            'window_seconds': time_diff,
                            'threshold_seconds': 120
                        }
                    })
                    break
            
            # Vérifier reverts (commentaires contenant "revert")
            revert_count = sum(1 for edit in edits if 'revert' in edit['comment'].lower())
            if revert_count >= 3:
                anomalies.append({
                    'type': 'revert_spam',
                    'page': page,
                    'severity': 'medium',
                    'timestamp': datetime.utcnow().isoformat(),
                    'details': {
                        'revert_count': revert_count
                    }
                })
        
        logger.info(f"Cas spam/vandalisme détectés: {len(anomalies)}")
        return anomalies
    
    
    @task
    def detecter_anomalies_donnees(events: List[Dict]) -> List[Dict]:
        """
        Détecte les anomalies dans les données
        
        - Événements sans page_id
        - Timestamps incohérents
        - Langue inconnue
        - Wiki null
        
        Args:
            events: Liste des événements
            
        Returns:
            Liste des anomalies données
        """
        anomalies = []
        
        for idx, event in enumerate(events):
            try:
                issues = []
                
                # Vérifier page_id
                if 'page_id' not in event or event.get('page_id') is None:
                    issues.append('page_id manquant')
                
                # Vérifier wiki
                if 'wiki' not in event or not event.get('wiki'):
                    issues.append('wiki null ou manquant')
                
                # Vérifier timestamp
                timestamp = event.get('timestamp', '')
                try:
                    if timestamp:
                        datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    issues.append('timestamp invalide')
                
                # Vérifier langue
                domain = event.get('meta', {}).get('domain', '')
                if not domain or '.' not in domain:
                    issues.append('langue inconnue')
                
                if issues:
                    anomalies.append({
                        'type': 'data_quality_issue',
                        'severity': 'low',
                        'timestamp': datetime.utcnow().isoformat(),
                        'details': {
                            'issues': issues,
                            'event_index': idx
                        }
                    })
                
            except Exception as e:
                logger.warning(f"Erreur détection anomalie données: {e}")
                continue
        
        logger.info(f"Anomalies données détectées: {len(anomalies)}")
        return anomalies
    
    
    @task
    def agreger_anomalies(
        spikes: List[Dict],
        anomalies_bots: List[Dict],
        spam: List[Dict],
        data_issues: List[Dict]
    ) -> Dict:
        """
        Agrège toutes les anomalies et génère un rapport
        
        Args:
            spikes: Anomalies spikes
            anomalies_bots: Anomalies bots
            spam: Anomalies spam/vandalisme
            data_issues: Anomalies données
            
        Returns:
            Rapport d'anomalies
        """
        toutes_anomalies = spikes + anomalies_bots + spam + data_issues
        
        # Sauvegarder les anomalies
        data_dir = Path('/tmp/airflow_wikimedia_data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        anomalies_file = data_dir / 'anomalies.jsonl'
        with open(anomalies_file, 'w', encoding='utf-8') as f:
            for anomaly in toutes_anomalies:
                f.write(json.dumps(anomaly) + '\n')
        
        # Générer rapport
        rapport = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_anomalies': len(toutes_anomalies),
            'par_type': {
                'edit_spike': len(spikes),
                'bot_anomalies': len(anomalies_bots),
                'spam_vandalisme': len(spam),
                'data_quality': len(data_issues)
            },
            'severity_breakdown': {
                'high': len([a for a in toutes_anomalies if a.get('severity') == 'high']),
                'medium': len([a for a in toutes_anomalies if a.get('severity') == 'medium']),
                'low': len([a for a in toutes_anomalies if a.get('severity') == 'low'])
            },
            'fichier_anomalies': str(anomalies_file)
        }
        
        logger.info("=" * 60)
        logger.info("RAPPORT ANOMALIES")
        logger.info("=" * 60)
        logger.info(f"Total: {rapport['total_anomalies']} anomalies")
        logger.info(f"Par type: {rapport['par_type']}")
        logger.info(f"Sévérité: {rapport['severity_breakdown']}")
        logger.info("=" * 60)
        
        return rapport
    
    
    # Orchestration des tâches
    donnees = charger_donnees_anomalies()
    spikes = detecter_spikes_activite(donnees)
    bots_anom = detecter_anomalies_bots(donnees)
    spam_anom = detecter_spam_vandalisme(donnees)
    data_anom = detecter_anomalies_donnees(donnees)
    agreger_anomalies(spikes, bots_anom, spam_anom, data_anom)


# Création de l'instance du DAG
dag_instance = dag_wikimedia_anomalies()
