"""
DAG de traitement et agrégations Wikimedia

Ce DAG traite les événements Wikimedia et génère des agrégations:
- Activité générale (par minute, heure, langue, wiki)
- Pages les plus actives (modifiées, créées, supprimées)
- Analyse utilisateurs et bots
- Calculs de ratios et statistiques
"""

from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Dict, List
from collections import Counter, defaultdict

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
    dag_id='wikimedia_traitement_agregations',
    default_args=default_args,
    description='Traitement et agrégations des événements Wikimedia',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['wikimedia', 'traitement', 'agrégations'],
)
def dag_wikimedia_traitement():
    """
    DAG pour traitement et agrégations des événements Wikimedia
    """
    
    @task
    def charger_donnees_brutes() -> List[Dict]:
        """
        Charge les données brutes du répertoire de données
        
        Returns:
            Liste des événements bruts
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
            
            logger.info(f"Chargé {len(events)} événements bruts")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            raise AirflowException(f"Impossible de charger les données: {e}")
        
        return events
    
    
    @task
    def calculer_activite_generale(events: List[Dict]) -> Dict:
        """
        Calcule les statistiques d'activité générale
        
        Génère:
        - Nombre d'événements par minute
        - Nombre d'événements par heure
        - Distribution par langue
        - Distribution par wiki
        
        Args:
            events: Liste des événements
            
        Returns:
            Dictionnaire avec agrégations d'activité
        """
        if not events:
            return {
                'total_events': 0,
                'par_minute': {},
                'par_heure': {},
                'par_langue': {},
                'par_wiki': {}
            }
        
        par_minute = defaultdict(int)
        par_heure = defaultdict(int)
        par_langue = Counter()
        par_wiki = Counter()
        
        for event in events:
            try:
                timestamp = event.get('timestamp', '')
                wiki = event.get('wiki', 'unknown')
                lang = event.get('meta', {}).get('domain', 'unknown').split('.')[0]
                
                # Parser le timestamp (format ISO)
                if timestamp:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    minute_key = dt.strftime('%Y-%m-%d %H:%M')
                    heure_key = dt.strftime('%Y-%m-%d %H:00')
                    
                    par_minute[minute_key] += 1
                    par_heure[heure_key] += 1
                
                par_langue[lang] += 1
                par_wiki[wiki] += 1
                
            except Exception as e:
                logger.warning(f"Erreur parsing événement: {e}")
                continue
        
        return {
            'total_events': len(events),
            'par_minute': dict(par_minute),
            'par_heure': dict(par_heure),
            'par_langue': dict(par_langue),
            'par_wiki': dict(par_wiki)
        }
    
    
    @task
    def analyser_pages(events: List[Dict]) -> Dict:
        """
        Analyse les pages les plus actives
        
        Identifie:
        - Pages les plus modifiées
        - Pages les plus créées
        - Pages les plus supprimées
        
        Args:
            events: Liste des événements
            
        Returns:
            Dictionnaire avec analyse des pages
        """
        pages_modifiees = Counter()
        pages_creees = Counter()
        pages_supprimees = Counter()
        
        for event in events:
            try:
                page_title = event.get('page_title', 'unknown')
                event_type = event.get('type', '').lower()
                
                if event_type == 'edit':
                    pages_modifiees[page_title] += 1
                elif event_type == 'new':
                    pages_creees[page_title] += 1
                elif event_type == 'delete':
                    pages_supprimees[page_title] += 1
                
            except Exception as e:
                logger.warning(f"Erreur analyse page: {e}")
                continue
        
        return {
            'pages_modifiees': dict(pages_modifiees.most_common(20)),
            'pages_creees': dict(pages_creees.most_common(20)),
            'pages_supprimees': dict(pages_supprimees.most_common(10))
        }
    
    
    @task
    def analyser_utilisateurs(events: List[Dict]) -> Dict:
        """
        Analyse l'activité des utilisateurs
        
        Calcule:
        - Top contributeurs
        - Ratio anonymes/connectés
        - Ratio bots/humains
        - Utilisateurs par type
        
        Args:
            events: Liste des événements
            
        Returns:
            Dictionnaire avec analyse utilisateurs
        """
        utilisateurs = Counter()
        anonymes = 0
        connectes = 0
        bots = 0
        humains = 0
        
        for event in events:
            try:
                user = event.get('user', 'anonymous')
                is_bot = event.get('bot', False)
                
                # Détection anonyme (adresse IP)
                is_anonymous = user.startswith('192.') or user.startswith('2001:') or user.startswith('::')
                
                if is_anonymous:
                    anonymes += 1
                else:
                    connectes += 1
                    utilisateurs[user] += 1
                
                if is_bot:
                    bots += 1
                else:
                    humains += 1
                
            except Exception as e:
                logger.warning(f"Erreur analyse utilisateur: {e}")
                continue
        
        total_users = anonymes + connectes
        
        return {
            'top_contributeurs': dict(utilisateurs.most_common(20)),
            'anonymes': anonymes,
            'connectes': connectes,
            'ratio_anonymes': round(anonymes / total_users * 100, 2) if total_users > 0 else 0,
            'bots': bots,
            'humains': humains,
            'ratio_bots': round(bots / (bots + humains) * 100, 2) if (bots + humains) > 0 else 0
        }
    
    
    @task
    def analyser_activite_bots(events: List[Dict]) -> Dict:
        """
        Analyse spécifique de l'activité des bots
        
        Génère:
        - Volume total bots
        - Ratio bot/humain
        - Bots les plus actifs
        
        Args:
            events: Liste des événements
            
        Returns:
            Dictionnaire avec analyse bots
        """
        bots_activity = Counter()
        total_bot_edits = 0
        
        for event in events:
            try:
                if event.get('bot', False):
                    bot_name = event.get('user', 'unknown')
                    bots_activity[bot_name] += 1
                    total_bot_edits += 1
            except Exception as e:
                logger.warning(f"Erreur analyse bot: {e}")
                continue
        
        return {
            'total_bot_edits': total_bot_edits,
            'bots_les_plus_actifs': dict(bots_activity.most_common(20)),
            'nombre_bots_uniques': len(bots_activity)
        }
    
    
    @task
    def generer_fichiers_sortie(
        activite_gen: Dict,
        analyse_pages: Dict,
        analyse_users: Dict,
        analyse_bots: Dict
    ) -> Dict:
        """
        Génère les fichiers de sortie JSON avec les agrégations
        
        Crée:
        - activity_by_hour.json
        - top_pages.json
        - user_activity.json
        - bot_ratio.json
        - language_distribution.json
        
        Args:
            activite_gen: Agrégations d'activité générale
            analyse_pages: Analyse des pages
            analyse_users: Analyse des utilisateurs
            analyse_bots: Analyse des bots
            
        Returns:
            Dictionnaire avec chemins des fichiers générés
        """
        data_dir = Path('/tmp/airflow_wikimedia_data')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        fichiers_generes = {}
        
        try:
            # Fichier activité par heure
            file_activity = data_dir / 'activity_by_hour.json'
            with open(file_activity, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.utcnow().isoformat(),
                    'par_heure': activite_gen.get('par_heure', {}),
                    'total_events': activite_gen.get('total_events', 0)
                }, f, indent=2, ensure_ascii=False)
            fichiers_generes['activity_by_hour'] = str(file_activity)
            
            # Fichier pages principales
            file_pages = data_dir / 'top_pages.json'
            with open(file_pages, 'w', encoding='utf-8') as f:
                json.dump(analyse_pages, f, indent=2, ensure_ascii=False)
            fichiers_generes['top_pages'] = str(file_pages)
            
            # Fichier activité utilisateurs
            file_users = data_dir / 'user_activity.json'
            with open(file_users, 'w', encoding='utf-8') as f:
                json.dump(analyse_users, f, indent=2, ensure_ascii=False)
            fichiers_generes['user_activity'] = str(file_users)
            
            # Fichier ratio bots
            file_bots = data_dir / 'bot_ratio.json'
            with open(file_bots, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.utcnow().isoformat(),
                    **analyse_bots
                }, f, indent=2, ensure_ascii=False)
            fichiers_generes['bot_ratio'] = str(file_bots)
            
            # Fichier distribution par langue
            file_langs = data_dir / 'language_distribution.json'
            with open(file_langs, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.utcnow().isoformat(),
                    'distribution': activite_gen.get('par_langue', {}),
                    'par_wiki': activite_gen.get('par_wiki', {})
                }, f, indent=2, ensure_ascii=False)
            fichiers_generes['language_distribution'] = str(file_langs)
            
            logger.info(f"Fichiers de sortie générés: {list(fichiers_generes.keys())}")
            
        except Exception as e:
            logger.error(f"Erreur génération fichiers: {e}")
            raise AirflowException(f"Impossible de générer les fichiers: {e}")
        
        return fichiers_generes
    
    
    # Orchestration des tâches
    donnees = charger_donnees_brutes()
    activite = calculer_activite_generale(donnees)
    pages = analyser_pages(donnees)
    utilisateurs = analyser_utilisateurs(donnees)
    bots = analyser_activite_bots(donnees)
    generer_fichiers_sortie(activite, pages, utilisateurs, bots)


# Création de l'instance du DAG
dag_instance = dag_wikimedia_traitement()
