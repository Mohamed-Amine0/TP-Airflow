#!/usr/bin/env python3
"""
Script pour générer des données de test pour le monitoring

Génère:
- monitoring_latest.json avec métriques de test
- rapport_YYYY-MM-DD.json avec rapport consolidé

Utilisation:
  python generate_test_data.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta


def generate_monitoring_data():
    """Génère le fichier de monitoring test"""
    
    monitoring_report = {
        "timestamp": datetime.utcnow().isoformat(),
        "system_status": "operational",
        "components": {
            "airflow": {
                "timestamp": datetime.utcnow().isoformat(),
                "dags": {
                    "wikimedia_ingestion_kafka": {
                        "state": "success",
                        "last_execution": (datetime.utcnow() - timedelta(minutes=5)).isoformat(),
                        "start_date": (datetime.utcnow() - timedelta(minutes=10)).isoformat(),
                        "end_date": datetime.utcnow().isoformat()
                    },
                    "wikimedia_traitement_agregations": {
                        "state": "success",
                        "last_execution": (datetime.utcnow() - timedelta(minutes=10)).isoformat(),
                        "start_date": (datetime.utcnow() - timedelta(minutes=15)).isoformat(),
                        "end_date": (datetime.utcnow() - timedelta(minutes=5)).isoformat()
                    },
                    "wikimedia_detection_anomalies": {
                        "state": "success",
                        "last_execution": (datetime.utcnow() - timedelta(minutes=15)).isoformat(),
                        "start_date": (datetime.utcnow() - timedelta(minutes=20)).isoformat(),
                        "end_date": (datetime.utcnow() - timedelta(minutes=10)).isoformat()
                    },
                    "wikimedia_reporting": {
                        "state": "running",
                        "last_execution": datetime.utcnow().isoformat(),
                        "start_date": (datetime.utcnow() - timedelta(minutes=2)).isoformat(),
                        "end_date": None
                    }
                }
            },
            "kafka": {
                "timestamp": datetime.utcnow().isoformat(),
                "topics": {
                    "wm.recentchange.raw": {
                        "partitions": 1,
                        "replication": 1,
                        "messages": 1250
                    },
                    "wm.bot.events": {
                        "partitions": 1,
                        "replication": 1,
                        "messages": 312
                    },
                    "wm.page.edits": {
                        "partitions": 1,
                        "replication": 1,
                        "messages": 938
                    },
                    "wm.errors": {
                        "partitions": 1,
                        "replication": 1,
                        "messages": 5
                    }
                },
                "brokers": 1,
                "lag": 0,
                "status": "healthy"
            },
            "data_quality": {
                "timestamp": datetime.utcnow().isoformat(),
                "anomalies_detected": 4,
                "invalid_events_rate": 0.4,
                "data_files": {
                    "activity_by_hour.json": {
                        "size_bytes": 118,
                        "modified": datetime.utcnow().isoformat()
                    },
                    "bot_ratio.json": {
                        "size_bytes": 226,
                        "modified": datetime.utcnow().isoformat()
                    },
                    "language_distribution.json": {
                        "size_bytes": 211,
                        "modified": datetime.utcnow().isoformat()
                    },
                    "top_pages.json": {
                        "size_bytes": 474,
                        "modified": datetime.utcnow().isoformat()
                    },
                    "user_activity.json": {
                        "size_bytes": 428,
                        "modified": datetime.utcnow().isoformat()
                    }
                },
                "ingestion_status": "operational"
            }
        },
        "alerts": [
            {
                "severity": "low",
                "message": "4 anomalies détectées dans le flux de données"
            }
        ]
    }
    
    return monitoring_report


def generate_report_data():
    """Génère le rapport consolidé journalier"""
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "date": date_str,
        "rapport_activite": {
            "total_events": 20,
            "top_pages": {
                "France": 2,
                "Python_(langage)": 2,
                "Paris": 1
            },
            "top_users": {
                "Admin_User": 2,
                "Jean_History": 2,
                "Alice_Developer": 2
            },
            "bot_stats": {
                "bots": 5,
                "humains": 15,
                "ratio_bots": 25.0
            }
        },
        "rapport_qualite": {
            "anomalies_detected": 4,
            "data_quality_score": 0.96,
            "invalid_events": {
                "count": 1,
                "rate": 0.04
            }
        },
        "rapport_trafic": {
            "par_heure": {
                "2026-05-20 15:00": 20
            },
            "par_wiki": {
                "enwiki": 7,
                "frwiki": 7,
                "dewiki": 3,
                "eswiki": 3
            },
            "par_langue": {
                "en": 7,
                "fr": 7,
                "de": 3,
                "es": 3
            }
        },
        "rapport_systeme": {
            "dag_executions": 4,
            "successful_tasks": 20,
            "failed_tasks": 0,
            "average_task_duration": 2.5,
            "avg_latency_ms": 1250
        }
    }
    
    return report


def main():
    """Génère tous les fichiers de test"""
    
    data_dir = Path(__file__).parent / 'data' / 'wikimedia'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "="*60)
    print("🔧 Génération des données de test")
    print("="*60)
    
    try:
        # Générer monitoring
        monitoring = generate_monitoring_data()
        monitoring_file = data_dir / 'monitoring_latest.json'
        with open(monitoring_file, 'w', encoding='utf-8') as f:
            json.dump(monitoring, f, indent=2, ensure_ascii=False)
        print(f"✓ {monitoring_file.name} généré ({monitoring_file.stat().st_size} bytes)")
        
        # Générer rapport
        report = generate_report_data()
        date_str = datetime.now().strftime('%Y-%m-%d')
        report_file = data_dir / f'rapport_{date_str}.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"✓ {report_file.name} généré ({report_file.stat().st_size} bytes)")
        
        print(f"\n📁 Fichiers générés dans: {data_dir}")
        print(f"\n✅ Données de test prêtes!")
        print("   Lancer: python serve_dashboard.py")
        print("   Puis accéder à: http://localhost:8000")
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
