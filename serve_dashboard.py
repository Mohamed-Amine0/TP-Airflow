#!/usr/bin/env python3
"""
Serveur HTTP simple pour servir le dashboard HTML et les données JSON

Utilisation:
  python serve_dashboard.py
  
Puis ouvrir http://localhost:8000 dans le navigateur
"""

import os
import sys
import json
import http.server
import socketserver
from pathlib import Path
from datetime import datetime

# Configuration
PORT = 8000
HOST = 'localhost'
SCRIPT_DIR = Path(__file__).parent.absolute()
DATA_DIR = SCRIPT_DIR / 'data' / 'wikimedia'


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Handler personnalisé pour servir le dashboard et les données"""
    
    def do_GET(self):
        """Gère les requêtes GET"""
        
        # Si c'est une requête pour les données JSON
        if self.path.startswith('/data/'):
            self.serve_data_file()
        # Servir le dashboard par défaut
        elif self.path == '/' or self.path == '':
            self.serve_dashboard()
        else:
            super().do_GET()
    
    def serve_dashboard(self):
        """Sert le fichier dashboard.html"""
        dashboard_file = SCRIPT_DIR / 'dashboard.html'
        
        if not dashboard_file.exists():
            self.send_error(404, "dashboard.html not found")
            return
        
        with open(dashboard_file, 'rb') as f:
            content = f.read()
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(content))
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.end_headers()
        self.wfile.write(content)
    
    def serve_data_file(self):
        """Sert un fichier JSON du répertoire data/wikimedia"""
        
        # Extraire le nom du fichier depuis l'URL
        filename = self.path.replace('/data/', '').split('?')[0]
        
        # Sécurité : empêcher les chemins qui sortent du répertoire
        if '..' in filename or '/' in filename:
            self.send_error(400, "Invalid path")
            return
        
        filepath = DATA_DIR / filename
        
        if not filepath.exists():
            # Si le fichier n'existe pas, retourner des données vides
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "File not found", "file": filename}).encode())
            return
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            self.send_error(500, f"Error reading file: {e}")
            return
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Personnalise les logs"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {format % args}")


def main():
    """Démarre le serveur HTTP"""
    
    os.chdir(SCRIPT_DIR)
    
    print("\n" + "="*60)
    print("🎯 Wikimedia Analytics Dashboard Server")
    print("="*60)
    print(f"\n📊 Accès au dashboard: http://{HOST}:{PORT}")
    print(f"📁 Répertoire de données: {DATA_DIR}")
    print(f"🌐 Serveur HTTP: {HOST}:{PORT}")
    print("\nFichiers JSON disponibles:")
    
    if DATA_DIR.exists():
        for json_file in sorted(DATA_DIR.glob('*.json*')):
            size = (json_file.stat().st_size / 1024)
            print(f"  ✓ {json_file.name} ({size:.1f} KB)")
    else:
        print("  ⚠️  Répertoire de données non trouvé")
    
    print("\n💡 Astuces:")
    print("  - Le dashboard se met à jour automatiquement chaque 5 secondes")
    print("  - Accédez aux fichiers JSON via: /data/activity_by_hour.json")
    print("  - Appuyez sur Ctrl+C pour arrêter le serveur")
    print("\n" + "="*60 + "\n")
    
    try:
        with socketserver.TCPServer(("", PORT), DashboardHandler) as httpd:
            print(f"✅ Serveur démarré sur http://{HOST}:{PORT}\n")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n❌ Serveur arrêté")
        sys.exit(0)
    except OSError as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
