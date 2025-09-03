#!/usr/bin/env python3
"""
Mock server Flask pour tester l'API sans Django
Sauvegarde ce fichier comme mock_server.py
"""

from flask import Flask, request, jsonify
import uuid

app = Flask(__name__)

@app.route('/api/v2/pharmacy/create', methods=['POST'])
def create_pharmacy():
    print("\n" + "="*50)
    print("🏥 REQUÊTE REÇUE - Création pharmacie")
    print("="*50)
    
    # Vérifier le Content-Type
    if not request.is_json:
        print("❌ Content-Type n'est pas JSON")
        return jsonify({'error': 'Content-Type doit être application/json'}), 400
    
    data = request.json
    print(f"📦 Données reçues: {data}")
    
    # Validation des données
    if not data:
        print("❌ Pas de données JSON")
        return jsonify({'error': 'Body JSON requis'}), 400
    
    if not data.get('name'):
        print("❌ Nom manquant")
        return jsonify({'error': 'Nom de pharmacie requis'}), 400
    
    if not data.get('id_nat'):
        print("❌ ID national manquant")
        return jsonify({'error': 'ID national requis'}), 400
    
    # Simuler la création
    pharmacy_id = str(uuid.uuid4())
    response = {
        'message': f"Pharmacie '{data['name']}' créée avec succès",
        'pharmacy_id': pharmacy_id,
        'id_nat': data['id_nat'],
        'status': 'created'
    }
    
    print(f"✅ Réponse envoyée: {response}")
    print("="*50)
    
    return jsonify(response), 201

@app.route('/api/v2/pharmacy/create', methods=['GET'])
def create_pharmacy_get():
    return jsonify({
        'error': 'Méthode GET non supportée',
        'message': 'Utilisez POST avec un body JSON'
    }), 405

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'message': 'Mock Server actif',
        'endpoints': [
            'POST /api/v2/pharmacy/create'
        ]
    })

if __name__ == '__main__':
    print("🚀 Mock Server Flask démarré")
    print("📍 URL: http://127.0.0.1:8000")
    print("📋 Endpoint: POST /api/v2/pharmacy/create")
    print("⏹️  Arrêter avec Ctrl+C")
    print("-"*40)
    
    app.run(host='127.0.0.1', port=8000, debug=True)