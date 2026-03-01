import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import os
broker = os.getenv('KAFKA_BROKER', 'redpanda:9092')

# Configuration du producteur
producer = KafkaProducer(
    bootstrap_servers=[broker], # <-- On utilise l'adresse interne au réseau Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'client_tickets'

# Données pour la simulation
types_demande = ['Technique', 'Facturation', 'Commercial', 'Autre']
priorites = ['Basse', 'Moyenne', 'Haute', 'Urgent']
exemples_demandes = [
    "Impossible de se connecter", 
    "Erreur sur la facture de janvier", 
    "Demande de devis pour nouveau capteur",
    "Latence réseau élevée"
]

def generate_ticket(ticket_id):
    """Génère un ticket aléatoire conforme à l'énoncé """
    return {
        "ticket_id": ticket_id,
        "client_id": f"CLI-{random.randint(100, 999)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "demande": random.choice(exemples_demandes),
        "type_demande": random.choice(types_demande),
        "priorite": random.choice(priorites)
    }

print(f"Démarrage de l'envoi vers le topic '{TOPIC}'...")

try:
    count = 1
    while True:
        ticket = generate_ticket(count)
        producer.send(TOPIC, ticket)
        print(f"Ticket envoyé : {ticket}")
        count += 1
        time.sleep(2)  # Simule un flux en temps réel
except KeyboardInterrupt:
    print("Arrêt du producteur.")
finally:
    producer.close()