# ==============================================================================
# kafka_producer.py — Producteur Kafka
# Simule des courses NYC Taxi en temps réel
#
# Utilise confluent-kafka (compatible Python 3.12)
# Envoie 2 courses par seconde vers le topic 'nyc_taxi_rides'
# ==============================================================================

import json
import random
import time
import os
from datetime import datetime
from confluent_kafka import Producer

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = "nyc_taxi_rides"

# ─────────────────────────────────────────────────────────────────────────────
# Initialisation du producteur
# ─────────────────────────────────────────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "nyc_taxi_producer",
})

# Données de référence NYC TLC
LOCATIONS = [132, 138, 161, 237, 236, 186, 230, 162, 142, 163,
             48, 68, 79, 87, 90, 100, 107, 114, 125, 170]
PAYMENT_TYPES = [1, 2, 3, 4]  # 1=CC, 2=Cash, 3=No charge, 4=Dispute
VENDOR_IDS = [1, 2]


def delivery_report(err, msg):
    """Callback appelé après chaque envoi pour confirmer la livraison."""
    if err is not None:
        print(f"❌ Erreur d'envoi : {err}")


def generate_ride() -> dict:
    """Génère une course NYC Taxi aléatoire réaliste."""
    distance = round(random.uniform(0.5, 25.0), 2)
    fare = round(distance * random.uniform(2.5, 4.5), 2)
    tip = round(fare * random.uniform(0, 0.3), 2)
    tolls = round(random.choice([0, 0, 0, 5.76, 6.55]), 2)
    total = round(fare + tip + tolls + 3.5, 2)  # 3.5 = surcharge

    return {
        "vendor_id": random.choice(VENDOR_IDS),
        "pickup_datetime": datetime.now().isoformat(),
        "pickup_location_id": random.choice(LOCATIONS),
        "dropoff_location_id": random.choice(LOCATIONS),
        "passenger_count": random.randint(1, 4),
        "trip_distance": distance,
        "fare_amount": fare,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "total_amount": total,
        "payment_type": random.choice(PAYMENT_TYPES),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚕 Producteur démarré → topic '{TOPIC}' sur {KAFKA_BOOTSTRAP}")
    print("Envoi de 2 courses par seconde... (Ctrl+C pour arrêter)\n")

    count = 0
    try:
        while True:
            ride = generate_ride()
            producer.produce(
                TOPIC,
                value=json.dumps(ride).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1

            if count % 10 == 0:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"✅ {count} courses envoyées | "
                    f"Dernière : zone {ride['pickup_location_id']} → "
                    f"zone {ride['dropoff_location_id']} | "
                    f"${ride['total_amount']}"
                )

            time.sleep(0.5)   # 2 courses par seconde

    except KeyboardInterrupt:
        print(f"\n⏹ Arrêt du producteur. Total envoyé : {count} courses.")
    finally:
        producer.flush()
        print("✅ Producteur arrêté proprement.")