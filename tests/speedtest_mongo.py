import time
from pymongo import MongoClient
import os

# Paramètres de connexion
#MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "forecast2"
COLLECTION_NAME = "weather_stations"

def main():
    # Connexion
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Définir une requête simple
    query = {
        "StationID": "ILAMAD25"
    }

    # Chronométrer l'accès
    start_time = time.time()
    results = list(collection.find(query))
    end_time = time.time()

    elapsed_time_ms = (end_time - start_time) * 1000

    print(f"Requête exécutée en {elapsed_time_ms:.2f} ms")
    print(f"Nombre de documents récupérés : {len(results)}")

if __name__ == "__main__":
    main()
