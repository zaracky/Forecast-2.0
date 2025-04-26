import os
import json
import time
from pymongo import MongoClient
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Variables
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "forecast2"
COLLECTION_NAME = "weather_stations"
JSON_FILE = "output/transformed_data.json"

def wait_for_file(filepath, timeout=30):
    """Attend que le fichier soit disponible avant de continuer."""
    start_time = time.time()
    while not os.path.isfile(filepath):
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timeout while waiting for {filepath}")
        time.sleep(1)

def connect_to_mongo(uri):
    """Connexion à MongoDB."""
    client = MongoClient(uri)
    return client

def create_unique_index(collection):
    """Créer un index unique sur StationID + _airbyte_extracted_at."""
    collection.create_index(
        [("StationID", 1), ("_airbyte_extracted_at", 1)],
        unique=True
    )

def load_json_data(file_path):
    """Chargement du fichier JSON."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def insert_data(collection, data):
    """Insertion sans doublons dans MongoDB."""
    inserted_count = 0
    skipped_count = 0

    for doc in data:
        query = {
            "StationID": doc.get("StationID"),
            "_airbyte_extracted_at": doc.get("_airbyte_extracted_at")
        }
        if not collection.find_one(query):
            collection.insert_one(doc)
            inserted_count += 1
        else:
            skipped_count += 1

    print(f"Insertion terminée : {inserted_count} nouveaux documents ajoutés, {skipped_count} doublons ignorés.")

def check_data_quality(collection):
    """Vérifie la qualité des données (valeurs manquantes, doublons)."""
    total_documents = collection.count_documents({})
    error_count = 0

    for doc in collection.find():
        if None in doc.values():
            error_count += 1

    if total_documents == 0:
        error_rate = 100
    else:
        error_rate = (error_count / total_documents) * 100

    print(f"Documents totaux : {total_documents}")
    print(f"Taux d'erreur : {error_rate:.2f}%")

def main():
    print("Chargement des données JSON...")
    wait_for_file(JSON_FILE)

    client = connect_to_mongo(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Crée un index unique avant insertion
    create_unique_index(collection)

    data = load_json_data(JSON_FILE)
    insert_data(collection, data)
    print("Données insérées dans MongoDB avec succès.")

    check_data_quality(collection)

if __name__ == "__main__":
    main()
