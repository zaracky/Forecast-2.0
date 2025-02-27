import time
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Connexion MongoDB avec DB et Collection depuis .env
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def measure_query_time(date, location):
    """Mesure le temps d'exécution d'une requête MongoDB."""
    query = {
        "weather_data.timestamp": {"$gte": date + "T00:00:00", "$lt": date + "T23:59:59"},
        "location.city": location
    }

    start_time = time.time()  # Début du chronométrage
    result = list(collection.find(query))  # Exécute la requête
    end_time = time.time()  # Fin du chronométrage

    execution_time = end_time - start_time
    print(f"Temps d'exécution de la requête : {execution_time:.4f} secondes")
    print(f"Nombre de résultats : {len(result)}")

    return execution_time
