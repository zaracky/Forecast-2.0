import pandas as pd
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

def check_data_quality():
    """Analyse la qualité des données stockées dans MongoDB."""
    data = list(collection.find({}, {"_id": 0}))  # Récupérer les données
    df = pd.DataFrame(data)

    if df.empty:
        print("Aucune donnée disponible pour l'analyse.")
        return

    # Vérifier les valeurs nulles
    missing_values = df.isnull().sum()
    missing_rate = (missing_values / len(df)) * 100

    # Vérifier les doublons
    duplicates = df.duplicated().sum()

    # Vérifier si les valeurs sont dans une plage acceptable
    temp_out_of_range = ((df['weather_data'].apply(lambda x: x.get('temperature', None)) < -50) |
                         (df['weather_data'].apply(lambda x: x.get('temperature', None)) > 60)).sum()

    humidity_out_of_range = ((df['weather_data'].apply(lambda x: x.get('humidity', None)) < 0) |
                             (df['weather_data'].apply(lambda x: x.get('humidity', None)) > 100)).sum()

    # Résumé des erreurs
    print("\n===== Qualité des données =====")
    print(f"Valeurs manquantes (% par colonne) :\n{missing_rate}")
    print(f"Nombre de doublons : {duplicates}")
    print(f"Nombre de températures hors plage : {temp_out_of_range}")
    print(f"Nombre d'humidités hors plage : {humidity_out_of_range}")

    return {
        "missing_values": missing_values.to_dict(),
        "missing_rate": missing_rate.to_dict(),
        "duplicates": duplicates,
        "temp_out_of_range": temp_out_of_range,
        "humidity_out_of_range": humidity_out_of_range
    }
