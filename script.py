import pandas as pd
import boto3
from pymongo import MongoClient
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
import os

# Charger les variables depuis le fichier .env
load_dotenv()

# Récupération des variables d'environnement
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
MONGO_URI = os.getenv('MONGO_URI')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Connexion MongoDB
client = MongoClient(MONGO_URI)  # Utilisation de l'URI MongoDB depuis .env
db = client.weather_data
collection = db.weather_stations

# Connexion S3 avec les clés d'accès depuis .env
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Métadonnées des stations
station_metadata = {
    "ILAMAD25": {
        "station_name": "La Madeleine",
        "location": {
            "latitude": 50.659,
            "longitude": 3.07,
            "elevation": 23,
            "city": "La Madeleine",
            "state": None
        },
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6"
    },
    "IICHTE19": {
        "station_name": "WeerstationBS",
        "location": {
            "latitude": 51.092,
            "longitude": 2.999,
            "elevation": 15,
            "city": "Ichtegem",
            "state": None
        },
        "hardware": "other",
        "software": "EasyWeatherV1.6.6"
    }
}

# Fonction pour convertir les températures de Fahrenheit en Celsius
def fahrenheit_to_celsius(fahrenheit):
    return (fahrenheit - 32) * 5.0/9.0

# Fonction pour tester l'intégrité des données d'un DataFrame
def integrity_checks(df):
    # Vérifier les colonnes disponibles
    print(f"Colonnes disponibles : {df.columns.tolist()}")

    # Vérifier les types des variables
    print(f"Types des variables : {df.dtypes}")

    # Vérifier les doublons
    print(f"Nombre de doublons : {df.duplicated().sum()}")

    # Vérifier les valeurs manquantes
    print(f"Valeurs manquantes par colonne : \n{df.isnull().sum()}")

# Fonction pour lire et transformer les données météo depuis un fichier S3
def process_weather_data_from_s3(bucket_name, file_name):
    # Télécharger le fichier depuis S3
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_content = s3_response['Body'].read()
    
    # Si le fichier est un .xlsx, le charger avec pandas
    if file_name.endswith('.xlsx'):
        df = pd.read_excel(BytesIO(file_content))
        
        # Déterminer la station en fonction du nom du fichier
        if 'La Madeleine' in file_name:
            station_id = 'ILAMAD25'
        elif 'Ichtegem' in file_name:
            station_id = 'IICHTE19'
        else:
            raise ValueError("Nom de fichier inconnu ou station non trouvée dans le nom du fichier.")
        
        # Récupérer les métadonnées de la station
        station_info = station_metadata.get(station_id)
        
        if station_info is None:
            raise ValueError(f"Station ID {station_id} non trouvée dans les métadonnées")
        
        # Liste pour stocker les documents à insérer dans MongoDB
        documents = []

        # Parcours chaque ligne du fichier Excel pour transformer les données
        for _, row in df.iterrows():
            document = {
                "station_id": station_id,
                "station_name": station_info["station_name"],
                "location": station_info["location"],
                "hardware": station_info["hardware"],
                "software": station_info["software"],
                "weather_data": {
                    "timestamp": datetime.strptime(row['Time'], '%I:%M %p').replace(year=2025, month=2, day=18),  # Assumer une date, tu peux l'ajuster
                    "temperature": fahrenheit_to_celsius(row['Temperature'].replace(' °F', '').strip()),  # Conversion en Celsius
                    "dew_point": fahrenheit_to_celsius(row['Dew Point'].replace(' °F', '').strip()),  # Conversion en Celsius
                    "humidity": int(row['Humidity'].replace(' %', '').strip()),  # Pourcentage
                    "wind_direction": row['Wind'].strip(),
                    "wind_speed": float(row['Speed'].replace(' mph', '').strip()),  # Conversion en mph
                    "wind_gust": float(row['Gust'].replace(' mph', '').strip()),  # Conversion en mph
                    "pressure": float(row['Pressure'].replace(' in', '').strip()),  # Pression en pouces de mercure
                    "precipitation_rate": float(row['Precip. Rate.'].replace(' in', '').strip()),  # Taux de précipitation en pouces
                    "precipitation_accumulated": float(row['Precip. Accum.'].replace(' in', '').strip()),  # Précipitations cumulées
                    "uv_index": int(row['UV']),
                    "solar_radiation": int(row['Solar'].replace(' w/m²', '').strip())  # Radiations solaires en W/m²
                }
            }
            
            # Ajouter le document à la liste
            documents.append(document)

        return documents
    
    # Si le fichier est un .json, le charger avec pandas
    elif file_name.endswith('.json'):
        df = pd.read_json(BytesIO(file_content))
        
        # Effectuer les tests d'intégrité sur les données JSON
        print(f"Test d'intégrité pour le fichier {file_name}:")
        integrity_checks(df)
        
        # Retourner les données brutes du fichier JSON pour insertion dans MongoDB sans métadonnées
        return df.to_dict('records')
    else:
        raise ValueError("Format de fichier non pris en charge")

# Fonction pour insérer les données dans MongoDB
def insert_weather_data(documents):
    if documents:
        collection.insert_many(documents)
        print(f"{len(documents)} documents insérés dans la base de données MongoDB.")
    else:
        print("Aucune donnée à insérer.")

# Fonction principale
def main():
    file_names = ['La_Madeleine.xlsx', 'Ichtegem.xlsx', 'data.json']  # Liste des fichiers dans ton bucket
    
    for file_name in file_names:
        # Traitement des données depuis S3
        documents = process_weather_data_from_s3(S3_BUCKET_NAME, file_name)
        
        # Insertion des documents dans MongoDB si nécessaire
        if isinstance(documents, list):  # Vérifie que les documents sont au format attendu
            insert_weather_data(documents)

# Exécution du script principal
if __name__ == "__main__":
    main()
