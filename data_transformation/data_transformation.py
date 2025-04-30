import pandas as pd
import json
import boto3
import io
import os
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Charger les variables d'environnement pour AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")
S3_BUCKET_NAME = os.getenv("AWS_SECRET_BUCKET_NAME")

# Configuration des stations météo à intégrer
METADATA_STATIONS = {
    "Station_Amateur_Weather_La_Madelaine": {
        "StationID": "ILAMAD25",
        "StationName": "La Madeleine",
        "Latitude": 50.659,
        "Longitude": 3.07,
        "Elevation": 23,
        "City": "La Madeleine",
        "Hardware": "other",
        "Software": "EasyWeatherPro_V5.1.6"
    },
    "Station_Amateur_Ichtegem": {
        "StationID": "IICHTE19",
        "StationName": "WeerstationBS",
        "Latitude": 51.092,
        "Longitude": 2.999,
        "Elevation": 15,
        "City": "Ichtegem",
        "Hardware": "other",
        "Software": "EasyWeatherV1.6.6"
    },
    "Stations_meteorologique_du_reseau_infoClimat": {
        "StationID": "UNKNOWN",
        "StationName": "InfoClimatStation",
        "Latitude": None,
        "Longitude": None,
        "Elevation": None,
        "City": None,
        "Hardware": None,
        "Software": None
    }
}

# Fonction pour se connecter à S3
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

# Fonction pour lire un fichier CSV depuis S3
def read_file_from_s3(key):
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

# Trouver le seul fichier dans le dossier donné sur S3
def get_single_csv_file_key(s3_prefix):
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix)

    if "Contents" not in response:
        raise FileNotFoundError(f"Aucun fichier trouvé avec le préfixe : {s3_prefix}")

    # Ne garder que les .csv
    csv_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]

    if len(csv_files) == 0:
        raise FileNotFoundError(f"Aucun fichier .csv trouvé dans : {s3_prefix}")
    elif len(csv_files) > 1:
        raise RuntimeError(f"Plus d’un fichier .csv trouvé dans : {s3_prefix}")

    return csv_files[0]

# Fonction de nettoyage et standardisation
def transform_data(df, station_folder_name):
    metadata = METADATA_STATIONS.get(station_folder_name, {})

    # Nettoyage des données
    df = df.drop_duplicates()
    df = df.dropna()

    # Typage correct
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Ajout des métadonnées de station
    for key, value in metadata.items():
        df[key] = value

    df["StationFolder"] = station_folder_name

    return df

# Fonction principale
def main():
    station_folders = {
        "Station_Amateur_Weather_La_Madelaine": "airbyte/Station_Amateur_Weather_La_Madelaine/",
        "Station_Amateur_Ichtegem": "airbyte/Station_Amateur_Ichtegem/",
        "Stations_meteorologique_du_reseau_infoClimat": "airbyte/Stations_meteorologique_du_reseau_infoClimat/"
    }

    dfs = []

    for station_name, s3_prefix in station_folders.items():
        try:
            s3_key = get_single_csv_file_key(s3_prefix)
            print(f"Fichier détecté pour {station_name} : {s3_key}")
            df = read_file_from_s3(s3_key)
            df_transformed = transform_data(df, station_name)
            dfs.append(df_transformed)
        except Exception as e:
            print(f"Erreur pour {station_name} : {e}")

    # Fusion des données
    final_df = pd.concat(dfs, ignore_index=True)
    final_records = final_df.to_dict(orient="records")

    # Sauvegarde en JSON
    os.makedirs("output", exist_ok=True)
    output_path = os.path.join("output", "transformed_data.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_records, f, ensure_ascii=False, indent=4)
