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

# Exemple d'utilisation
def main():
    # Fichiers précis dans ton S3
    files_info = {
        "Station_Amateur_Weather_La_Madelaine": "airbyte/Station_Amateur_Weather_La_Madelaine/2025_04_26_1745688137182_0.csv",
        "Station_Amateur_Ichtegem": "airbyte/Station_Amateur_Ichtegem/2025_04_26_1745686582564_0.csv",
        "Stations_meteorologique_du_reseau_infoClimat": "airbyte/Stations_meteorologique_du_reseau_infoClimat/2025_04_26_1745686581549_0.csv"
    }

    dfs = []

    for station_folder, s3_key in files_info.items():
        df = read_file_from_s3(s3_key)
        df_transformed = transform_data(df, station_folder)
        dfs.append(df_transformed)

    # Fusion de tous les DataFrames
    final_df = pd.concat(dfs, ignore_index=True)

    # Export au format JSON pour MongoDB
    final_records = final_df.to_dict(orient="records")

    # Sauvegarder localement (ou réécrire vers S3 si besoin)
    with open("transformed_data.json", "w", encoding='utf-8') as f:
        json.dump(final_records, f, ensure_ascii=False, indent=4)

    print("Transformation terminée. Fichier prêt pour MongoDB.")

if __name__ == "__main__":
    main()
