import boto3
import pandas as pd
import json
import os
from io import BytesIO

# Configuration AWS
BUCKET_NAME = "ton-bucket"
AWS_ACCESS_KEY = "ta-clé"
AWS_SECRET_KEY = "ta-clé-secrète"

# Connexion S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def download_from_s3(file_key):
    """Télécharge un fichier depuis S3."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    return obj['Body'].read()

def load_data(file_key):
    """Charge le fichier JSON ou Excel depuis S3."""
    file_data = download_from_s3(file_key)
    
    if file_key.endswith(".json"):
        return pd.read_json(BytesIO(file_data), lines=True)
    elif file_key.endswith(".xlsx"):
        return pd.read_excel(BytesIO(file_data))
    else:
        raise ValueError("Format non supporté")

def validate_data(df):
    """Effectue des vérifications basiques sur les données."""
    print("Validation des données :")
    print("Colonnes disponibles :", df.columns.tolist())
    print("Types des variables :", df.dtypes)
    print("Valeurs manquantes :", df.isnull().sum())
    print("Doublons :", df.duplicated().sum())
    print("Nombre total de lignes :", len(df))

def transform_data(df):
    """Transformation des données pour MongoDB."""
    df.fillna("", inplace=True)  # Remplacer NaN par une chaîne vide
    return df.to_dict(orient="records")

def save_to_json(data, output_file):
    """Sauvegarde des données transformées en JSON."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def main(file_keys):
    for file_key in file_keys:
        print(f"Traitement du fichier : {file_key}")
        df = load_data(file_key)
        validate_data(df)
        transformed_data = transform_data(df)
        validate_data(pd.DataFrame(transformed_data))
        output_file = f"output_{os.path.basename(file_key)}.json"
        save_to_json(transformed_data, output_file)
        print(f"Données transformées sauvegardées dans {output_file}")

if __name__ == "__main__":
    file_keys = [
        "chemin/vers/fichier1.xlsx", 
        "chemin/vers/fichier2.xlsx", 
        "chemin/vers/fichier3.json"
    ]
    main(file_keys)
