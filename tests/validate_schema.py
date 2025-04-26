from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Charger variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)
db = client["forecast2"]
collection = db["weather_stations"]

# Définir ton "schéma attendu"
required_fields = ["StationID", "StationName", "Latitude", "Longitude", "Elevation", "City"]

total_docs = 0
invalid_docs = 0

for doc in collection.find():
    total_docs += 1
    if not all(field in doc and doc[field] is not None for field in required_fields):
        invalid_docs += 1

if total_docs > 0:
    error_rate = (invalid_docs / total_docs) * 100
else:
    error_rate = 100

print(f"Documents totaux : {total_docs}")
print(f"Documents invalides : {invalid_docs}")
print(f"Taux d'invalidité : {error_rate:.2f}%")
