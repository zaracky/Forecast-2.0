from pymongo import MongoClient, WriteConcern
import os
from dotenv import load_dotenv

# Charger variables
load_dotenv()

# Connexion à un ReplicaSet simulé
MONGO_URI = os.getenv("MONGO_URI")  # Exemple : mongodb://user:pass@host1,host2,host3/?replicaSet=rs0
REPLICA_SET_NAME = "rs0"  # Nom fictif de ReplicaSet

client = MongoClient(
    MONGO_URI,
    replicaset=REPLICA_SET_NAME,
    serverSelectionTimeoutMS=5000
)

db = client["forecast2"]
collection = db.get_collection(
    "weather_stations",
    write_concern=WriteConcern(w=2, wtimeout=5000)  # Demande écriture sur au moins 2 membres
)

# Insertion avec write concern w=2
doc = {
    "StationID": "TEST_REPLICATION",
    "Temperature": 25,
    "City": "SimulationReplica"
}

try:
    result = collection.insert_one(doc)
    print(f"Document inséré avec succès : {result.inserted_id}")
except Exception as e:
    print(f"Erreur lors de l'insertion avec réplication : {e}")

# Lecture du status du ReplicaSet (optionnel)
try:
    repl_status = client.admin.command("replSetGetStatus")
    print("\nEtat du ReplicaSet :")
    for member in repl_status.get("members", []):
        print(f"- {member['name']} : {member['stateStr']}")
except Exception as e:
    print(f"Erreur lors de la lecture du statut de réplication : {e}")
