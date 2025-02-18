# Weather Data Processing
Ce projet consiste à récupérer des fichiers de données météo depuis un bucket S3, à les transformer en fonction des métadonnées de chaque station, et à insérer les données dans une base de données MongoDB. Le script prend en charge différents formats de fichiers (Excel et JSON) et teste l'intégrité des données avant de les importer dans MongoDB.

## Prérequis
Avant de commencer, assurez-vous d'avoir les éléments suivants :

-Un compte AWS et un accès aux services S3.

-Une base de données MongoDB fonctionnelle.

-Un environnement Python 3.x configuré.

## Installation
### 1. Cloner ce repository (si nécessaire)
 
`git clone https://votre-repository.git`

`cd weather-data-processing`

### 2. Installer les dépendances
Assurez-vous que l'environnement virtuel est activé et installez les modules nécessaires via le fichier requirements.txt :

`pip install -r requirements.txt`

### 3. Configurer les variables d'environnement
Créez un fichier .env à la racine du projet et remplissez-le avec vos informations sensibles :
`AWS_ACCESS_KEY=your_aws_access_key`
`AWS_SECRET_KEY=your_aws_secret_key`
`MONGO_URI=mongodb://localhost:27017/`
`S3_BUCKET_NAME=your-s3-bucket-name`

## Utilisation
### 1. Lancer le script
Une fois que votre environnement virtuel est configuré et que les variables d'environnement sont chargées, vous pouvez exécuter le script principal :
`python weather_data_processor.py`

Le script va :

-Télécharger les fichiers depuis le bucket S3 spécifié.

-Traiter les données météo contenues dans les fichiers Excel ou JSON.

-Ajouter les métadonnées de la station météo pour chaque fichier Excel.

-Tester l'intégrité des données avant de les insérer dans MongoDB.

-Insérer les données traitées dans une collection MongoDB.

### 2. Structure du projet
Le projet contient les fichiers suivants :

weather-data-processing/

├── weather_data_processor.py   # Script principal de traitement des données

├── requirements.txt            # Liste des dépendances Python

├── .env                        # Fichier de configuration des variables d'environnement

├── README.md                   # Documentation du projet

└── data/                       # Dossier contenant les fichiers récupérés depuis S3

## Fonctionnalités du script
-Lecture des fichiers S3 : Le script récupère les fichiers météo depuis un bucket S3.

-Transformation des données : Les données sont converties (par exemple, les températures en Fahrenheit sont converties en Celsius).

-Ajout de métadonnées : Les métadonnées de chaque station sont ajoutées aux documents avant l'insertion dans MongoDB.

-Vérification de l'intégrité des données : Le script vérifie les colonnes disponibles, les types de données, les doublons et les valeurs manquantes pour assurer l'intégrité des données.

-Insertion dans MongoDB : Les données sont insérées dans la base de données MongoDB dans la collection weather_stations.

## Tests d'intégrité
Pour chaque fichier JSON, le script vérifie les éléments suivants :

-Colonnes disponibles.

-Types des variables.

-Doublons dans les données.

-Valeurs manquantes dans les colonnes.

## Contribution
Si vous souhaitez contribuer à ce projet, vous pouvez effectuer un fork du repository, puis créer une pull request avec vos modifications.

## Ajouter de nouvelles fonctionnalités
Si vous ajoutez de nouvelles fonctionnalités ou des sources de données, n'oubliez pas de mettre à jour ce fichier README.md.
Vous pouvez également ajouter des tests pour vérifier les nouvelles fonctionnalités.

## Support
Si vous avez des questions ou des problèmes avec le projet, n'hésitez pas à ouvrir une issue sur le repository.

