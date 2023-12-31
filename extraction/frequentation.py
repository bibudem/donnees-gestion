import pyodbc
import csv
import logging
import configparser
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour extraire les données de fréquentation et d\'occupation dans des fichiers CSV')
    parser.add_argument('--date_debut', required=True, help='Date de début (aaaa-mm-dd)')
    parser.add_argument('--date_fin', required=True, help='Date de fin (aaaa-mm-dd)')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie (sans l\'extension)')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/frequentation.py")

logger.info(f"Début de l'extraction des données de fréquentation et d'occupation")

# Configuration de l'accès à la base de données
config = configparser.ConfigParser()
config.read('_config.ini')

# Lest arguments en ligne de commande
args = parse_arguments()

# Établir une connexion à la base de données
conn_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={config['sqlserver-freq']['server']};DATABASE={config['sqlserver-freq']['database']};UID={config['sqlserver-freq']['username']};PWD={config['sqlserver-freq']['password']};TrustServerCertificate=yes"
conn = pyodbc.connect(conn_string)

# Créer un curseur
cursor = conn.cursor()

# On commence avec les données de fréquentation

# Exécuter une requête SELECT
cursor.execute(f"SELECT * FROM Entrees WHERE CAST(Date as Date) >= '{args.date_debut}' AND CAST(Date as Date) <= '{args.date_fin}';")

# Récupérer les résultats
resultats = cursor.fetchall()

# Nom des colonnes
colonnes = [col[0] for col in cursor.description]

# Écrire les résultats dans un fichier CSV
with open(args.fichier_sortie + "_frequentation.csv", 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Écrire les noms de colonnes
    writer.writerow(colonnes)
    
    # Écrire les données
    for row in resultats:
        writer.writerow(row)

# On poursuit avec les données d'occupation

# Exécuter une requête SELECT
cursor.execute(f"SELECT * FROM Occupation WHERE CAST(Date as Date) >= '{args.date_debut}' AND CAST(Date as Date) <= '{args.date_fin}';")

# Récupérer les résultats
resultats = cursor.fetchall()

# Nom des colonnes
colonnes = [col[0] for col in cursor.description]

# Écrire les résultats dans un fichier CSV
with open(args.fichier_sortie + "_occupation.csv", 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Écrire les noms de colonnes
    writer.writerow(colonnes)
    
    # Écrire les données
    for row in resultats:
        writer.writerow(row)


# Fermer le curseur et la connexion
cursor.close()
conn.close()
