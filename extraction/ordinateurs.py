import pyodbc
import csv
import logging
import configparser
import argparse
import sys
import os
sys.path.append(os.path.abspath("../commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour extraire les données d\'utilisation des postes publics dans un fichier CSV')
    parser.add_argument('--debut', required=True, help='Date de début (aaaa-mm-dd)')
    parser.add_argument('--fin', required=True, help='Date de fin (aaaa-mm-dd)')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("ordinateurs.py")

logger.info(f"Début de l'extraction des données d'utilisation des postes publics")

# Configuration de l'accès à la base de données
config = configparser.ConfigParser()
config.read('../config/_ordinateurs.ini')

# Lest arguments en ligne de commande
args = parse_arguments()

# Établir une connexion à la base de données
conn_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={config['sqlserver']['server']};DATABASE={config['sqlserver']['database']};UID={config['sqlserver']['username']};PWD={config['sqlserver']['password']};TrustServerCertificate=yes"
conn = pyodbc.connect(conn_string)

# Créer un curseur
cursor = conn.cursor()

# Exécuter une requête SELECT
cursor.execute(f"SELECT * FROM Enregistrements_calcules WHERE CAST(Ouverture_session AS Date) >= '{args.debut}' AND CAST(Ouverture_session AS Date) <= '{args.fin}'")

# Récupérer les résultats
resultats = cursor.fetchall()

# Nom des colonnes
colonnes = [col[0] for col in cursor.description]

# Fermer le curseur et la connexion
cursor.close()
conn.close()

# Écrire les résultats dans un fichier CSV
with open(args.fichier_sortie, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Écrire les noms de colonnes
    writer.writerow(colonnes)
    
    # Écrire les données
    for row in resultats:
        writer.writerow(row)
