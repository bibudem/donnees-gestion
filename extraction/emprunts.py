import csv
import logging
import configparser
import argparse
import sys
import os
import re
from datetime import datetime
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour lire le fichier des emprunts et produire un fichier CSV prêt à charger dans l\'entrepôt')
    parser.add_argument('--fichier_entree', required=True, help='Chemin du fichier des emprunts')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie')
    return parser.parse_args()

# Fonction pour vérifier et convertir la date si nécessaire
def convert_and_check_date(date_str):
    # Expression régulière pour vérifier le format YYYY-MM-DD
    pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
    
    # Vérifier si la date est déjà au format YYYY-MM-DD
    if pattern.match(date_str):
        return date_str  # La date est déjà au bon format, donc on la retourne telle quelle
    
    # Sinon, convertir la date du format "DD/MM/YYYY" au format "YYYY-MM-DD"
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    return date_obj.strftime('%Y-%m-%d')

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/emprunts.py")

logger.info(f"Début de l'extraction des données de prêt")

# Les arguments en ligne de commande
args = parse_arguments()


# Ouvrir le fichier de données délimité par des TAB
with open(args.fichier_entree, 'r', encoding='utf8') as fichier_entree, open(args.fichier_sortie, 'w', encoding='utf8', newline='') as fichier_sortie:
    lecteur = csv.DictReader(fichier_entree, delimiter='\t')
    entetes = lecteur.fieldnames
    reader = csv.DictWriter(fichier_sortie, fieldnames=entetes, delimiter='\t')
    reader.writeheader()
    # Parcourir chaque ligne du fichier d'entrée
    for ligne in lecteur:
        # Vérifier et convertir la date si nécessaire
        ligne['date'] = convert_and_check_date(ligne['date'])
#        print(ligne['date'])
        # Écrire la ligne dans le fichier de sortie
        reader.writerow(ligne)
