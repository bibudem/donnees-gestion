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
    # Expression régulière pour vérifier le format YYYY-mm-dd HH:MM:SS
    pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    
    if pattern.match(date_str):
        return date_str # La date est déjà au bon format, donc on la retourne telle quelle
    else :
        try:
            date_obj = datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
            return date_obj.strftime('%Y-%m-%d %H:%M:%S') # Convertir la date du format "dd/mm/YYYY HH:MM:SS" au format "YYYY-mm-dd HH:MM:SS"
        except ValueError:
            return None # La chaine analysée n'est pas une date, on retourne None pour supprimer l'enregistrement


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
    entetes = [colonne if colonne != 'Date/heure de l\'évènement' else 'date' for colonne in entetes] #Renommer la colonne "Date/heure de l'évènement"

    writer = csv.DictWriter(fichier_sortie, fieldnames=entetes, delimiter='\t')
    writer.writeheader()

    # Parcourir chaque ligne du fichier d'entrée
    for ligne in lecteur:
        # Convertir et remplacer la date
        ligne['date'] = convert_and_check_date(ligne['Date/heure de l\'évènement'])
        del ligne['Date/heure de l\'évènement'] # Supprimer l'ancien champ

        # Créer un nouveau dictionnaire contenant uniquement les champs présents dans les entêtes
        ligne_filtree = {colonne: ligne[colonne] for colonne in entetes if colonne in ligne}

        # Écrire la ligne dans le fichier de sortie
        writer.writerow(ligne_filtree) # Écrire la ligne dans le fichier de sortie
