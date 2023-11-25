import csv
import logging
import configparser
import argparse
import sys
import os
sys.path.append(os.path.abspath("../commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour lire le fichier des étudiants et produire un fichier CSV prêt à charger dans l\'entrepôt')
    parser.add_argument('--fichier_entree', required=True, help='Chemin du fichier des étudiants')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("etudiants.py")

logger.info(f"Début de l'extraction des données des étudiants")

# Les arguments en ligne de commande
args = parse_arguments()

# Définir les positions de début et de fin de chaque champ
# Positions: à compter de 0, et la deuxième valeur n'est pas incluse
positions_champs = [(11, 25), (423, 428), (458, 464), (468, 498), (498, 567), (568, 598)]

# Ouvrir le fichier de données à largeur fixe en mode lecture
with open(args.fichier_entree, 'r', encoding='ISO-8859-1') as fichier_entree:
    # Ouvrir le fichier CSV en mode écriture
    with open(args.fichier_sortie, 'w', newline='') as fichier_sortie:
        # Créer un objet writer pour écrire dans le fichier CSV
        writer = csv.writer(fichier_sortie)

        # Écrire l'en-tête du fichier CSV si nécessaire
        writer.writerow(['codebarres', 'codeCycle', 'codeProgramme', 'programme', 'courriel', 'login'])

        # Lire chaque ligne du fichier de données à largeur fixe
        for ligne in fichier_entree:
            # Extraire les champs en fonction des positions spécifiées
            champs_extraits = [ligne[start:end].strip() for start, end in positions_champs]

            # Écrire les champs extraits dans le fichier CSV
            writer.writerow(champs_extraits)
