import csv
import logging
import configparser
import argparse
import sys
import os
import gzip
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour lire le fichier des étudiants et produire un fichier CSV prêt à charger dans l\'entrepôt')
    parser.add_argument('--fichier_entree', required=True, help='Chemin du fichier des étudiants')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/etudiants.py")

logger.info(f"Début de l'extraction des données des étudiants")

# Les arguments en ligne de commande
args = parse_arguments()

# Définir les positions de début et de fin de chaque champ
# Positions: à compter de 0, et la deuxième valeur n'est pas incluse
positions_champs = [(11, 25), (423, 428), (458, 464), (468, 498), (498, 567), (568, 598)]

def traiter_fichier(fichier_entree, sortie):
    with open(sortie, 'w', encoding='utf8', newline='') as fichier_sortie:
        # Créer un objet writer pour écrire dans le fichier CSV
        writer = csv.writer(fichier_sortie)

        # Écrire l'en-tête du fichier CSV si nécessaire
        writer.writerow(['codebarres', 'codeCycle', 'codeProgramme', 'programme', 'courriel', 'login'])

        # Lire chaque ligne du fichier de données à largeur fixe
        for ligne in fichier_entree:
            # Extraire les champs en fonction des positions spécifiées
            champs_extraits = [ligne[start:end].strip() for start, end in positions_champs]

            # Vérifier si la colonne "login" est nulle ou vide
            if champs_extraits[-1]:  # -1 correspond à la dernière colonne, supposée être "login"
                # Écrire les champs extraits dans le fichier CSV
                writer.writerow(champs_extraits)


# On va faire une distinction selon que le fichier est compressé (.gz) ou non

if (args.fichier_entree.endswith(".gz")):
    with gzip.open(args.fichier_entree, "rt", encoding="ISO-8859-1") as f:
        traiter_fichier(f, args.fichier_sortie)
else:
    with open(args.fichier_entree, "r", encoding="ISO-8859-1") as f:
        traiter_fichier(f, args.fichier_sortie)


