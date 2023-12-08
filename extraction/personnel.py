import csv
import logging
import configparser
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour lire le fichier du personnel et produire un fichier CSV prêt à charger dans l\'entrepôt')
    parser.add_argument('--fichier_entree', required=True, help='Chemin du fichier du personnel')
    parser.add_argument('--fichier_sortie', required=True, help='Chemin du fichier CSV de sortie')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/personnel.py")

logger.info(f"Début de l'extraction des données du personnel")

# Les arguments en ligne de commande
args = parse_arguments()

# Définir les positions de début et de fin de chaque champ
# Positions: à compter de 0, et la deuxième valeur n'est pas incluse
positions_champs = [(214, 221), (175, 214), (221, 222), (222, 302), (302, 316), (361, 391), (391, 399)]

# Ouvrir le fichier de données à largeur fixe en mode lecture
with open(args.fichier_entree, 'r', encoding='ISO-8859-1') as fichier_entree:
    # Ouvrir le fichier CSV en mode écriture
    with open(args.fichier_sortie, 'w', encoding='utf8', newline='') as fichier_sortie:
        # Créer un objet writer pour écrire dans le fichier CSV
        writer = csv.writer(fichier_sortie)

        # Écrire l'en-tête du fichier CSV si nécessaire
        writer.writerow(['CodeUnite', 'DescUnite', 'Statut', 'Courriel', 'CodeBarres', 'Fonction', 'Login'])

        # Lire chaque ligne du fichier de données à largeur fixe
        for ligne in fichier_entree:
            # Extraire les champs en fonction des positions spécifiées
            champs_extraits = [ligne[start:end].strip() for start, end in positions_champs]

            # Écrire les champs extraits dans le fichier CSV
            writer.writerow(champs_extraits)

