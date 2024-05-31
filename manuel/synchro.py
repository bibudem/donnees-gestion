import configparser
import logging
import psycopg2
import argparse
import sys
import os
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, copy_from_csv

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour extraire, charger et transformer les données Synchro dans l\'entrepôt')
    parser.add_argument('--dossier', required=True, help='Chemin du dossier où se trouvent les fichiers Synchro à charger')
    parser.add_argument('--sortie', required=True, help='Chemin du dossier où déposer les fichiers de sortie (temporaires)')
    parser.add_argument('--session', required=False, default=False)
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("manuel/synchro.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Chemin du fichier CSV à charger
chemin_dossier = args.dossier
dossier_sortie = args.sortie
session = args.session

# On boucle sur les fichiers dans le dossier
dossier = Path(chemin_dossier)
for fichier in dossier.iterdir():
    if fichier.is_file():

        # On obtient la date depuis le nom du fichier
        chiffres = re.search(r'\d{8}', fichier.name)
        if chiffres:
            jour = datetime.strptime(chiffres.group(0), "%Y%m%d") - timedelta(days=1)
            jour_iso = jour.strftime("%Y-%m-%d")

            # On boucle seulement sur les fichiers étudiants
            if (fichier.name.startswith("synchro_ac")):
                fichier_etudiants = str(fichier.resolve())
                fichier_personnel = fichier_etudiants.replace("/synchro_ac.TXT", "/synchro_rh.txt")
                nom_fichier_etudiants = fichier.name
                nom_fichier_personnel = nom_fichier_etudiants.replace("synchro_ac.TXT", "synchro_rh.txt")
                fichier_csv_etudiants = dossier_sortie + os.sep + nom_fichier_etudiants + ".csv"
                fichier_csv_personnel = dossier_sortie + os.sep + nom_fichier_personnel + ".csv"

                print(datetime.now().isoformat() + " - Traitement du fichier Synchro " + nom_fichier_etudiants)

                # Extraction des données
                resultat = subprocess.run(["python", "extraction/etudiants.py", "--fichier_entree", fichier_etudiants, "--fichier_sortie", fichier_csv_etudiants], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    logger.error("Erreur dans l'extraction des données de Synchro AC " + fichier_etudiants + " : " + resultat.stderr)
                resultat = subprocess.run(["python", "extraction/personnel.py", "--fichier_entree", fichier_personnel, "--fichier_sortie", fichier_csv_personnel], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    logger.error("Erreur dans l'extraction des données de Synchro RH " + fichier_personnel + " : " + resultat.stderr)
                # Chargement des données
                resultat = subprocess.run(["python", "chargement/etudiants.py", "--jour", jour_iso, "--fichier", fichier_csv_etudiants], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    logger.error("Erreur dans le chargement des données de Synchro AC " + fichier_csv_etudiants + " : " + resultat.stderr)
                resultat = subprocess.run(["python", "chargement/personnel.py", "--jour", jour_iso, "--fichier", fichier_csv_etudiants], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    logger.error("Erreur dans le chargement des données de Synchro RH " + fichier_csv_etudiants + " : " + resultat.stderr)
                # On peut maintenant supprimer les fichiers CSV
                os.remove(fichier_csv_etudiants)
                os.remove(fichier_csv_personnel)
                # Transformation des données
                if (session):
                    resultat = subprocess.run(["python", "transformation/clienteles.py", "--session", jour_iso], capture_output=True, text=True)
                else:
                    resultat = subprocess.run(["python", "transformation/clienteles.py"], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    logger.error("Erreur dans la transformation des données de Synchro " + jour_iso + " : " + resultat.stderr)
