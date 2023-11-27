# Script qui lance l'extraction, le chargement et la transformation
# des données quotidiennes dans l'entrepôt de données
from datetime import datetime, timedelta
import argparse
import os
import sys
import subprocess
import configparser


# Les fonctions utilisées dans le script
def parse_arguments():
    parser = argparse.ArgumentParser(description="Script qui pilote l'extraction, le chargement et la transformation des données quotidiennes dans l\'entrepôt")
    parser.add_argument('--dossier_temp', required=True, help='Chemin du dossier temporaire pour la sortie des fichiers')
    return parser.parse_args()


# Quelques traitements communs

args = parse_arguments()
config = configparser.ConfigParser()
config.read('../config/_quotidien.ini')

dossier_sortie = os.path.abspath(args.dossier_temp)
if not (os.path.exists(dossier_sortie) and os.path.isdir(dossier_sortie) and os.access(dossier_sortie, os.W_OK)):
    #TODO: log, meilleure sortie
    print("Impossible d'écrire dans " + dossier_sortie)
    sys.exit(1)

journee_actuelle = datetime.now().strftime("%Y-%m-%d")


# Sessions sur les ordinateurs publics
# Principe: on extrait, charge et transforme les données de la veille
# Les données sources sont également traitées quotidiennement par
# des scripts, alors il faut s'assurer de rouler celui-ci après
# celui sur les données sources.

# Extraction des données
journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
fichier_csv = f"{dossier_sortie}{os.path.sep}ordinateurs-{journee_extraction}.csv"
script = "../extraction/ordinateurs.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--date_debut", journee_extraction, "--date_fin", journee_extraction, "--fichier_sortie", fichier_csv], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

# Chargement des données
script = "../chargement/ordinateurs.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--fichier", fichier_csv], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

# Transformation des données
# TODO


# Clientèles
# Principe: on convertit les fichiers Synchro en CSV, on charge les
# données dans l'entrepôt, puis on effectue les transformations.
# À noter qu'il faut faire les deux premières étapes pour le
# personnel et les étudiants, mais la troisième étape (transformation)
# est jumelée

# Extraction des données

fichier_synchro = config['clienteles']['synchro_ac']
fichier_csv_ac = f"{dossier_sortie}{os.path.sep}etudiants.csv"
script = "../extraction/etudiants.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--fichier_entree", fichier_synchro, "--fichier_sortie", fichier_csv_ac], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

fichier_synchro = config['clienteles']['synchro_rh']
fichier_csv_rh = f"{dossier_sortie}{os.path.sep}personnel.csv"
script = "../extraction/personnel.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--fichier_entree", fichier_synchro, "--fichier_sortie", fichier_csv_rh], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

# Chargement des données

journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

script = "../chargement/etudiants.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--jour", journee_extraction, "--fichier", fichier_csv_ac], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

script = "../chargement/personnel.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script, "--jour", journee_extraction, "--fichier", fichier_csv_rh], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)

# Transformation des données

script = "../transformation/clienteles.py"
dossier_script = os.path.dirname(script)
resultat = subprocess.run(["python", script], cwd=dossier_script, capture_output=True, text=True)
#TODO: gestion des erreurs avec resultat.returncode et resultat.stdout (ou stderr)
