# Script qui lance l'extraction, le chargement et la transformation
# des données quotidiennes dans l'entrepôt de données
from datetime import datetime, timedelta
import argparse
import os
import sys
import subprocess
import configparser
import logging
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs


# Les fonctions utilisées dans le script
def parse_arguments():
    parser = argparse.ArgumentParser(description="Script qui pilote l'extraction, le chargement et la transformation des données quotidiennes dans l\'entrepôt")
    parser.add_argument('--dossier_temp', required=True, help='Chemin du dossier temporaire pour la sortie des fichiers')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("utils/pilote_quotidien.py")
logger.info("Début de l'exécution du script quotidien")

# Les arguments et le fichier de config
args = parse_arguments()
config = configparser.ConfigParser()
config.read('_config.ini')

# On s'assure qu'on peut écrire dans le dossier de sortie, sinon on quitte
dossier_sortie = os.path.abspath(args.dossier_temp)
if not (os.path.exists(dossier_sortie) and os.path.isdir(dossier_sortie) and os.access(dossier_sortie, os.W_OK)):
    logger.error("Impossible d'écrire dans le dossier " + dossier_sortie)
    sys.exit(1)

journee_actuelle = datetime.now().strftime("%Y-%m-%d")

##########################
# Réservations de salles #
##########################
# Principe: on extrait, charge et transforme les données de la veille

# Extraction des données
journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
fichier_csv = f"{dossier_sortie}{os.path.sep}reservations-{journee_extraction}.csv"
script = "extraction/salles-reservations.py"
resultat = subprocess.run(["python", script, "--date_debut", journee_extraction, "--date_fin", journee_extraction, "--fichier_sortie", fichier_csv], capture_output=True, text=True)
if (resultat.returncode > 0):
    # On traite l'erreur mais on ne poursuit pas ce traitement
    logger.error("Erreur dans l'extraction des données des réservations de salles : " + resultat.stderr)
else:
    # On poursuit avec le chargement
    script = "chargement/reservations.py"
    resultat = subprocess.run(["python", script, "--fichier", fichier_csv], capture_output=True, text=True)
    if (resultat.returncode > 0):
        # On traite l'erreur mais on ne poursuit pas ce traitement
        logger.error("Erreur dans le chargement des données des réservations de salles : " + resultat.stderr)
    else:
        # On poursuit avec la transformation
        script = "transformation/reservations.py"
        resultat = subprocess.run(["python", script], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur
            logger.error("Erreur dans la transformation des données des réservations de salle : " + resultat.stderr)


##############################
# Événements et inscriptions #
##############################
# Principe: on extrait, charge et transforme les données de la veille

# Extraction des données
journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
fichier_csv = f"{dossier_sortie}{os.path.sep}evenements-{journee_extraction}"
script = "extraction/evenements_inscriptions.py"
resultat = subprocess.run(["python", script, "--date_debut", journee_extraction, "--date_fin", journee_extraction, "--fichier_sortie", fichier_csv], capture_output=True, text=True)
if (resultat.returncode > 0):
    # On traite l'erreur mais on ne poursuit pas ce traitement
    logger.error("Erreur dans l'extraction des données des événements et inscriptions : " + resultat.stderr)
else:
    # On poursuit avec les événements
    script = "chargement/evenements.py"
    resultat = subprocess.run(["python", script, "--fichier", fichier_csv + "_evenements.csv"], capture_output=True, text=True)
    if (resultat.returncode > 0):
        # On traite l'erreur mais on ne poursuit pas ce traitement
        logger.error("Erreur dans le chargement des données des événements : " + resultat.stderr)
    else:
        # On poursuit avec la transformation
        script = "transformation/evenements.py"
        resultat = subprocess.run(["python", script], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur
            logger.error("Erreur dans la transformation des données des événements : " + resultat.stderr)

    # On poursuit avec les inscriptions
    script = "chargement/inscriptions.py"
    resultat = subprocess.run(["python", script, "--fichier", fichier_csv + "_inscriptions.csv"], capture_output=True, text=True)
    if (resultat.returncode > 0):
        # On traite l'erreur mais on ne poursuit pas ce traitement
        logger.error("Erreur dans le chargement des données des inscriptions : " + resultat.stderr)
    else:
        # On poursuit avec la transformation
        script = "transformation/inscriptions.py"
        resultat = subprocess.run(["python", script], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur
            logger.error("Erreur dans la transformation des inscriptions : " + resultat.stderr)

###############################
# Fréquentation et occupation #
###############################
# Principe: on extrait, charge et transforme les données de la veille

# Extraction des données
journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
fichier_csv = f"{dossier_sortie}{os.path.sep}comptepersonnes-{journee_extraction}"
script = "extraction/frequentation.py"
resultat = subprocess.run(["python", script, "--date_debut", journee_extraction, "--date_fin", journee_extraction, "--fichier_sortie", fichier_csv], capture_output=True, text=True)
if (resultat.returncode > 0):
    # On traite l'erreur mais on ne poursuit pas ce traitement
    logger.error("Erreur dans l'extraction des données de fréquentation : " + resultat.stderr)
else:
    # On poursuit avec le chargement
    script = "chargement/frequentation.py"
    resultat = subprocess.run(["python", script, "--fichier_freq", fichier_csv + "_frequentation.csv", "--fichier_occ", fichier_csv + "_occupation.csv"], capture_output=True, text=True)
    if (resultat.returncode > 0):
        # On traite l'erreur mais on ne poursuit pas ce traitement
        logger.error("Erreur dans le chargement des données de fréquentation : " + resultat.stderr)
    else:
        # On poursuit avec la transformation
        script = "transformation/frequentation_occupation.py"
        resultat = subprocess.run(["python", script], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur
            logger.error("Erreur dans la transformation des données de fréquentation : " + resultat.stderr)


########################################
# Sessions sur les ordinateurs publics #
########################################
# Principe: on extrait, charge et transforme les données de la veille

# Extraction des données
journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
fichier_csv = f"{dossier_sortie}{os.path.sep}ordinateurs-{journee_extraction}.csv"
script = "extraction/ordinateurs.py"
resultat = subprocess.run(["python", script, "--date_debut", journee_extraction, "--date_fin", journee_extraction, "--fichier_sortie", fichier_csv], capture_output=True, text=True)
if (resultat.returncode > 0):
    # On traite l'erreur mais on ne poursuit pas ce traitement
    logger.error("Erreur dans l'extraction des données des ordinateurs publics : " + resultat.stderr)
else:
    # On poursuit avec le chargement
    script = "chargement/ordinateurs.py"
    resultat = subprocess.run(["python", script, "--fichier", fichier_csv], capture_output=True, text=True)
    if (resultat.returncode > 0):
        # On traite l'erreur mais on ne poursuit pas ce traitement
        logger.error("Erreur dans le chargement des données des ordinateurs publics : " + resultat.stderr)
    else:
        # On poursuit avec la transformation
        script = "transformation/ordinateurs.py"
        resultat = subprocess.run(["python", script], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur
            logger.error("Erreur dans la transformation des données des ordinateurs publics : " + resultat.stderr)

# TODO:
#   - emprunts
#   - statistiques de référence
#   - étudiants, personnel, clientèles
