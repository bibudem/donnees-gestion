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

####################
# Fichiers Synchro #
####################

# On vérifie d'abord si on a les fichiers Synchro de la veille
chemin_fichier_synchro_ac = config['quotidien']['synchro_ac']
chemin_fichier_synchro_rh = config['quotidien']['synchro_rh']

# On va s'assurer qu'on a les fichiers
if (os.path.isfile(chemin_fichier_synchro_ac) and os.path.isfile(chemin_fichier_synchro_rh)):
    # On a les fichiers, on vérifie leur date
    jour_synchro_ac = datetime.fromtimestamp(os.path.getmtime(chemin_fichier_synchro_ac)).date().strftime("%Y-%m-%d")
    jour_synchro_rh = datetime.fromtimestamp(os.path.getmtime(chemin_fichier_synchro_rh)).date().strftime("%Y-%m-%d")
    if (jour_synchro_rh == jour_synchro_ac and jour_synchro_ac == journee_actuelle):
        # On a les bonnes dates de fichier, on peut procéder
        # Extraction des fichiers, d'abord les étudiants
        journee_extraction = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        fichier_csv_ac = f"{dossier_sortie}{os.path.sep}synchro_ac-{journee_extraction}.csv"
        script = "extraction/etudiants.py"
        resultat = subprocess.run(["python", script, "--fichier_entree", chemin_fichier_synchro_ac, "--fichier_sortie", fichier_csv_ac], capture_output=True, text=True)
        if (resultat.returncode > 0):
            # On traite l'erreur mais on ne poursuit pas ce traitement
            logger.error("Erreur dans l'extraction des données de Synchro académique : " + resultat.stderr)
        else:
            # On poursuit avec le fichier du personnel
            fichier_csv_rh = f"{dossier_sortie}{os.path.sep}synchro_rh-{journee_extraction}.csv"
            script = "extraction/personnel.py"
            resultat = subprocess.run(["python", script, "--fichier_entree", chemin_fichier_synchro_rh, "--fichier_sortie", fichier_csv_rh], capture_output=True, text=True)
            if (resultat.returncode > 0):
                # On traite l'erreur mais on ne poursuit pas ce traitement
                logger.error("Erreur dans l'extraction des données de Synchro RH : " + resultat.stderr)
            else:
                # On poursuit avec le chargement des étudiants
                journee_chargement = (datetime.strptime(journee_actuelle, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                script = "chargement/etudiants.py"
                resultat = subprocess.run(["python", script, "--jour", journee_chargement, "--fichier", fichier_csv_ac], capture_output=True, text=True)
                if (resultat.returncode > 0):
                    # On traite l'erreur mais on ne poursuit pas ce traitement
                    logger.error("Erreur dans le chargement des données de Synchro académique : " + resultat.stderr)
                else:
                    # On poursuit avec le chargement du personnel
                    script = "chargement/personnel.py"
                    resultat = subprocess.run(["python", script, "--jour", journee_chargement, "--fichier", fichier_csv_rh], capture_output=True, text=True)
                    if (resultat.returncode > 0):
                        # On traite l'erreur mais on ne poursuit pas ce traitement
                        logger.error("Erreur dans le chargement des données de Synchro RH : " + resultat.stderr)
                    else:
                        # On poursuit avec la transformation, étudiants et personnel
                        script = "transformation/clienteles.py"
                        resultat = subprocess.run(["python", script], capture_output=True, text=True)
                        if (resultat.returncode > 0):
                            # On traite l'erreur mais on ne poursuit pas ce traitement
                            logger.error("Erreur dans la transformation des données de clientèles : " + resultat.stderr)
    else:
        logger.error("Erreur de date dans les fichiers Synchro: " + jour_synchro_ac + ", " + jour_synchro_rh + " mais " + journee_actuelle + " attendu")
else:
    # Les fichiers Synchro ne sont pas accessibles 
    logger.error("Impossible de lire les fichiers Synchro " + chemin_fichier_synchro_rh + " ou " + chemin_fichier_synchro_ac)

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
