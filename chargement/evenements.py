import configparser
import logging
import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("../commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour charger les événements de formation dans l\'entrepôt')
    parser.add_argument('--fichier', required=True, help='Chemin du fichier CSV à charger')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("chargement/evenements.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Chemin du fichier CSV à charger
chemin_fichier_csv = args.fichier

# Nom de la table dans la base de données
nom_table = "_tmp_evenements"

logger.info(f"Début du chargement des inscriptions depuis le fichier {chemin_fichier_csv}")

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Ensuite on charge les données
    # id,title,start,end,owner.name,presenter
    requete = f"""
        COPY {nom_table}
        (id, title, start, finish, owner, presenter)
        FROM '{chemin_fichier_csv}'
        DELIMITER ','
        CSV HEADER;
    """
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
