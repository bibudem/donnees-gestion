import configparser
import logging
import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, copy_from_csv

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour charger des synomymes dans l\'entrepôt')
    parser.add_argument('--fichier', required=True, help='Chemin du fichier CSV de synonymes à charger')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("chargement/synonymes.py")

# Les arguments en ligne de commande
args = parse_arguments()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Les cycles
    chemin_fichier_csv = args.fichier

    # Nom de la table dans la base de données
    # Elle sera vidée avant le chargement
    nom_table = "_synonymes"

    logger.info(f"Début du chargement des synonymes depuis le fichier {chemin_fichier_csv}")

    # On commence par supprimer toute donnée dans la table
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete, logger)

    # Ensuite on charge les données
    requete = f"""
        INSERT INTO {nom_table}
        (domaine, rejeter, accepter)
        VALUES %s
    """
    copy_from_csv(connexion, requete, chemin_fichier_csv, logger, "\t")

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
