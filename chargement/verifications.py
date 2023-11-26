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
    parser = argparse.ArgumentParser(description='Script pour charger les données de vérification dans l\'entrepôt')
    parser.add_argument('--fichier_cycles', required=True, help='Chemin du fichier CSV des cycles à charger')
    parser.add_argument('--fichier_statuts_reservations', required=True, help='Chemin du fichier CSV des status de réservation à charger')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("chargement/verifications.py")

# Les arguments en ligne de commande
args = parse_arguments()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Les cycles
    chemin_fichier_csv = args.fichier_cycles

    # Nom de la table dans la base de données
    # Elle sera vidée avant le chargement
    nom_table = "_verif_cycles"

    logger.info(f"Début du chargement des cycles depuis le fichier {chemin_fichier_csv}")

    # On commence par supprimer toute donnée dans la table
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete, logger)

    # Ensuite on charge les données
    requete = f"""
        COPY {nom_table}
        (cycle)
        FROM '{chemin_fichier_csv}'
        DELIMITER ';'
        CSV HEADER;
    """
    executer_requete(connexion, requete, logger)

    # Les status de réservation
    chemin_fichier_csv = args.fichier_statuts_reservations

    # Nom de la table dans la base de données
    # Elle sera vidée avant le chargement
    nom_table = "_verif_statuts_reservations"

    logger.info(f"Début du chargement des statuts de réservation depuis le fichier {chemin_fichier_csv}")

    # On commence par supprimer toute donnée dans la table
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete, logger)

    # Ensuite on charge les données
    requete = f"""
        COPY {nom_table}
        (nom, conserver)
        FROM '{chemin_fichier_csv}'
        DELIMITER ','
        CSV HEADER;
    """
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
