import configparser
import logging
import psycopg2
from datetime import datetime, timedelta
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour charger les données des emprunts de documents WMS dans l\'entrepôt')
    parser.add_argument('--jour', default=None, help='Date (aaaa-mm-jj) qui correspond aux données chargées')
    parser.add_argument('--fichier', required=True, help='Chemin du fichier CSV des emprunts à charger')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("chargement/emprunts.py")

# Les arguments en ligne de commande
args = parse_arguments()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Chemin du fichier CSV à charger
    chemin_fichier_csv = args.fichier

    # Nom de la table dans la base de données
    nom_table = "_tmp_emprunts"

    logger.info(f"Début du chargement des emprunts depuis le fichier {chemin_fichier_csv}")

    # On commence par supprimer toute donnée dans la table temporaire
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete, logger)

    # Ensuite on charge les données
    # Fichier 1 mai 2023 au 28 février 2024
    #cb_document	Cote du document	institution_pret	bibliotheque_pret	cb_usager	date	bibliotheque_document	institution_doc	institution_usager
    requete = f"""
        COPY {nom_table}
        (cb_document, cote, institution_pret, bibliotheque_pret, cb_usager, date, bibliotheque_document, institution_doc, institution_usager)
        FROM '{chemin_fichier_csv}'
        DELIMITER '\t'
        CSV HEADER;
    """
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
