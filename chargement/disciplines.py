import configparser
import logging
import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour charger les données de disciplines dans l\'entrepôt')
    parser.add_argument('--fichier', required=True, help='Chemin du fichier CSV à charger')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("chargement/disciplines.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Chemin du fichier CSV à charger
chemin_fichier_csv = args.fichier

# Nom de la table dans la base de données
# Elle sera vidée avant le chargement
nom_table = "disciplines"

logger.info(f"Début du chargement des disciplines depuis le fichier {chemin_fichier_csv}")

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # On commence par supprimer toute donnée dans la table
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete, logger)

    # Ensuite on charge les données
    requete = f"""
        COPY {nom_table}
        (discipline, bibliothecaire, bibliotheque, secteur)
        FROM '{chemin_fichier_csv}'
        DELIMITER ';'
        CSV HEADER;
    """
    executer_requete(connexion, requete, logger)

    # Ajout du secteur de la bibliothèque
    requete = f"""
        UPDATE {nom_table}
        SET secteur = CASE
            WHEN bibliotheque = 'Aménagement' THEN 'TGDAMLD'
            WHEN bibliotheque = 'Droit' THEN 'TGDAMLD'
            WHEN bibliotheque = 'Thèrèse-Gouin-Décarie' THEN 'TGDAMLD'
            WHEN bibliotheque = 'Campus de Laval' THEN 'TGDAMLD'
            WHEN bibliotheque = 'Musique' THEN 'TGDAMLD'
            WHEN bibliotheque = 'Mathématiques et informatique' THEN 'Santé Sciences'
            WHEN bibliotheque = 'Marguerite-d’Youville' THEN 'Santé Sciences'
            WHEN bibliotheque = 'Médecine vétérinaire' THEN 'Santé Sciences'
            WHEN bibliotheque = 'Santé' THEN 'Santé Sciences'
            WHEN bibliotheque = 'Sciences' THEN 'Santé Sciences'
            WHEN bibliotheque = 'Lettres et sciences humaines' THEN 'Lettres et sciences humaines'
           ELSE 'Non déterminé'
        END;
"""

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
