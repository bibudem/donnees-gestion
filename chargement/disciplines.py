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
        INSERT INTO {nom_table}
        (discipline, bibliothecaire, bibliotheque)
        VALUES %s
    """
    copy_from_csv(connexion, requete, chemin_fichier_csv, logger)

    # Ajout du secteur de la bibliothèque
    requete = f"""
        UPDATE {nom_table} D
            SET secteur = (
                SELECT secteur
                FROM secteurs S
                WHERE D.bibliotheque = S.bibliotheque
                );
"""
    executer_requete(connexion, requete, logger)

    # Ajout du facteur de calcul pour chaque paire de bibliothécaire et de discipline
    requete = f"""
            UPDATE disciplines 
                SET facteur = 1.0/subquery.count
                    FROM (
                        SELECT discipline, COUNT(*) as count
                        FROM disciplines
                        GROUP BY discipline
                         ) AS subquery
                    WHERE disciplines.discipline = subquery.discipline;
"""
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
    