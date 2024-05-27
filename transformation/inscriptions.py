import configparser
import logging
import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, prefixe_sha256, suffixe_sha256, cursor2csv, executer_requete_select
from courriel import envoyer_courriel

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur les inscriptions')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/inscriptions.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table temporaire des réservations
    logger.info(f"Début de la transformation des données de sessions sur les inscriptions")


    # On copie les informations de _tmp_evenements vers evenements 
    requete = """
        INSERT INTO inscriptions (id, courriel, evenement_id)
        SELECT e.booking_id, e.email, e.event_id
        FROM _tmp_inscriptions e
        ON CONFLICT (id, evenement_id)
        DO UPDATE SET
            courriel = EXCLUDED.courriel,
            usager = NULL;
    """
    executer_requete(connexion, requete, logger)

    # On copie proprietaire dans presentateur si ce dernier est 'null'
    requete = f"""
        UPDATE inscriptions i
        SET usager = c.usager, discipline = c.discipline
        FROM _clientele_cumul c
        WHERE i.courriel = c.courriel
        AND i.usager = NULL;
    """
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
