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
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur les événements')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/evenements.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table temporaire des réservations
    logger.info(f"Début de la transformation des données de sessions sur les événements")

    # On supprime les événements de Tests
    requete = "DELETE FROM _tmp_evenements WHERE presenter ILIKE '%Test%'"
    executer_requete(connexion, requete, logger)


    # On copie les informations de _tmp_evenements vers evenements 
    requete = """
        INSERT INTO evenements (id, titre, journee, proprietaire, presentateur, nb_participants)
        SELECT e.id, e.title, CAST(e.start AS DATE), e.owner, e.presenter, COUNT(i.email) AS nb_participants
        FROM _tmp_evenements e
        LEFT JOIN _tmp_inscriptions i ON e.id = i.event_id
        GROUP BY e.id;
    """
    executer_requete(connexion, requete, logger)

    # On copie proprietaire dans presentateur si ce dernier est 'null'
    requete = f"""
        UPDATE evenements
        SET presentateur = proprietaire
        WHERE presentateur is null;
    """
    executer_requete(connexion, requete, logger)

    # On va inscrire la donnée de session depuis la date
    requete = """
        UPDATE evenements
        SET session = 
            CASE
                WHEN EXTRACT(MONTH FROM journee) >= 9 THEN MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 9, 1)
                WHEN EXTRACT(MONTH FROM journee) >= 5 THEN MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 5, 1)
                ELSE MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 1, 1)
            END
        WHERE session IS NULL;
    """
    executer_requete(connexion, requete, logger)

    # On supprime les données temporaires
    requete = "DELETE FROM _tmp_evenements"
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
