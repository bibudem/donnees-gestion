#import configparser
import logging
#import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, cursor2csv, executer_requete_select
from courriel import envoyer_courriel

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur la fréquentation et l\'occupation dans l\'entrepôt')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/frequentation_occupation.py")

# Les arguments en ligne de commande
args = parse_arguments()

try:

    logger.info(f"Début de la transformation des données de fréquentation et d'occupation")
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # On vérifie si on a les bons noms de bibliothèque
    requete = "SELECT t.secteur FROM _tmp_frequentation t WHERE t.secteur NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Fréquentation') GROUP BY t.secteur"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des bibliothèques pour la fréquentation a identifié de nouvelles bibliothèques non prises en charge.

Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.


"""
            envoyer_courriel("Entrepôt de données - Nouvelles bibliothèques pour la fréquentation", intro + res, logger)
            sys.exit(1)

    requete = "SELECT t.secteur FROM _tmp_occupation t WHERE t.secteur NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Fréquentation') GROUP BY t.secteur"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des bibliothèques pour l'occupation a identifié de nouvelles bibliothèques non prises en charge.

Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.


"""
            envoyer_courriel("Entrepôt de données - Nouvelles bibliothèques pour l'occupation", intro + res, logger)
            sys.exit(1)

    # On va copier les données temporaires dans la table finale
    requete = """
        INSERT INTO frequentation
        (journee, bibliotheque, frequentation)
        SELECT DATE_TRUNC('day', date) as jour, secteur, SUM(entrees)
        FROM _tmp_frequentation GROUP BY jour, secteur
        ON CONFLICT (journee, bibliotheque)
        DO UPDATE SET
            frequentation = EXCLUDED.frequentation,
            occupation = NULL,
            session = NULL;
    """
    executer_requete(connexion, requete, logger)
    requete = """
        UPDATE frequentation SET occupation =
            (SELECT SUM(t.occupation)/4 FROM _tmp_occupation t
            WHERE t.secteur = bibliotheque 
            AND DATE_TRUNC('day', t.date) = journee)
        WHERE occupation IS NULL;
    """
    executer_requete(connexion, requete, logger)

    # On corrige les noms de bibliothèque
    requete = """
        UPDATE frequentation
        SET bibliotheque = s.accepter
        FROM _synonymes s
        WHERE bibliotheque = s.rejeter
        AND s.domaine = 'Fréquentation'
    """
    executer_requete(connexion, requete, logger)

    # On va inscrire la donnée de session depuis la date
    requete = """
        UPDATE frequentation
        SET session = 
            CASE
                WHEN EXTRACT(MONTH FROM journee) >= 9 THEN MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 9, 1)
                WHEN EXTRACT(MONTH FROM journee) >= 5 THEN MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 5, 1)
                ELSE MAKE_DATE(EXTRACT(YEAR FROM journee)::INTEGER, 1, 1)
            END
        WHERE session IS NULL;
    """
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
