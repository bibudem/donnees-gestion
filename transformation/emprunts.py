import configparser
import logging
import psycopg2
import argparse
import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, prefixe_sha256, suffixe_sha256, cursor2csv, executer_requete_select
from courriel import envoyer_courriel

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations des données sur les emprunts dans l\'entrepôt')
    return parser.parse_args()

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/emprunts.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table temporaire des réservations
    logger.info("Début de la transformation des données sur les emprunts")

    # On supprime les lignes sans date
    requete = "DELETE FROM _tmp_emprunts WHERE date IS NULL OR date = '';"
    executer_requete(connexion, requete, logger)

# WHERE date IS NOT NULL; -- Ajout d'une condition pour ignorer les lignes où la date est NULL

    # Vérification et correction des noms d'institutions
    requete = """
        UPDATE _tmp_emprunts t
        SET
            bibliotheque_pret = COALESCE(
                (SELECT s.accepter FROM _synonymes s WHERE t.bibliotheque_pret = s.rejeter AND s.domaine = 'Emprunts'),
                t.bibliotheque_pret
            ),
            institution_doc = COALESCE(
                (SELECT s.accepter FROM _synonymes s WHERE t.institution_doc = s.rejeter AND s.domaine = 'Emprunts'),
                t.institution_doc
            ),
            institution_usager = COALESCE(
                (SELECT s.accepter FROM _synonymes s WHERE t.institution_usager = s.rejeter AND s.domaine = 'Emprunts'),
                t.institution_usager
            )
        WHERE
            EXISTS (
                SELECT 1 FROM _synonymes s 
                WHERE (t.bibliotheque_pret = s.rejeter OR t.institution_doc = s.rejeter OR t.institution_usager = s.rejeter) 
                AND s.domaine = 'Emprunts'
            )
    """
    executer_requete(connexion, requete, logger)

    # Remplacer le nom des bibliothèques des autres institutions par un seul terme 'Bibliothèque extérieure'
    requete = """
        UPDATE _tmp_emprunts
        SET bibliotheque_pret = 'Bibliothèque extérieure', bibliotheque_document = 'Bibliothèque extérieure'
        WHERE institution_doc NOT LIKE '%Université de Montréal%';
        """
    executer_requete(connexion, requete, logger)

    # Vérification si de nouveaux noms de bibliothèques non pris en charge ont été ajoutés
    requete = """
        SELECT DISTINCT succursale
        FROM (
            SELECT t.bibliotheque_pret AS succursale
            FROM _tmp_emprunts t
            WHERE t.bibliotheque_pret NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            AND t.bibliotheque_pret NOT IN (SELECT s.accepter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            UNION
            SELECT t.bibliotheque_document AS succursale
            FROM _tmp_emprunts t
            WHERE t.bibliotheque_document NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            AND t.bibliotheque_document NOT IN (SELECT s.accepter FROM _synonymes s WHERE s.domaine = 'Emprunts')
        ) AS merged_succursales
    """
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
    La vérification a identifié de nouvelles bibliothèques non prises en charge.

    Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.
    """
            envoyer_courriel("Entrepôt de données - Nouvelles bibliothèques pour les emprunts", intro + res, logger)
            sys.exit(1)

    # Vérification et correction des noms de bibliothèques

    requete = """
        UPDATE _tmp_emprunts t
        SET
            bibliotheque_pret = COALESCE(
                (SELECT s.accepter FROM _synonymes s WHERE t.bibliotheque_pret = s.rejeter AND s.domaine = 'Emprunts'),
                t.bibliotheque_pret
            ),
            bibliotheque_document = COALESCE(
                (SELECT s.accepter FROM _synonymes s WHERE t.bibliotheque_document = s.rejeter AND s.domaine = 'Emprunts'),
                t.bibliotheque_document
            )
        WHERE
            EXISTS (
                SELECT 1 FROM _synonymes s 
                WHERE (t.bibliotheque_pret = s.rejeter OR t.bibliotheque_document = s.rejeter) 
                AND s.domaine = 'Emprunts'
            )
    """
    executer_requete(connexion, requete, logger)


    ### EST-CE QU'ON AJOUTE LES DONNEES DISCIPLINE?

    # Vérification des dates -- Envoi d'un courriel si certaines dates ne correspondent pas au format YYYY-mm-dd HH:MM:SS malgré les scripts d'extraction et de chargement
    requete = """
        SELECT *
        FROM _tmp_emprunts
        WHERE NOT (date ~ '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$')
    """
    with connexion.cursor() as cursor:
        cursor.execute(requete)
        resultats = cursor.fetchall()
        if resultats:
                noms_colonnes = [desc[0] for desc in cursor.description]
                res = cursor2csv(resultats, noms_colonnes)
                intro = """
                    La vérification a identifié des dates incorrectes. 

                    Vous devez vérifier les données suivantes :
                """
                envoyer_courriel("Entrepôt de données - Dates incorrectes pour les emprunts", intro + res, logger)

    # Copie des données temporaires dans la table finale
    requete = f"""
        INSERT INTO emprunts
        (cb_document, cote, bibliotheque_pret, cb_usager, date, bibliotheque_document, institution_doc, institution_usager)
        SELECT
            cb_document,
            cote,
            bibliotheque_pret,
            sha256(concat('{prefixe}', cb_usager, '{suffixe}')::bytea)::varchar,
            TO_TIMESTAMP(date, 'YYYY-MM-DD HH24:MI:SS'), -- Conversion de la colonne date VARCHAR en TIMESTAMP
            bibliotheque_document,
            institution_doc,
            institution_usager
        FROM _tmp_emprunts
        ON CONFLICT DO NOTHING

    """
    executer_requete(connexion, requete, logger)

    # Suppression des données temporaires
    requete = "DELETE FROM _tmp_emprunts"
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
