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
    logger.info(f"Début de la transformation des données sur les emprunts")

    # On vérifie si on a les bons noms d'institutions
    requete = """
        SELECT DISTINCT institution
        FROM (
            SELECT t.institution_pret AS institution
            FROM _tmp_emprunts t
            WHERE t.institution_pret NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            UNION
            SELECT t.institution_doc AS institution
            FROM _tmp_emprunts t
            WHERE t.institution_doc NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            UNION
            SELECT t.institution_usager AS institution
            FROM _tmp_emprunts t
            WHERE t.institution_usager NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
        ) AS merged_institutions
    """
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
    La vérification a identifié de nouvelles institutions non prises en charge.

    Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.
    """
            envoyer_courriel("Entrepôt de données - Nouvelles institutions pour les emprunts", intro + res, logger)
            sys.exit(1)

    # On corrige les noms de institutions
    requete = f"""
        UPDATE _tmp_emprunts t
        SET
            institution_pret = CASE
                                   WHEN EXISTS (SELECT 1 FROM _synonymes s WHERE t.institution_pret = s.rejeter AND s.domaine = 'Emprunts')
                                   THEN (SELECT s.accepter FROM _synonymes s WHERE t.institution_pret = s.rejeter AND s.domaine = 'Emprunts')
                                   ELSE t.institution_pret
                                END,
            institution_doc = CASE
                                  WHEN EXISTS (SELECT 1 FROM _synonymes s WHERE t.institution_doc = s.rejeter AND s.domaine = 'Emprunts')
                                  THEN (SELECT s.accepter FROM _synonymes s WHERE t.institution_doc = s.rejeter AND s.domaine = 'Emprunts')
                                  ELSE t.institution_doc
                              END,
            institution_usager = CASE
                                     WHEN EXISTS (SELECT 1 FROM _synonymes s WHERE t.institution_usager = s.rejeter AND s.domaine = 'Emprunts')
                                     THEN (SELECT s.accepter FROM _synonymes s WHERE t.institution_usager = s.rejeter AND s.domaine = 'Emprunts')
                                     ELSE t.institution_usager
                                 END
        WHERE
            EXISTS (SELECT 1 FROM _synonymes s WHERE (t.institution_pret = s.rejeter OR t.institution_doc = s.rejeter OR t.institution_usager = s.rejeter) AND s.domaine = 'Emprunts')
    """
    executer_requete(connexion, requete, logger)

    # REMPLACER NOMS DE BIBLIOTHEQUE DES AUTRES INSITUTIONS PAR UN SEUL TERME 'Bibliothèque Extérieure'
    requete = f"""
        UPDATE _tmp_emprunts
        SET bibliotheque_pret = 'Bibliothèque Extérieure', bibliotheque_document = 'Bibliothèque Extérieure'
        WHERE institution_doc != 'Université de Montréal' OR institution_pret != 'Université de Montréal';
        """
    executer_requete(connexion, requete, logger)

    # VERIFICATION DE NOMS DE BIBLIOTHEQUES
    requete = f"""
        SELECT DISTINCT succursale
        FROM (
            SELECT t.bibliotheque_pret AS succursale
            FROM _tmp_emprunts t
            WHERE t.bibliotheque_pret NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
            UNION
            SELECT t.bibliotheque_document AS succursale
            FROM _tmp_emprunts t
            WHERE t.bibliotheque_document NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Emprunts')
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


    # REMPLACER LES TERMES REJETÉS PAR LES TERMES ACCEPTÉS

    requete = f"""
        UPDATE _tmp_emprunts t
        SET
            bibliotheque_pret = CASE
                                   WHEN EXISTS (SELECT 1 FROM _synonymes s WHERE t.bibliotheque_pret = s.rejeter AND s.domaine = 'Emprunts')
                                   THEN (SELECT s.accepter FROM _synonymes s WHERE t.bibliotheque_pret = s.rejeter AND s.domaine = 'Emprunts')
                                   ELSE t.bibliotheque_pret
                                END,
            bibliotheque_document = CASE
                                  WHEN EXISTS (SELECT 1 FROM _synonymes s WHERE t.bibliotheque_document = s.rejeter AND s.domaine = 'Emprunts')
                                  THEN (SELECT s.accepter FROM _synonymes s WHERE t.bibliotheque_document = s.rejeter AND s.domaine = 'Emprunts')
                                  ELSE t.bibliotheque_document
                              END
        WHERE
            EXISTS (SELECT 1 FROM _synonymes s WHERE (t.bibliotheque_pret = s.rejeter OR t.bibliotheque_document = s.rejeter) AND s.domaine = 'Emprunts')
    """
    executer_requete(connexion, requete, logger)


    ### EST-CE QU'ON AJOUTE LES DONNEES DISCIPLINE?


    # On va copier les données temporaires dans la table finale
    requete = f"""
        INSERT INTO emprunts
        (cb_document, cote, institution_pret, bibliotheque_pret, cb_usager, date, bibliotheque_document, institution_doc, institution_usager)
        SELECT
            cb_document,
            cote,
            institution_pret,
            bibliotheque_pret,
            sha256(concat('{prefixe}', cb_usager, '{suffixe}')::bytea)::varchar,
            TO_DATE(date, 'YYYY-MM-DD'), -- Conversion de la colonne date VARCHAR en DATE
            bibliotheque_document,
            institution_doc,
            institution_usager
        FROM _tmp_emprunts
        ON CONFLICT DO NOTHING
    """
    executer_requete(connexion, requete, logger)

    # On supprime les données temporaires
    #requete = "DELETE FROM _tmp_emprunts"
    #executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
