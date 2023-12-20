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
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur les réservations l\'entrepôt')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/reservations.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table temporaire des réservations
    nom_table = "_tmp_reservations"
    logger.info(f"Début de la transformation des données de réservation de salles")

    # On supprime les statuts non conservés
    requete = f"DELETE FROM {nom_table} t WHERE EXISTS (SELECT 1 FROM _verif_statuts_reservations v WHERE v.nom = t.status AND NOT(v.conserver))"
    executer_requete(connexion, requete, logger)

    # On supprime les tests et les formations
    requete = f"DELETE FROM {nom_table} t WHERE t.location_name = 'Bibliothèque de test' OR t.location_name = 'Bibliothèque de formation'"
    executer_requete(connexion, requete, logger)

    # On vérifie si on a de nouveaux statuts

    requete = f"SELECT status FROM {nom_table} WHERE status NOT IN (SELECT nom FROM _verif_statuts_reservations) GROUP BY status"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des statuts de réservation de salles a identifié de nouveaux statuts non pris en charge.

Vous devez vérifier la table _verif_statuts_reservations pour ajouter les noms ci-dessous.


"""
            envoyer_courriel("Entrepôt de données - Nouveaux statuts de réservation", intro + res, logger)
            sys.exit(1)

    # On vérifie si on a les bons noms de bibliothèque
    requete = f"SELECT t.location_name FROM {nom_table} t WHERE t.location_name NOT IN (SELECT s.accepter FROM _synonymes s WHERE s.domaine = 'Réservations') GROUP BY t.location_name"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des bibliothèques de réservation de salles a identifié de nouvelles bibliothèques non prises en charge.

Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.


"""
            envoyer_courriel("Entrepôt de données - Nouvelles bibliothèques pour la réservation", intro + res, logger)
            sys.exit(1)

    # On corrige les noms de bibliothèque
    requete = f"""
        UPDATE {nom_table} t
        SET location_name = s.accepter
        FROM _synonymes s
        WHERE t.location_name = s.rejeter
        AND s.domaine = 'Réservations'
    """
    executer_requete(connexion, requete, logger)

    # On ajoute les données de login et de discipline si on les a
    requete = f"""
        UPDATE {nom_table} t
        SET usager = c.usager, discipline = c.discipline
        FROM _clientele_cumul c
        WHERE sha256(concat('{prefixe}', t.email, '{suffixe}')::bytea)::varchar = c.courriel
    """
    executer_requete(connexion, requete, logger)

    # Si on ne les a pas, on insère une discipline inconnue
    requete = f"UPDATE {nom_table} SET discipline = 'Inconnue' WHERE usager IS NULL"
    executer_requete(connexion, requete, logger)

    # On va copier les données temporaires dans la table finale
    requete = f"""
        INSERT INTO reservations
        (usager, courriel, discipline, journee, bibliotheque, categorie, salle)
        SELECT
            usager,
            sha256(concat('{prefixe}', email, '{suffixe}')::bytea)::varchar,
            discipline,
            fromDate,
            location_name,
            category_name,
            item_name
        FROM _tmp_reservations
    """
    executer_requete(connexion, requete, logger)

    # On va inscrire la donnée de session depuis la date
    requete = """
        UPDATE reservations
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
    requete = f"DELETE FROM {nom_table}"
    executer_requete(connexion, requete, logger)

finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
