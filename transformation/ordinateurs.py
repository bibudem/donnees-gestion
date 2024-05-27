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
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur l\'utilisation des postes publics dans l\'entrepôt')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/ordinateurs.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table temporaire des réservations
    logger.info(f"Début de la transformation des données de sessions sur les ordinateurs publics")

    # On supprime le secteur de Tests
    requete = "DELETE FROM _tmp_sessions_ordinateurs WHERE Secteur LIKE '%Tests%'"
    executer_requete(connexion, requete, logger)

    # On vérifie si on a les bons noms de bibliothèque
    requete = "SELECT t.Secteur FROM _tmp_sessions_ordinateurs t WHERE t.Secteur NOT IN (SELECT s.rejeter FROM _synonymes s WHERE s.domaine = 'Ordinateurs') GROUP BY t.Secteur"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des bibliothèques d'utilisation des postes publics a identifié de nouvelles bibliothèques non prises en charge.

Vous devez vérifier la table _synonymes pour ajouter les noms ci-dessous.


"""
            envoyer_courriel("Entrepôt de données - Nouvelles bibliothèques pour les ordinateurs publics", intro + res, logger)
            sys.exit(1)

    # On va copier les données temporaires dans la table finale
    requete = f"""
        INSERT INTO sessions_ordinateurs
        (usager, journee, dateheure, lieu, ordinateur, duree)
        SELECT
            sha256(concat('{prefixe}', utilisateur_login, '{suffixe}')::bytea)::varchar,
            Date(Ouverture_session),
            Ouverture_session,
            Secteur,
            Station,
            Duree
        FROM _tmp_sessions_ordinateurs
        ON CONFLICT (usager, ordinateur, dateheure)
        DO UPDATE SET
            journee = EXCLUDED.journee,
            lieu = EXCLUDED.lieu,
            duree = EXCLUDED.duree,
            session = NULL,
            courriel = NULL,
            discipline = NULL,
            bibliotheque = NULL
    """
    executer_requete(connexion, requete, logger)

    # On corrige les noms de bibliothèque
    requete = """
        UPDATE sessions_ordinateurs t
        SET lieu = s.accepter
        FROM _synonymes s
        WHERE t.lieu = s.rejeter
        AND s.domaine = 'Ordinateurs'
    """
    executer_requete(connexion, requete, logger)

    # On ajoute les données de courriel, discipline et biblitoheque si on les a
    requete = f"""
        UPDATE sessions_ordinateurs t
        SET courriel = c.courriel, discipline = c.discipline, bibliotheque = c.bibliotheque
        FROM _clientele_cumul c
        WHERE t.usager = c.usager
        AND (t.courriel IS NULL OR t.discipline IS NULL OR t.bibliotheque IS NULL)
    """
    executer_requete(connexion, requete, logger)

    # Si on ne les a pas, on insère une discipline inconnue
    requete = "UPDATE sessions_ordinateurs SET discipline = 'Inconnue' WHERE discipline IS NULL"
    executer_requete(connexion, requete, logger)

    # On va inscrire la donnée de session depuis la date
    requete = """
        UPDATE sessions_ordinateurs
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
