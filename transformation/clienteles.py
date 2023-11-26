import configparser
import logging
import psycopg2
import argparse
import sys
import os
sys.path.append(os.path.abspath("../commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete, prefixe_sha256, suffixe_sha256, cursor2csv, executer_requete_select
from courriel import envoyer_courriel

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour effectuer les validations et transformations sur les clientèles dans l\'entrepôt')
    parser.add_argument('--session', action='store_true', help='Si présent, va faire le chargement des clientèles de la session')
    return parser.parse_args()


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("transformation/clienteles.py")

# Les arguments en ligne de commande
args = parse_arguments()

# Le préfixe et le suffixe pour les sha256
prefixe = prefixe_sha256()
suffixe = suffixe_sha256()

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # Travail sur la table des étudiants
    nom_table = "_tmp_etudiants"
    logger.info(f"Début de la transformation des données sur les étudiants")

    # On vérifie si on a de nouveaux codes de cycles
    requete = f"SELECT codecycle, codeprogramme, programme from {nom_table} WHERE substring(codecycle, 1, 1) NOT IN (SELECT cycle FROM _verif_cycles)"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des cycles d'étude lors du chargement des étudiants a identifié de nouveaux cycles d'études.

Vous devez vérifier le premier caractère des codes de cycle ci-dessous et les ajouter dans la table _verif_cycles.


"""
            envoyer_courriel("Entrepôt de données - Nouveaux cycles d'études", intro + "\n\n" + res, logger)

    # On peut maintenant inscrire le niveau
    requete = f"UPDATE {nom_table} SET niveau = 'Étudiant - 1er cycle' WHERE substring(codecycle, 1, 1) = '1'"
    executer_requete(connexion, requete, logger)
    requete = f"UPDATE {nom_table} SET niveau = 'Étudiant - 2e cycle' WHERE substring(codecycle, 1, 1) = '2'"
    executer_requete(connexion, requete, logger)
    requete = f"UPDATE {nom_table} SET niveau = 'Étudiant - 3e cycle' WHERE substring(codecycle, 1, 1) = '3'"
    executer_requete(connexion, requete, logger)
    requete = f"UPDATE {nom_table} SET niveau = 'Postdoctorat' WHERE substring(codecycle, 1, 1) = '4'"
    executer_requete(connexion, requete, logger)
    requete = f"UPDATE {nom_table} SET niveau = 'Étudiant - Médecine 3e cycle' WHERE substring(codecycle, 1, 1) = '6'"
    executer_requete(connexion, requete, logger)
    requete = f"UPDATE {nom_table} SET niveau = 'Étudiant - Certificat' WHERE substring(codecycle, 1, 1) = 'C'"
    executer_requete(connexion, requete, logger)

    # La discipline

    # On vérifie si on a de nouveaux codes de programmes
    requete = f"SELECT codeprogramme, programme from {nom_table} WHERE codeprogramme NOT IN (SELECT code FROM programmes) GROUP BY codeprogramme, programme ORDER BY codeprogramme;"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des programmes d'étude lors du chargement des étudiants a identifié de nouveaux programmes.

Vous devez vérifier les codes de programme ci-dessous et les ajouter dans la table programmes.


"""
            envoyer_courriel("Entrepôt de données - Nouveaux programmes d'études", intro + res, logger)

    # On insère la discipline
    requete = f"""
        UPDATE {nom_table}
        SET discipline = programmes.discipline
        FROM programmes
        WHERE {nom_table}.codeprogramme = programmes.code;
    """
    executer_requete(connexion, requete, logger)
    requete = f"""
        UPDATE {nom_table}
        SET discipline = 'Inconnue'
        WHERE NOT EXISTS (SELECT 1 FROM programmes WHERE {nom_table}.codeprogramme = programmes.code);
    """
    executer_requete(connexion, requete, logger)

    # La bibliothèque (depuis la table des disciplines)

    # On s'assure que pour chaque discipline il y a une bibliothèque
    requete = f"SELECT t.discipline FROM {nom_table} t WHERE t.discipline NOT IN (SELECT discipline FROM disciplines WHERE disciplines.discipline = t.discipline) GROUP BY t.discipline;"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
La vérification des disciplines lors du chargement des étudiants a identifié des disciplines sans correspondances.

Vous devez vérifier les disciplines ci-dessous et vous assurer qu'elles sont dans la table disciplines.


"""
            envoyer_courriel("Entrepôt de données - Disciplines non définies", intro + res, logger)

    requete = f"""
        UPDATE {nom_table}
        SET bibliotheque = disciplines.bibliotheque
        FROM disciplines
        WHERE {nom_table}.discipline = disciplines.discipline;
    """
    executer_requete(connexion, requete, logger)

    # On va copier les données temporaires dans la table cumulative
    # On suppose ici qu'on conserve tous les étudiants (pas de critères d'élimination)
    requete = f"""
        INSERT INTO _clientele_cumul
        (jour, usager, courriel, codebarres, fonction, niveau, code_programme, programme, discipline, bibliotheque)
        SELECT 
            jour,
            sha256(concat('{prefixe}', login, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', courriel, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', codebarres, '{suffixe}')::bytea)::varchar,
            'Étudiant',
            niveau,
            codeprogramme,
            programme,
            discipline,
            bibliotheque
        FROM _tmp_etudiants
        ON CONFLICT (usager) DO UPDATE
        SET
            jour = EXCLUDED.jour,
            courriel = EXCLUDED.courriel,
            codebarres = EXCLUDED.codebarres, 
            fonction = EXCLUDED.fonction,
            niveau = EXCLUDED.niveau,
            code_programme = EXCLUDED.code_programme,
            programme = EXCLUDED.programme,
            discipline = EXCLUDED.discipline,
            bibliotheque = EXCLUDED.bibliotheque;
    """
    executer_requete(connexion, requete, logger)



finally:
    # On ferme la connexion
    fermer_connexion(connexion, logger)
