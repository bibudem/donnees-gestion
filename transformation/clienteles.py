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
    parser.add_argument('--session', help='Si présent, le chargement de la clientèle de session sera effectué avec la date spécifiée')
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
            sys.exit(1)

    # On va maintenant inscrire les disciplines
    requete = f"""
        UPDATE _tmp_etudiants
        SET discipline = programmes.discipline
        FROM programmes
        WHERE programmes.code = SUBSTRING(_tmp_etudiants.codecycle, 2)
        AND _tmp_etudiants.discipline IS NULL
        AND programmes.nom IS NULL
    """
    executer_requete(connexion, requete, logger)

    requete = f"""
        UPDATE _tmp_etudiants
        SET discipline = programmes.discipline
        FROM programmes
        WHERE programmes.code = SUBSTRING(_tmp_etudiants.codecycle, 2)
        AND programmes.nom = _tmp_etudiants.programme
        AND _tmp_etudiants.discipline IS NULL
    """
    executer_requete(connexion, requete, logger)

    # On vérifie si on a inscrit toutes les disciplines
    requete = f"SELECT codecycle, codeprogramme, programme, count(*) as Nb FROM _tmp_etudiants WHERE discipline IS NULL GROUP BY codecycle, codeprogramme, programme ORDER BY codecycle;"
    with connexion.cursor() as cursor:
        executer_requete_select(cursor, requete, logger)
        resultats = cursor.fetchall()
        if (cursor.rowcount > 0):
            # On doit maintenant les retourner en courriel
            noms_colonnes = [desc[0] for desc in cursor.description]
            res = cursor2csv(resultats, noms_colonnes)
            intro = """
L'ajout des disciplines aux étudiants n'est pas complet, certains étudiants ont des programmes qui ne figurent pas dans la table des programmes.

Vous devez vérifier les informations ci-dessous et les ajouter dans la table programmes.


"""
            envoyer_courriel("Entrepôt de données - Nouveaux programmes d'études", intro + res, logger)
            sys.exit(1)

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
            sys.exit(1)

    requete = f"""
        UPDATE {nom_table}
        SET bibliotheque = disciplines.bibliotheque
        FROM disciplines
        WHERE {nom_table}.discipline = disciplines.discipline;
    """
    executer_requete(connexion, requete, logger)

    # Tous les étudiants font partie de la table finale
    # des clientèles, alors on met le booléen à true
    requete = "UPDATE _tmp_etudiants SET clientele = true"
    executer_requete(connexion, requete, logger)

    # On va copier les données temporaires dans la table cumulative
    # On suppose ici qu'on conserve tous les étudiants (pas de critères d'élimination)
    requete = f"""
        INSERT INTO _clientele_cumul
        (journee, usager, courriel, codebarres, fonction, niveau, code_programme, programme, discipline, bibliotheque, clientele)
        SELECT 
            journee,
            sha256(concat('{prefixe}', login, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', courriel, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', codebarres, '{suffixe}')::bytea)::varchar,
            'Étudiant',
            niveau,
            codeprogramme,
            programme,
            discipline,
            bibliotheque,
            clientele
        FROM _tmp_etudiants
        ON CONFLICT (usager) DO UPDATE
        SET
            journee = EXCLUDED.journee,
            courriel = EXCLUDED.courriel,
            codebarres = EXCLUDED.codebarres, 
            fonction = EXCLUDED.fonction,
            niveau = EXCLUDED.niveau,
            code_programme = EXCLUDED.code_programme,
            programme = EXCLUDED.programme,
            discipline = EXCLUDED.discipline,
            bibliotheque = EXCLUDED.bibliotheque,
            clientele = EXCLUDED.clientele;
    """
    executer_requete(connexion, requete, logger)

    # Le personnel
    # Les statuts:
    #   - Q: personnel retraité (on peut les supprimer)
    #   - X: professeurs retraités (on peut les supprimer)
    #   - P: personnel régulier (on les conserve dans _cumul)
    #   - T: personnel temporaire (on les conserver dans _cumul)
    #   - R: professeurs
    #   - C: chargés de cours
    #       - Ensemble ils sont dans 155 unités différentes

    # On supprime les retraités
    requete = "DELETE FROM _tmp_personnel WHERE (statut = 'Q' OR statut = 'X');"
    executer_requete(connexion, requete, logger)

    # On supprime les login en double (ça arrive parfois)
    requete = "DELETE FROM _tmp_personnel WHERE id IN (SELECT MIN(id) FROM _tmp_personnel t GROUP BY t.login HAVING COUNT(t.login) > 1);"
    executer_requete(connexion, requete, logger)

    # On supprime les usagers ssans login (ne devrait pas arriver, mais c'est une clé primaire)
    requete = "DELETE FROM _tmp_personnel WHERE (login IS NULL);"
    executer_requete(connexion, requete, logger)

    # On supprime les personnes qui sont aussi étudiants (on priorise les étudiants)
    requete = "DELETE FROM _tmp_personnel WHERE login in (SELECT login FROM _tmp_etudiants);"
    executer_requete(connexion, requete, logger)

    # On ajuste les fonctions et les niveaux
    requete = "UPDATE _tmp_personnel SET nfonction = 'Chargé de cours', niveau = 'Chargé de cours' WHERE statut = 'C';"
    executer_requete(connexion, requete, logger)
    requete = "UPDATE _tmp_personnel SET nfonction = 'Professeur', niveau = 'Professeur' WHERE statut = 'R';"
    executer_requete(connexion, requete, logger)
    requete = "UPDATE _tmp_personnel SET nfonction = 'Personnel' WHERE (statut = 'P' OR statut = 'T');"
    executer_requete(connexion, requete, logger)

    # On ajoute la discipline en fonction du code de l'unité
    requete = """
        UPDATE _tmp_personnel
        SET discipline = unites.discipline
        FROM unites
        WHERE codeunite = unites.code
    """
    executer_requete(connexion, requete, logger)
    
    # On ajoute la bibliothèque en fonction de la discipline
    requete = """
        UPDATE _tmp_personnel
        SET bibliotheque = disciplines.bibliotheque
        FROM disciplines
        WHERE _tmp_personnel.discipline = disciplines.discipline
    """
    executer_requete(connexion, requete, logger)

    # On va ajuster la colonne qui indique si on le veut
    # dans les clientèles

    # Pour le personnel: non
    requete = "UPDATE _tmp_personnel SET clientele = false WHERE (statut = 'P' OR statut = 'T')"
    executer_requete(connexion, requete, logger)

    # Pour les chargés de cours, selon la fonction
    requete = "UPDATE _tmp_personnel SET clientele = false WHERE statut = 'C'"
    executer_requete(connexion, requete, logger)
    requete = "UPDATE _tmp_personnel SET clientele = true WHERE statut = 'C' AND fonction IN ('Chargé(e) de cours')"
    executer_requete(connexion, requete, logger)

    # Pour les professeurs, selon la fonction
    requete = "UPDATE _tmp_personnel SET clientele = false WHERE statut = 'R'"
    executer_requete(connexion, requete, logger)
    requete = """
        UPDATE _tmp_personnel SET clientele = true
        WHERE statut = 'R' AND
        (
            fonction LIKE 'Chercheur%' OR
            fonction LIKE 'Enseignant%' OR
            (fonction LIKE 'Prof%' AND NOT fonction LIKE '%clini%')
        );
    """
    executer_requete(connexion, requete, logger)

    # On va copier les données temporaires dans la table cumulative
    requete = f"""
        INSERT INTO _clientele_cumul
        (journee, usager, courriel, codebarres, fonction, code_unite, unite, niveau, discipline, bibliotheque, clientele)
        SELECT 
            journee,
            sha256(concat('{prefixe}', login, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', courriel, '{suffixe}')::bytea)::varchar,
            sha256(concat('{prefixe}', codebarres, '{suffixe}')::bytea)::varchar,
            nfonction,
            codeunite,
            descunite,
            niveau,
            discipline,
            bibliotheque,
            clientele
        FROM _tmp_personnel
        ON CONFLICT (usager) DO UPDATE
        SET
            journee = EXCLUDED.journee,
            courriel = EXCLUDED.courriel,
            codebarres = EXCLUDED.codebarres, 
            fonction = EXCLUDED.fonction,
            code_unite = EXCLUDED.code_unite,
            unite = EXCLUDED.unite,
            niveau = EXCLUDED.niveau,
            discipline = EXCLUDED.discipline,
            bibliotheque = EXCLUDED.bibliotheque,
            clientele = EXCLUDED.clientele;
    """
    executer_requete(connexion, requete, logger)

    # On supprime les données temporaires
    requete = "DELETE FROM _tmp_personnel"
    executer_requete(connexion, requete, logger)
    requete = "DELETE FROM _tmp_etudiants"
    executer_requete(connexion, requete, logger)

    # Si nécessaire, on fait un chargement dans la table de clientèle
    if (args.session):

        # La journée des données
        jour = args.session

        # On ne peut pas avoir des données pour une même journée
        requete = f"DELETE FROM clientele WHERE journee = '{jour}'"
        executer_requete(connexion, requete, logger)

        # Normalement la table _clientele_cuml est prête
        requete = f"""
            INSERT INTO clientele (journee, usager, courriel, codebarres, fonction, niveau, code_programme, programme, code_unite, unite, discipline, bibliotheque)
            SELECT
                journee,
                usager,
                courriel,
                codebarres,
                fonction,
                niveau,
                code_programme,
                programme,
                code_unite,
                unite,
                discipline,
                bibliotheque
            FROM _clientele_cumul
            WHERE _clientele_cumul.journee = '{jour}'
            AND _clientele_cumul.clientele = true
        """
        executer_requete(connexion, requete, logger)


        # On va inscrire la donnée de session depuis la date
        requete = """
            UPDATE clientele
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
