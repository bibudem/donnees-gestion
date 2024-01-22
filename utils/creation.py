import configparser
import psycopg2
import logging
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("utils/creation.py")

# Domaines à créer/supprimer
domaines = configparser.ConfigParser()
domaines.read('_config.ini')

# Requêtes de création de tables
try:

    # Quelques constantes
    tmp_prefixe = "_tmp_"
    connexion = se_connecter_a_la_base_de_donnees(logger)

    # La table de correspondance des programmes
    if (domaines.getboolean('domaines', 'programmes')):
        logger.info("Début de la création de la table des programmes")

        nom_table = "programmes"

        # On supprime la table si elle existe
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                code VARCHAR(50),
                nom VARCHAR(255) NULL,
                discipline VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table des secteurs dans lesquels se retrouvent les bibliotheques

    if (domaines.getboolean('domaines', 'secteurs')):
        logger.info("Début de la création de la table des secteurs")

        nom_table = "secteurs"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                bibliotheque VARCHAR(100),
                secteur VARCHAR(100),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (bibliotheque)
            );
        """
        logger.info("Fin de la création de la table des secteurs")
        executer_requete(connexion, requete, logger)


    # La table des disciplines
    if (domaines.getboolean('domaines', 'disciplines')):
        logger.info("Début de la création de la table des disciplines")

        nom_table = "disciplines"

        # On supprime la table si elle existe
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                discipline VARCHAR(255),
                bibliothecaire VARCHAR(255),
                bibliotheque VARCHAR(255),
                secteur VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (discipline, bibliothecaire)
            );
        """
        executer_requete(connexion, requete, logger)


    # La table des unités
    if (domaines.getboolean('domaines', 'unites')):
        logger.info("Début de la création de la table des unités")

        # On supprime la table si elle existe
        executer_requete(connexion, "DROP TABLE IF EXISTS unites", logger)

        # Création de la table
        requete = """
            CREATE TABLE unites (
                code VARCHAR(255),
                nom VARCHAR(255),
                discipline VARCHAR(255),
                CONSTRAINT pkey_unites PRIMARY KEY (code)
            );
        """
        executer_requete(connexion, requete, logger)



    # Les réservations de salles
    if (domaines.getboolean('domaines', 'reservations')):

        nom_table = "reservations"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                fromDate TIMESTAMP,
                email VARCHAR(255),
                status VARCHAR (255),
                location_name VARCHAR(255),
                category_name VARCHAR(255),
                item_name VARCHAR(255),
                usager VARCHAR(255),
                discipline VARCHAR(255),
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                usager VARCHAR(255),
                courriel VARCHAR(255),
                discipline VARCHAR(255),
                journee DATE,
                session DATE,
                bibliotheque VARCHAR(255),
                categorie VARCHAR(255),
                salle VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

    # Les inscriptions
    if (domaines.getboolean('domaines', 'inscriptions')):

        nom_table = "inscriptions"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                event_id VARCHAR(50),
                booking_id VARCHAR(50),
                registration_type VARCHAR (255),
                email VARCHAR(255),
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (event_id, booking_id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                id VARCHAR(50),
                usager VARCHAR(255),
                courriel VARCHAR(255),
                discipline VARCHAR(255),
                evenement_id VARCHAR(50),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (id, evenement_id)
            );
        """
        executer_requete(connexion, requete, logger)

    # Les inscriptions
    if (domaines.getboolean('domaines', 'evenements')):

        nom_table = "evenements"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        # id,title,start,end,owner.name,presenter
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                id VARCHAR(50),
                title VARCHAR(255),
                start TIMESTAMP,
                finish TIMESTAMP,
                owner VARCHAR(255),
                presenter VARCHAR(255),
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        # TODO: pas clair ce qu'on fera avec ça
        requete = f"""
            CREATE TABLE {nom_table} (
                id VARCHAR(50),
                titre VARCHAR(255),
                journee DATE,
                session DATE,
                proprietaire VARCHAR(255),
                presentateur VARCHAR(255),
                nb_participants INTEGER,
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

    # Les ordinateurs publics
    if (domaines.getboolean('domaines', 'ordinateurs')):

        nom_table = "sessions_ordinateurs"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table

        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                ID INTEGER,
                Secteur VARCHAR(100),
                Station VARCHAR(100),
                Utilisateur_Login VARCHAR(255),
                Ouverture_session TIMESTAMP,
                Fermeture_session TIMESTAMP,
                Duree INTEGER,
                courriel VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(255),
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                usager VARCHAR(255),
                courriel VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                session DATE,
                journee DATE,
                dateheure TIMESTAMP,
                lieu VARCHAR(255),
                ordinateur VARCHAR(255),
                duree INTEGER,
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (usager, ordinateur, dateheure)
            );
        """
        executer_requete(connexion, requete, logger)

    # La fréquentation
    if (domaines.getboolean('domaines', 'frequentation')):

        nom_table = "frequentation"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table

        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                Enregistrement INTEGER,
                Secteur VARCHAR(100),
                Date TIMESTAMP,
                Entrees INTEGER,
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (enregistrement)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                journee DATE,
                session DATE,
                bibliotheque VARCHAR(100),
                frequentation INTEGER,
                occupation INTEGER,
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (journee, bibliotheque)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table temporaire pour le chargement de l'occupation
        nom_table = "occupation"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                Enregistrement INTEGER,
                Secteur VARCHAR(100),
                Date TIMESTAMP,
                Occupation INTEGER,
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (enregistrement)
            );
        """
        executer_requete(connexion, requete, logger)

    # Les fichiers Synchro (étudiants et personnel)
    if (domaines.getboolean('domaines', 'synchro')):


        # La table temporaire pour le chargement des étudiants
        nom_table = "etudiants"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                codebarres VARCHAR(50),
                codeCycle VARCHAR(10),
                codeProgramme VARCHAR(10),
                programme VARCHAR(255),
                courriel VARCHAR(100),
                login VARCHAR(100),
                niveau VARCHAR(50),
                journee DATE,
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                clientele BOOLEAN,
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (login)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table temporaire pour le chargement du personnel
        nom_table = "personnel"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {tmp_prefixe}{nom_table} (
                id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                CodeUnite VARCHAR(10),
                DescUnite VARCHAR(255),
                Statut VARCHAR(5),
                Courriel VARCHAR(255),
                CodeBarres VARCHAR(50),
                Fonction VARCHAR(255),
                nfonction VARCHAR(255),
                Login VARCHAR(100),
                niveau VARCHAR(50),
                journee DATE,
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                clientele BOOLEAN,
                CONSTRAINT pkey_{tmp_prefixe}{nom_table} PRIMARY KEY (id)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table de clientèle cumulative, non visible dans PowerBI
        nom_table = "_clientele_cumul"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                journee date,
                usager VARCHAR(255),
                courriel VARCHAR(255),
                codebarres VARCHAR(255),
                fonction VARCHAR(50),
                niveau VARCHAR(50),
                session VARCHAR(255),
                code_programme VARCHAR(10),
                programme VARCHAR(255),
                code_unite VARCHAR(10),
                unite VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                clientele BOOLEAN
            );
        """
        executer_requete(connexion, requete, logger)

        # La table de clientèle visible dans PowerBI
        nom_table = "clientele"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                journee date,
                usager VARCHAR(255),
                courriel VARCHAR(255),
                codebarres VARCHAR(255),
                fonction VARCHAR(50),
                niveau VARCHAR(50),
                session VARCHAR(255),
                code_programme VARCHAR(10),
                programme VARCHAR(255),
                code_unite VARCHAR(10),
                unite VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (journee, usager)
            );
        """
        executer_requete(connexion, requete, logger)


        # Création de la table statistiques de la clientèle

    if (domaines.getboolean('domaines', 'usagers_stats')):
        logger.info("Début de la création de la table des statistiques des usagers")

        nom_table = "disciplines_stats"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        requete = f"""
            CREATE TABLE {nom_table}  (
            discipline VARCHAR(255),
            bibliothecaire VARCHAR(255),
            bibliotheque VARCHAR(255),
            secteur VARCHAR(255),
            fonction VARCHAR(255),
            niveau VARCHAR(255),
            nb_personnes NUMERIC,
            CONSTRAINT pkey_disciplines_stats PRIMARY KEY (discipline, bibliothecaire, fonction, niveau)
            );
        """
        executer_requete(connexion, requete, logger)

    
    # Les tables de vérification
    if (domaines.getboolean('domaines', 'verifications')):

        # Les codes de cycles
        nom_table = "_verif_cycles"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                cycle VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (cycle)
            );
        """
        executer_requete(connexion, requete, logger)

        # Les status de réservation de salles
        nom_table = "_verif_statuts_reservations"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                nom VARCHAR(255),
                conserver BOOLEAN,
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (nom)
            );
        """
        executer_requete(connexion, requete, logger)

    # La table de synonymes
    if (domaines.getboolean('domaines', 'synonymes')):

        nom_table = "_synonymes"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                domaine VARCHAR(255),
                rejeter VARCHAR(255),
                accepter VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (domaine, rejeter)
            );
        """
        executer_requete(connexion, requete, logger)

finally:
    # Fermeture de la connexion
    fermer_connexion(connexion, logger)
