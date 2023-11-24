import configparser
import psycopg2
import logging
import sys
import os
sys.path.append(os.path.abspath("../commun"))
from logs import initialisation_logs
from db import se_connecter_a_la_base_de_donnees, fermer_connexion, executer_requete

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("creation.py")

# Domaines à créer/supprimer
domaines = configparser.ConfigParser()
domaines.read('_domaines.ini')

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
                code VARCHAR(50),
                nom VARCHAR(255),
                departement VARCHAR(255),
                discipline VARCHAR(255),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (code)
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
                dateheure TIMESTAMP,
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
                Utlisateur_Login VARCHAR(50),
                Ouverture_session TIMESTAMP,
                Fermeture_session TIMESTAMP,
                Duree INTEGER,
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
                ordinateur VARCHAR(100),
                debut TIMESTAMP,
                fin TIMESTAMP,
                duree INTEGER,
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (usager, ordinateur, debut)
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
                journee date,
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
                Login VARCHAR(100),
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
                usager VARCHAR(255),
                courriel VARCHAR(255),
                codebarres VARCHAR(50),
                fonction VARCHAR(50),
                niveau VARCHAR(50),
                code_programme VARCHAR(10),
                programme VARCHAR(255),
                code_unite VARCHAR(10),
                unite VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (usager)
            );
        """
        executer_requete(connexion, requete, logger)

        # La table de clientèle visible dans PowerBI
        nom_table = "clientele"
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table, logger)

        # Création de la table
        requete = f"""
            CREATE TABLE {nom_table} (
                session VARCHAR(50),
                usager VARCHAR(255),
                courriel VARCHAR(255),
                codebarres VARCHAR(50),
                fonction VARCHAR(50),
                niveau VARCHAR(50),
                code_programme VARCHAR(10),
                programme VARCHAR(255),
                code_unite VARCHAR(10),
                unite VARCHAR(255),
                discipline VARCHAR(255),
                bibliotheque VARCHAR(100),
                CONSTRAINT pkey_{nom_table} PRIMARY KEY (session, usager)
            );
        """
        executer_requete(connexion, requete, logger)

finally:
    # Fermeture de la connexion
    fermer_connexion(connexion, logger)
