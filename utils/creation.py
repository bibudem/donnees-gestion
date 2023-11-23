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
                discipline VARCHAR(255)
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
                bookId VARCHAR(50),
                fromDate TIMESTAMP,
                email VARCHAR(255),
                status VARCHAR (255),
                location_name VARCHAR(255),
                category_name VARCHAR(255),
                item_name VARCHAR(255)
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
                dateheure TIMESTAMP,
                bibliotheque VARCHAR(255),
                categorie VARCHAR(255),
                salle VARCHAR(255)
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
                email VARCHAR(255)
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
                evenement_id VARCHAR(50)
            );
        """
        executer_requete(connexion, requete, logger)

finally:
    # Fermeture de la connexion
    fermer_connexion(connexion, logger)
