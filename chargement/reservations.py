import configparser
import logging
import psycopg2
from psycopg2 import sql
import argparse


# Configuration de la base de données
config = configparser.ConfigParser()
config.read('../config/_db.ini')

# Configuration du journal
journalisation = configparser.ConfigParser()
journalisation.read('../config/_logs.ini')
logging.basicConfig(filename=journalisation['logs']['fichier'], level=getattr(logging, journalisation['logs']['niveau']), format='%(asctime)s - %(levelname)s: [%(filename)s] %(message)s')

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour charger les données de réservation de salles dans l\'entrepôt')
    parser.add_argument('--fichier', required=True, help='Chemin du fichier CSV à charger')
    return parser.parse_args()

def se_connecter_a_la_base_de_donnees():
    try:
        conn = psycopg2.connect(
            dbname=config['database']['dbname'],
            user=config['database']['user'],
            password=config['database']['password'],
            host=config['database']['host'],
            port=config['database']['port']
        )
        logging.info("Connexion à la base de données réussie.")
        return conn
    except Exception as e:
        logging.error(f"Erreur lors de la connexion à la base de données : {e}")
        raise

def fermer_connexion(conn):
    try:
        conn.close()
        logging.info("Connexion à la base de données fermée.")
    except Exception as e:
        logging.error(f"Erreur lors de la fermeture de la connexion : {e}")
        raise

def executer_requete(conn, requete_sql):
    try:
        with conn.cursor() as cursor:
            cursor.execute(requete_sql)
            logging.info(f"Requête exécutée avec succès : {requete_sql}")
            conn.commit()
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution de la requête : {e}")
        conn.rollback()
        raise

# Les arguments en ligne de commande
args = parse_arguments()

# Chemin du fichier CSV à charger
chemin_fichier_csv = args.fichier

# Nom de la table dans la base de données
nom_table = "_tmp_reservations"

logging.info(f"Début du chargement des réservations depuis le fichier {chemin_fichier_csv}")

try:

    # Connexion à la base de données
    connexion = se_connecter_a_la_base_de_donnees()

    # On commence par supprimer toute donnée dans la table temporaire
    requete = f"DELETE FROM {nom_table};"
    executer_requete(connexion, requete)

    # Ensuite on charge les données
    requete = f"""
        COPY {nom_table}
        FROM '{chemin_fichier_csv}'
        DELIMITER ','
        CSV HEADER;
    """
    executer_requete(connexion, requete)

finally:
    # On ferme la connexion
    fermer_connexion(connexion)
