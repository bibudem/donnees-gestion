import configparser
import psycopg2
import logging

# Configuration de la base de données
config = configparser.ConfigParser()
config.read('../config/_db.ini')

# Domaines à créer/supprimer
domaines = configparser.ConfigParser()
domaines.read('_domaines.ini')


# Configuration du journal
journalisation = configparser.ConfigParser()
journalisation.read('../config/_logs.ini')
logging.basicConfig(filename=journalisation['logs']['fichier'], level=getattr(logging, journalisation['logs']['niveau']), format='%(asctime)s - %(levelname)s: [%(filename)s] %(message)s')

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

def fermer_connexion(conn):
    try:
        conn.close()
        logging.info("Connexion à la base de données fermée.")
    except Exception as e:
        logging.error(f"Erreur lors de la fermeture de la connexion : {e}")
        raise

# Requêtes de création de tables
try:

    # Quelques constantes
    tmp_prefixe = "_tmp_"
    connexion = se_connecter_a_la_base_de_donnees()

    # Les réservations de salles
    if (domaines.getboolean('domaines', 'reservations')):

        nom_table = "reservations"

        # La table temporaire pour le chargement
        executer_requete(connexion, "DROP TABLE IF EXISTS " + tmp_prefixe + nom_table)

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
        executer_requete(connexion, requete)

        # La table qui sera visible dans PowerBI
        executer_requete(connexion, "DROP TABLE IF EXISTS " + nom_table)

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
        executer_requete(connexion, requete)


finally:
    # Fermeture de la connexion
    fermer_connexion(connexion)
