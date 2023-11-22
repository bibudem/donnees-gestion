import logging
import psycopg2
import configparser


def se_connecter_a_la_base_de_donnees(logger):

    # Configuration de la base de données
    config = configparser.ConfigParser()
    config.read('../config/_db.ini')

    try:
        conn = psycopg2.connect(
            dbname=config['database']['dbname'],
            user=config['database']['user'],
            password=config['database']['password'],
            host=config['database']['host'],
            port=config['database']['port']
        )
        logger.info("Connexion à la base de données réussie.")
        return conn
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à la base de données : {e}")
        raise

def fermer_connexion(conn, logger):
    try:
        conn.close()
        logger.info("Connexion à la base de données fermée.")
    except Exception as e:
        logger.error(f"Erreur lors de la fermeture de la connexion : {e}")
        raise

def executer_requete(conn, requete_sql, logger):
    try:
        with conn.cursor() as cursor:
            cursor.execute(requete_sql)
            logger.info(f"Requête exécutée avec succès : {requete_sql}")
            conn.commit()
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête : {e}")
        conn.rollback()
        raise
