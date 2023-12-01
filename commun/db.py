import logging
import psycopg2
import configparser


def se_connecter_a_la_base_de_donnees(logger):

    # Configuration de la base de données
    config = configparser.ConfigParser()
    config.read('_config.ini')

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
            conn.commit()
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête : {e}")
        conn.rollback()
        raise

def executer_requete_select(cursor, requete_sql, logger):
    try:
        cursor.execute(requete_sql)
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête : {e}")
        raise

def prefixe_sha256():
    config = configparser.ConfigParser()
    config.read('_config.ini')
    return config['database']['prefixe']

def suffixe_sha256():
    config = configparser.ConfigParser()
    config.read('_config.ini')
    return config['database']['suffixe']

def cursor2csv(resultats, noms_colonnes, sep = "\t"):
    texte_delimite = ""
    if resultats:
        texte_delimite = sep.join(noms_colonnes) + '\n'
        texte_delimite += '\n'.join(sep.join(map(str, ligne)) for ligne in resultats)
    
    return texte_delimite