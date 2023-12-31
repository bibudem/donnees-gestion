import logging
import configparser

logger = logging.getLogger

def initialisation_logs():
    journalisation = configparser.ConfigParser()
    journalisation.read('_config.ini')
    logging.basicConfig(filename=journalisation['logs']['fichier'], level=getattr(logging, journalisation['logs']['niveau']), format='%(asctime)s - %(levelname)s: [%(name)s] %(message)s')
