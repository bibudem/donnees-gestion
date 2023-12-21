import argparse
import requests
import csv
from datetime import datetime
import logging
import configparser
import sys
import os
sys.path.append(os.path.abspath("commun"))
from logs import initialisation_logs

def obtenir_token(url_token, client_id, client_secret):
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }

    response = requests.post(url_token, data=data)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        print(f"Erreur lors de l'obtention du jeton d'accès. Code d'erreur : {response.status_code}")
        return None

def appel_api(url, token, params=None):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()['transactions']
    else:
        print(f"Erreur lors de la requête à l'API. Code d'erreur : {response.status_code}")
        return None

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour obtenir une liste de transactions de référence en format CSV')
    parser.add_argument('--date_debut', required=True, help='Date de début (aaaa-mm-dd)')
    parser.add_argument('--date_fin', required=True, help='Date de fin (aaaa-mm-dd)')
    parser.add_argument('--fichier_sortie', required=True, help='Fichier de sortie')

    return parser.parse_args()

def convertir_json_en_csv(data, fichier_csv):

    # Le Json ressemble à ceci:
    # {'transaction_id': 19065511, 'dataset_id': 3634, 'created': '2023-09-01 11:57:54', 'question': '', 'details': '', 'answer': '', 'internal_note': '', 'owner': 'charles.turgeon@umontreal.ca', 'custom_fields': [{'field_id': 676, 'field_label': 'Unités', 'field_answer': 'Thérèse-Gouin-Décarie'}, {'field_id': 677, 'field_label': 'Localisation', 'field_answer': 'Comptoir de service'}, {'field_id': 678, 'field_label': "Type d'intervention", 'field_answer': 'Question de renseignement'}, {'field_id': 679, 'field_label': 'Modalité', 'field_answer': 'Non virtuel'}]}
    champs = ["transaction_id", "created", "owner", "Unités", "Localisation", "Type d'intervention", "Modalité"]

    with open(fichier_csv, 'w', newline='', encoding='utf-8') as csv_file:

        writer = csv.DictWriter(csv_file, fieldnames=champs)

        # Écrire l'en-tête CSV
        writer.writeheader()

        # Écrire les données
        for objet in data:
            ligne = {}
            # On va chercher les informations explicitement pour les éléments de premier niveau
            ligne["transaction_id"] = objet.get("transaction_id", "")
            ligne["created"] = objet.get("created", "")
            ligne["owner"] = objet.get("owner", "")

            # Ensuite on boucle sur les custom_fields
            custom = objet.get("custom_fields", [])
            for field in custom:
                ligne[field.get("field_label", "")] = field.get("field_answer", "")

            writer.writerow(ligne)

# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/reference.py")

# Le fichier de configuration
config = configparser.ConfigParser()
config.read('_config.ini')

# Les arguments en ligne de commande
args = parse_arguments()

# L'URL de base de l'API
url_api = config['reference']['url']

# L'ID du jeu de données Reference Analytics
dataset_id = config['reference']['dataset']

# Le jeton d'accès
url_token = url_api + "/oauth/token"
client_id = config['reference']['client']
client_secret = config['reference']['secret']
access_token = obtenir_token(url_token, client_id, client_secret)

# On va calculer le nombre de jours
date_debut = datetime.strptime(args.date_debut, "%Y-%m-%d")
date_fin = datetime.strptime(args.date_fin, "%Y-%m-%d")

if access_token:

    # On doit passer deux dates en paramètres, pour le début et la fin
    # Maximum un mois selon la documentation
    params = {'date_range[]': [date_debut.strftime("%Y-%m-%d"),date_fin.strftime("%Y-%m-%d")]}

    # On appelle l'API avec le jeton d'accès obtenu
    resultats = appel_api(url_api + "/ra/dataset/" + dataset_id + "/transactions", access_token, params)

    # On écrit le fichier CSV
    convertir_json_en_csv(resultats, args.fichier_sortie)

else:
    logger.error("Impossible d'obtenir le jeton d'accès")
