import argparse
import requests
import csv
from datetime import datetime, timedelta
import logging
import configparser
import sys
import os
sys.path.append(os.path.abspath("../commun"))
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
        # Deux cas de figure: retourne un array (inscriptions) ou un objet (événements)
        if isinstance(response.json(), list):
            return response.json()
        else:
            return response.json().get("events")       
    else:
        print(f"Erreur lors de la requête à l'API. Code d'erreur : {response.status_code}")
        return None

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour obtenir les événements et leurs inscriptions en format CSV')
    parser.add_argument('--date_debut', required=True, help='Date de début (aaaa-mm-dd)')
    parser.add_argument('--date_fin', required=True, help='Date de fin (aaaa-mm-dd)')
    parser.add_argument('--fichier_sortie', required=True, help='Fichier de sortie (sans l\'exension)')
    return parser.parse_args()

def convertir_json_en_csv(data, champs, fichier_csv):
    with open(fichier_csv, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=champs)
        
        # Écrire l'en-tête CSV
        writer.writeheader()

        # Écrire les données
        for objet in data:
            ligne = {}
            for champ in champs:
                # Support pour les champs imbriqués (par exemple, "event.id")
                champs_nestes = champ.split('.')
                valeur = objet
                for champ_neste in champs_nestes:
                    valeur = valeur.get(champ_neste, '')
                    if valeur is None:
                        break
                ligne[champ] = valeur
            writer.writerow(ligne)

def generer_intervalles(date_debut, date_fin, intervalle_jours):
    date_actuelle = date_debut
    while date_actuelle <= date_fin:
        date_suivante = date_actuelle + timedelta(days=intervalle_jours)
        yield date_actuelle, min(date_suivante - timedelta(days=1), date_fin)
        date_actuelle = date_suivante


# Configuration du journal
initialisation_logs()
logger = logging.getLogger("extraction/inscriptions.py")

# Accès LibCal
config = configparser.ConfigParser()
config.read('../config/_libcal.ini')

# Les arguments en ligne de commande
args = parse_arguments()

# L'URL de base de l'API
url_api = config['evenements']['url']

# On va obtenir le jeton d'accès
url_token = url_api + "/oauth/token"
client_id = config['evenements']['client']
client_secret = config['evenements']['secret']
access_token = obtenir_token(url_token, client_id, client_secret)

# La logique est la suivante:
#   - On ne peut pas paginer sur les événements ou inscriptions
#       avec l'API de LibCal, et la limite est de 500 par appel
#   - On va donc boucler sur les événements sur un certain nombre
#       de jours (10) pour qu'on n'ait jamais plus de 500 événements
#   - Pour obtenir les inscriptions, il faut y aller événement
#       par événement

if access_token:

    # Les arrays qui vont contenir les données en JSON
    evenements = []
    inscriptions = []

    # On va calculer le nombre de jours total
    date_debut = datetime.strptime(args.date_debut, "%Y-%m-%d")
    date_fin = datetime.strptime(args.date_fin, "%Y-%m-%d")
    nb_jours_total = (date_fin - date_debut).days

    # On va boucler par groupe de 10 jours
    for debut, fin in generer_intervalles(date_debut, date_fin, 10):
        print(f"De {debut.strftime('%Y-%m-%d')} à {fin.strftime('%Y-%m-%d')}; {(fin - debut).days + 1} jours")

        # On va chercher les données sur les événements pour la période
        params = {'date': debut.strftime("%Y-%m-%d"), 'days': (fin - debut).days, 'limit': config.getint('evenements', 'page'), 'cal_id': 7690}
        nouveaux_evenements = appel_api(url_api + "/events", access_token, params)

        # On les conserve dans la liste des événements
        evenements.extend(nouveaux_evenements)

        # On peut maintenant boucler sur les événements
        for evenement in nouveaux_evenements:
            # L'identifiant est ce qui va nous permettre d'aller chercher les inscriptions
            ev_id = str(evenement.get("id"))
            # On appelle l'API pour les inscriptions
            inscriptions_evenement = appel_api(url_api + "/events/" + ev_id + "/registrations", access_token, [])
            # On traite les inscrits
            registrants = inscriptions_evenement[0].get("registrants")
            for registrant in registrants:
                registrant['event_id'] = ev_id
            inscriptions.extend(registrants)

        # À ce moment-ci, on a tous les événements dans evenements[]
        # et toutes les inscriptions dans inscriptions[]
        # On peut créer les fichiers CSV
        convertir_json_en_csv(evenements, ["id", "title", "start", "end", "owner.name", "presenter"], args.fichier_sortie + "_evenements.csv")
        convertir_json_en_csv(inscriptions, ["event_id", "booking_id", "registration_type", "email"], args.fichier_sortie + "_inscriptions.csv")

else:
    logger.error("Impossible d'obtenir le jeton d'accès")
