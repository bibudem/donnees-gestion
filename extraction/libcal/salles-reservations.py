import argparse
import requests
import csv
from datetime import datetime

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
        return response.json()
    else:
        print(f"Erreur lors de la requête à l'API. Code d'erreur : {response.status_code}")
        return None

def paginer_api(url, token, params=None, taille_page=100):
    page = 1
    resultats_totaux = []

    while True:
        params_page = {'page': page}
        if params:
            params_page.update(params)

        # Appel de l'API
        data = appel_api(url, token, params=params_page)

        if not data:
            break

        # Ajout des résultats à la liste totale
        resultats_totaux.extend(data)

        # Vérification s'il y a plus de pages
        if len(data) < taille_page:
            break

        # Passage à la page suivante
        page += 1

    return resultats_totaux

def parse_arguments():
    parser = argparse.ArgumentParser(description='Script pour obtenir une liste de réservations en format CSV')
    parser.add_argument('--url_api', required=True, help='URL de base de l\'API LibCal')
    parser.add_argument('--client_id', required=True, help='ID du client pour l\'authentification')
    parser.add_argument('--client_secret', required=True, help='Secret du client pour l\'authentification')
    parser.add_argument('--date_debut', required=True, help='Date de début (aaaa-mm-dd)')
    parser.add_argument('--date_fin', required=True, help='Date de fin (aaaa-mm-dd)')
    parser.add_argument('--taille_page', type=int, default=100, help='Taille de la page de résultats (par défaut: 100)')
    parser.add_argument('--fichier_sortie', required=True, help='Fichier de sortie')

    return parser.parse_args()

def convertir_json_en_csv(data, fichier_csv):
    # Liste des propriétés que vous souhaitez extraire
    champs = ["bookId", "fromDate", "email", "status", "location_name", "category_name", "item_name"]

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

if __name__ == "__main__":

    # Les arguments en ligne de commande
    args = parse_arguments()

    # L'URL de base de l'API
    url_api = args.url_api

    # On va obtenir le jeton d'accès
    url_token = url_api + "/oauth/token"
    client_id = args.client_id
    client_secret = args.client_secret
    access_token = obtenir_token(url_token, client_id, client_secret)

    # On va calculer le nombre de jours
    date_debut = datetime.strptime(args.date_debut, "%Y-%m-%d")
    date_fin = datetime.strptime(args.date_fin, "%Y-%m-%d")
    difference = date_fin - date_debut
    nb_jours = difference.days

    if access_token:

        # Les paramètres à passer
        params_page = {'date': date_debut.strftime("%Y-%m-%d"), 'days': nb_jours, 'limit': args.taille_page, 'include_tentative': 0, 'include_cancel': 0}

        # On appelle l'API paginée avec le jeton d'accès obtenu
        resultats = paginer_api(url_api + "/space/bookings", access_token, params_page, args.taille_page)

        # On écrit le fichier CSV
        convertir_json_en_csv(resultats, args.fichier_sortie)

    else:
        print("Impossible d'obtenir le jeton d'accès.")
