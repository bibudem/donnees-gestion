import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import configparser

def envoyer_courriel(objet, contenu, logger):

    # Configuration courriel
    config = configparser.ConfigParser()
    config.read('_config.ini')

    if (config.getboolean('email', 'envoyer')):

        # Paramètres SMTP
        serveur_smtp = config['email']['server']
        port_smtp = config['email']['port']
        expediteur = config['email']['from']
        # Si le serveur indiqué est "localhost", on présume que le username et password ne sont pas nécessaires
        if config['email']['server'] != "localhost":
            username = config['email']['username']
            password = config['email']['password']

        # Destinataire
        destinataire = config['email']['to']

        # Construire le message
        message = MIMEMultipart()
        message['From'] = expediteur
        message['To'] = destinataire
        message['Subject'] = objet

        # Ajouter le corps du message
        message.attach(MIMEText(contenu, 'plain'))

        try:
            # Se connecter au serveur SMTP
            with smtplib.SMTP(serveur_smtp, port_smtp) as serveur:
                # Démarrer la connexion sécurisée (TLS)
                serveur.starttls()

                # S'authentifier auprès du serveur SMTP, seulement si nous avons les infos de login pour un SMTP qui n'est pas localhost
                if 'username' in locals() and 'password' in locals() and 'serveur_smtp' != 'localhost':
                    serveur.login(username, password)

                # Envoyer le message
                serveur.send_message(message)
            logger.info(f"Courriel envoyé à {destinataire} - Objet: {objet}")

        except smtplib.SMTPException as e:
            logger.error(f"Échec de l'envoi du courriel à {destinataire} - Objet: {objet}")

