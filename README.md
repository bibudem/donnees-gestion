# Manipulation des données de gestion

Ce dépôt contient différents scripts pour extraire et manipuler les données de gestion des bibliothèques de l'UdeM.

Les manipulations sont divisées en trois grandes parties:

1. **Extraction**: les données sont extraites depuis les fichiers ou systèmes sources et stockées dans un fichier temporaire.
2. **Chargement**: les données sont lues dans les fichiers temporaires pour être chargés tels quels - ou à peu près - dans l'entrepôt de données, dans des tables temporaires et non exposées.
3. **Transformation**: les données des tables temporaires sont manipulées puis transférées vers des tables définitives, sur lesquelles des rapports peuvent être constuits.

## Aperçu des sources de données

### Clientèles (usagers)

Les données sur les clientèles sont obtenues à partir de deux extractions quotidiennes (étudiants et personnel) obtenues du progiciel de gestion intégré (Synchro).

Les données utiles sont d'abord extraites de ces fichiers, puis chargées dans l'entrepôt.

Le chargement alimente quotidiennement la liste cumulative des usagers, à des fins de référence, avec des informations sur leur discipline. Par ailleurs, une fois par session, les données d'une journée sont copiées dans une table de clientèle qui sert à fournir des informations démographiques stables.

### Réservations de salles

Les réservations de salles sont effectuées dans LibCal. L'extraction des données, quotidienne, s'effectue par l'API de LibCal. La transformation des données permet d'associer des disciplines aux usagers qui effectuent des réservations.

### Formations

Les formations libres offertes aux usagers des bibliothèques sont gérées dans LibCal. L'extraction des données, qui contient à la fois la liste des formations et les inscriptions à ces formations, s'effectue quotidiennement par l'API de LibCal.

La transformation des données permet d'effectuer partiellement une association de l'usager avec une discipline, lorsque l'usager s'est inscrit avec son adresse institutionnelle.

### Fréquentation et occupation

Les bibliothèques sont équipées de caméras compte-personnes à l'entrée, ce qui permet d'avoir des données de fréquentation et d'occupation des bibliothèques. Ces données sont récupérées depuis l'API d'un service infonuagique du fournisseur à toutes les nuits et stockées dans une base de données.

Nos scripts vont lire cette base de données quotidiennement afin d'en extraire les données utiles puis les charger et transformer dans l'entrepôt.

### Utilisation des postes publics

Les ordinateurs accessibles aux usagers des bibliothèques enregistrent dans une base de données des informations sur les sessions ouvertes.

Nos scripts vont lire cette base de données quotidiennement afin d'en extraire les données utiles puis les charger dans l'entrepôt.

La transformation des données permettra d'associer des disciplines aux usagers qui utilisent les postes publics.

## Instructions générales

### Récupération des sources

Faire un *clone* du dépôt GitHub:

```bash
git clone https://github.com/bibudem/donnees-gestion.git
cd donnees-gestion
```

### Fichier de configuration

Un fichier de configuration `_config.ini` doit être créé. Un exemple est fourni dans le fichier `config-exemple.ini`, toutes les valeurs doivent être renseignées si vous souhaitez exécuter l'ensemble des scripts.

### Journalisation

Les différents scripts utlisent un journal (logs) commun.

C'est le fichier `_config.ini` qui contient les informations.

### Exécution des scripts

Vous devez avoir Python, version 3.10 ou plus récente. Il est probable qu'une version plus ancienne soit compatible, mais nous ne l'avons pas testé.

Nous suggérons fortement d'utiliser un environnement virtuel Python pour éviter les conflits de modules.

```bash
python -m venv .env
source .env/bin/activate
[exécution de code Python]
[pour en sortir]
deactivate
```

Tous les scripts doivent être exécutés depuis le dossier racine, normalement `donnees-gestion`.

## Étapes à suivre

### Création de la structure

**À faire une seule fois! Des données seront supprimées!***

Le script `utils/creation.py` permet de (re)créer la structure de données (les tables) de l'entrepôt.

Avant de créer les tables, il les supprime si elles existent.

Vous pouvez contrôler quelles tables seront créées en modifiant la section `domaines` du fichier `_config.ini`. Seules les tables liées aux domaines ayant la valeur `TRUE` seront supprimées et recréées.

### Extraction des données

Les scripts du dossier `extraction` permettent d'extraire les données des fichiers ou systèmes sources. Pour savoir quel paramètres utiliser, simplement lancer le script sans arguments.

En général, les scripts ont besoin d'une date de début, d'une date de fin, et d'un fichier de sortie.

### Chargement des données

Les scripts du dossier `chargement` permettent de charger les données dans l'entrepôt. En général, les scripts ont besoin du nom du fichier à charger, obtenu de l'étape précédente.

### Transformation des données

Les scripts du dossier `transformation` permettent de transformer les données pour les rendre utilisables dans des rapports. La plupart n'ont pas besoin de paramètres.

À noter que ces scripts suppriment les données des tables temporaires liées au domaine, et ils envoient des courriels lorsque des erreurs de validation des données se produisent, ce qui stoppe leur exécution.

## Exécution quotidienne

La plupart des sources de données sont destinées à être chargées quotidiennement dans l'entrepôt.

Pour faciliter l'exécution des différentes étapes, le script `utils/pilote_quotidien.py` est proposé. Il travaille sur les données de la veille uniquement.

