# Manipulation des données de gestion

Ce dépôt contient différents script pour extraire et manipuler les données de gestion des bibliothèques de l'UdeM.

## Généralités

### Connexion à la base de données

Un fichier de configuration `config/_db.ini` doit contenir les informations de connexion à la base de données. Il n'est pas dans l'entrepôt Git pour ne pas exposer ces informations.

Voici un exemple de fichier:

```ini
[database]
dbname = entrepot
user = usager pour la base de données
password = mot de passe de la base de données
host = localhost
port = 5432
```

### Journalisation

Les différents scripts utlisent un journal (logs) commun.

C'est le fichier `config/_logs.ini` qui contient les informations, et il doit avoir ces informations:

```ini
[logs]
fichier = ../logs/entrepot.log
niveau = INFO
```

Il n'est pas dans l'entrepôt Git pour permettre de modifier ces informations selon l'environnement d'exécution.

## Création des tables SQL

Le script `utils/creation.py` contient le code et les requêtes pour créer les structures de données dans la base de données.

Puisque ce script peut également supprimer des tables, il est nécessaire de l'utiliser avec précaution.

Pour contrôler ce qui est créé et supprimé, il utilise un fichier de configuration nommé `utils/_domaines.ini`. Ce fichier doit contenir les informations suivantes:

```ini
[domaines]
reservations = TRUE
```

Pour chaque domaine de données, si la valeur est TRUE, les tables seront supprimées et recréées. S'il est à FALSE, elles ne seront pas touchées.

Une fois ce fichier créé, et les informations de connexion à la base de données bien inscrites dans `config/_db.ini`, on peut exécuter le script sans paramètres:

```bash
python creation.py
```

## Extraction

Le dossier `extraction` contient les différents scripts
qui permettent d'extraire des données de nos systèmes.

Pour l'instant, seules les réservations dans LibCal sont scriptées.
