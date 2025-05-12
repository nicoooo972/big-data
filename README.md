# Datamart-Scala

Un projet de data engineering utilisant Docker, Spark, Airflow, MinIO et PostgreSQL pour construire un datamart.

## Table des matières

- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Démarrage](#démarrage)
- [Structure du projet](#structure-du-projet)
- [Utilisation](#utilisation)
  - [Airflow](#airflow)
  - [Spark](#spark)
  - [MinIO](#minio)
  - [PostgreSQL](#postgresql)
- [Développement](#développement)
- [Contribuer](#contribuer)
- [Licence](#licence)

## Architecture

Ce projet vise à construire un Data Mart analytique à partir de données brutes (par exemple, les données des taxis de New York), en utilisant un pipeline de données moderne et conteneurisé. Il démontre les étapes clés de l'ingestion, du stockage dans un Data Lake, de la transformation, du chargement dans un Data Warehouse, et de la modélisation en Data Mart.

L'architecture s'appuie sur les composants suivants, orchestrés via Docker Compose :
- **MinIO** : Sert de Data Lake pour stocker les données brutes (ex: fichiers Parquet) et les données intermédiaires.
- **Apache Spark** : Utilisé pour les traitements de données à grande échelle, notamment :
    - L'ingestion de données depuis des sources locales/externes vers MinIO (cf. `spark_data_integration`).
    - La transformation et le chargement des données de MinIO vers le Data Warehouse (cf. `spark_data_integration_tp2`).
- **PostgreSQL** : Plusieurs instances sont utilisées :
    - `data-warehouse` : Stocke les données nettoyées et structurées (par exemple, la table `yellow_tripdata`).
    - `data-mart` : Héberge le Data Mart modélisé (par exemple, en schéma en étoile/flocon, cf. `sql_scripting`) pour l'analyse.
    - `postgres-airflow` : Sert de backend de métadonnées pour Airflow.
- **Apache Airflow** : Orchestre l'ensemble du pipeline ETL, depuis la collecte des données jusqu'au peuplement du Data Mart, à travers une série de DAGs (cf. `airflow/dags`).

Les services principaux incluent :

- **Airflow** : (scheduler, webserver, worker, triggerer, redis, postgres) pour l'orchestration des tâches.
- **Spark** : (master, workers) pour le traitement distribué des données.
- **MinIO** : Pour le stockage d'objets (fichiers bruts, données intermédiaires, etc.).
- **PostgreSQL** :
  - `postgres-airflow` : Base de données backend pour Airflow.
  - `data-warehouse` : Instance pour le data warehouse.
  - `data-mart` : Instance pour le data mart.

## Prérequis

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (généralement inclus avec Docker Desktop)
- Git (pour cloner le projet)
- Environnement Python (pour les scripts locaux ou l'installation de dépendances si nécessaire en dehors de Docker)

## Installation

1. **Clonez le dépôt :**

   ```bash
   git clone <URL_DU_DEPOT>
   cd nom-du-repertoire-du-projet
   ```
2. **Dépendances Python (optionnel, pour développement local) :**
   Si vous avez besoin d'installer les dépendances Python localement (par exemple, pour des outils de linting ou des tests en dehors de Docker), vous pouvez créer un environnement virtuel et installer les paquets :

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   Les dépendances principales sont gérées au sein des images Docker.

## Démarrage

1. **Configurer les variables d'environnement :**
   Avant de démarrer les services, il est crucial de configurer les variables d'environnement pour l'accès à S3 (MinIO) par les applications Spark. Créez un fichier `.env` à la racine de votre projet avec le contenu suivant, en remplaçant les valeurs par vos identifiants MinIO (par défaut `minio` et `minio123` comme indiqué dans la section MinIO plus bas) :

   ```env
   AWS_ACCESS_KEY_ID=minio
   AWS_SECRET_ACCESS_KEY=minio123
   ```
   Ce fichier `.env` sera automatiquement pris en compte par `docker-compose` pour injecter ces variables dans les services concernés (notamment les conteneurs Spark).

2. **Construire et démarrer les conteneurs Docker :**
   Depuis la racine du projet, lancez :

   ```bash
   docker-compose up --build -d
   ```

   L'option `--build` recompile les images si des modifications ont été apportées (par exemple au `Dockerfile`). L'option `-d` lance les conteneurs en mode détaché.
3. **Vérifier les services :**

   - Airflow Web UI : [http://localhost:8080](http://localhost:8080) (identifiants par défaut : `airflow`/`airflow` si non modifié dans la configuration Airflow)
   - Spark Master Web UI : [http://localhost:8081](http://localhost:8081)
   - MinIO Console : [http://localhost:9001](http://localhost:9001) (identifiants par défaut : `minio`/`minio123` comme indiqué dans votre `docker-compose.yml` ou la section MinIO ci-dessous)
4. **Arrêter les services :**
   Pour arrêter les conteneurs :

   ```bash
   docker-compose down
   ```

   Pour arrêter et supprimer les volumes (attention, cela supprimera les données persistantes comme celles de MinIO et PostgreSQL si les volumes ne sont pas configurés pour persister en dehors des conteneurs) :

   ```bash
   docker-compose down -v
   ```

## Structure du projet

```
.
├── airflow/                # Configuration, DAGs, et plugins Airflow
│   ├── dags/               # Contient les définitions des pipelines de données (workflows)
│   ├── logs/               # Stockage des logs générés par les tâches Airflow
│   ├── config/             # Fichiers de configuration Airflow
│   └── plugins/            # Plugins personnalisés pour Airflow (opérateurs, hooks)
├── data/                   # Données locales (ex: fichiers CSV/Parquet pour ingestion initiale)
├── docker/                 # Dockerfiles pour les images Spark personnalisées (master et worker)
├── minio/                  # Données persistantes pour MinIO (si montées localement)
├── spark_data_integration/ # Projet Scala/Spark pour l'ingestion (local vers S3) et Datalake vers DWH
├── sql_scripting/          # Scripts SQL pour la création du schéma et le peuplement du Data Mart
├── references/             # Contient la documentation du projet et autres références
├── .env                    # Fichier pour les variables d'environnement (à créer, non versionné)
├── .gitignore              # Fichiers et dossiers à ignorer par Git
├── docker-compose.yml      # Orchestre le déploiement des services via Docker Compose
├── Dockerfile              # Dockerfile de base pour l'image Airflow (si build personnalisé)
├── README.md               # Ce fichier
└── requirements.txt        # Dépendances Python (principalement pour le dev local ou certains opérateurs Airflow)
```

## Utilisation

### Airflow

- Accédez à l'interface utilisateur d'Airflow sur [http://localhost:8080](http://localhost:8080).
- Les DAGs se trouvent dans le répertoire `airflow/dags/`. Tout nouveau DAG ajouté ici sera automatiquement détecté par Airflow.

### Spark

- Le master Spark est accessible via l'URL `spark://spark-master:7077` depuis les autres conteneurs du même réseau Docker.
- L'interface utilisateur du master Spark est sur [http://localhost:8081](http://localhost:8081).
- Soumettez vos applications Spark en utilisant cette URL de master.

### MinIO

- Accédez à la console MinIO sur [http://localhost:9001](http://localhost:9001).
- Utilisez les identifiants configurés dans `docker-compose.yml` (par défaut `minio`/`minio123`).
- Configurez vos applications pour utiliser l'endpoint MinIO `http://minio:9000` depuis les autres conteneurs.

### PostgreSQL

Le `docker-compose.yml` définit plusieurs services PostgreSQL :

- **`postgres-airflow`** : Utilisé par Airflow. Port hôte `15433`.
- **`data-warehouse`** : Pour votre data warehouse. Port hôte `15432`.
- **`data-mart`** : Pour votre data mart. Port hôte `15435`.

Les identifiants par défaut sont `postgres`/`admin` pour `data-warehouse` et `data-mart`, et `airflow`/`airflow` pour `postgres-airflow`.
Vous pouvez vous y connecter en utilisant un client PSQL ou un outil d'interface graphique de base de données.
