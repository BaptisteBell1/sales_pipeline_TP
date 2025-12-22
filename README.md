# 🎯 TP2 --- Industrialisation du traitement Spark : projet Python packagé

------------------------------------------------------------------------

## 🧱 Objectif du TP2

L'objectif de cette deuxième partie est d'**industrialiser** votre
traitement en le transformant en un **projet Python structuré et
packagé**, pouvant être :
- exécuté automatiquement,
- versionné, testé et maintenu dans le temps.

Autrement dit, vous passez du **prototype (notebook)** à une
**application de traitement de données** réutilisable et déployable.

------------------------------------------------------------------------

## 1. 🗂️ Structure du projet Python

Créez un projet suivant une structure standard, par exemple :

    sales_pipeline/
    │
    ├── sales_pipeline/           # Code source du projet
    │   ├── __init__.py
    │   ├── config/
    │   │    └── config.yaml
    │   ├── bronze/
    │   │    └── ingestion.py
    │   ├── silver/
    │   │    └── cleaning.py
    │   ├── gold/
    │   │    └── aggregation.py
    │   └── utils/
    │        └── spark_session.py
    │        └── utils.py
    ├── tests/                    # Tests unitaires
    │   └── test_cleaning.py
    │
    ├── main.py                   # Point d’entrée du traitement
    ├── pyproject.toml            # Fichier de configuration utilisé par les outils de packaging
    ├── requirements.txt          # Dépendances du projet
    └── README.md                 # Documentation du projet

------------------------------------------------------------------------

## 🚀 Utilisation du projet

### 📦 Installer les dépendances

Installer toutes les dépendances listées dans le fichier `requirements.txt` :

```bash
pip install -r requirements.txt
```

### ▶️ Lancer le programme principal
Pour exécuter le fichier principal :
```bash
python main.py
````

### 🧪 Lancer les tests

Les tests sont situés dans le dossier `tests/`.
Pour les exécuter avec **pytest** :

```bash
pytest tests/
```

------------------------------------------------------------------------

## 🧩 Livrables attendus

-   Le **projet Python complet** (code + arborescence)
-   Le **fichier `README.md`** avec instructions d'exécution
-   Le **fichier `requirements.txt`**
-   Un **notebook Databricks** de test pour appeler votre
    projet Python packagé

------------------------------------------------------------------------