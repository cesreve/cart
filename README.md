# Projet *dbt* data engineer

## Fonctionnalités Clés:

* **Première partie:** Réalise les requêtes SQL pour générer les 4 tables demandées `transactions`, `add_to_basket`, `shopper_stats` et `daily_global_stats`
* **Deuxième partie:** Se référer au document `Deuxième partie.md` pour une description détaillée de la stratégie de suivi et du monitoring de la qualité des données.
* **Tests dbt:** Utilise les capacités de test de dbt pour appliquer les règles et les contraintes de qualité des données (test générique dans `schema.yml` et test spécifique dans `tests.yml`)

## Prérequis techniques:

Ce projet utilise le gestionnaire de packages uv. Assurez-vous qu'uv est installé avant d'exécuter le projet.

## Démarrage:

1. Installez les dépendances à l'aide d'uv.
2. Exécutez le projet dbt à l'aide des commandes suivantes:
    * `uv run dbt run` pour exécuter les transformations de données.
    * `uv run dbt test` pour exécuter les tests de qualité des données.

## Structure du projet:

1. Le projet suit une structure dbt standard, avec des modèles et des tests
2. Les données arrivent dans le répertoire `landing`. Le fichier `data1.json` correspond aux données fournies pour l'exercice. 
3. Le répertoire `dbtcart` contient la configuration et le code du projet *dbt*. Le fichier data1.json correspond aux données fournies pour l'exercice. Les autres données ont été ajoutées pour faire des tests.
4. Les requetes utilisées pour créer les tables demandées sont dans les répertoires src, fct et dim:
    * Le répertoire `src` copie directement les données brutes depuis le fichier .json dans *duckdb*
    * Le répertoire `fct` expose la table de faits
    * Le répertoire `dim` expose les requetes SQL demandées.
5. La deuxième partie relative concernant le suivi et le monitoring de la qualité de données JSON se trouve dans le document `Deuxième partie.md`.