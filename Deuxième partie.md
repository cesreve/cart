### Suivi et monitoring de la qualité des Données JSON

#### Liste des KPIs à surveiller:

*   % des .json conformes au schéma prédéfini
*   % des fichiers qui vérifient les contraintes de non-duplication et d'unicité des clés (ex: `event_id`, `shopper_id`)
*   % des fichiers avec une cohérence des données (ex: `total_transaction_amount` correspond à la somme des `total_price` des produits))
*   Vérification du typage des dates (ex: `event_timestamp`)

#### Stratégie:

*   **Validation** des données dès réception à l'aide des KPIs définis
*   **Monitorer** l'évolution des KPIs définis à l'aide de tableaux de bords
*   **Mettre en place** un système d'alerting si la qualité des données se dégrade
*   **Enregistrer** les données dans des tables de rejets
*   **Utiliser** *dbt* pour la mise en place des tests
*   **Documenter** la stratégie et les règles de qualité de données
*   **Collaboration** avec les fournisseurs de données et le business


#### Considérations:

*   **Adapter** en fonction du volume, de la vélocité et de la criticité des données reçues.
*   **Prioriser** et définir les KPIs en fonction de leur impact sur l'analyse des données et les processus métiers.
*   **Automatiser** le système d'alerte et de monitoring de la qualité des données.