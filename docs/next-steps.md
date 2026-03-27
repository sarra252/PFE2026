# Limites actuelles et Next Steps

## Limites actuelles
- Pas de connexion Teradata reelle (mode mock uniquement)
- Pas d'execution SQL sur une base distante
- Optimisation heuristique sans EXPLAIN plan
- Prompt engineering initial, sans apprentissage par feedback utilisateur

## Next Steps (phase 2)
1. Brancher teradatasql/ODBC sur environnement de test
2. Ajouter module EXPLAIN + recommandations basees plan d'execution
3. Ajouter scoring qualite automatique sur jeux de requetes
4. Ajouter authentification robuste (JWT) et gestion roles
5. Ajouter UI web legere pour demo soutenance