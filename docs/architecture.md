# Architecture (MVP)

## Flux principal
1. Client envoie une requete API
2. Middleware genere request_id + mesure latence
3. Service metier (generation/documentation/optimisation)
4. Couche SQL safety valide lecture seule
5. API renvoie payload standard (status/data/warnings/errors)

## Composants
- FastAPI (transport HTTP)
- Mock LLM (generation SQL)
- SQL Safety Engine (validation et blocage)
- Logging standardise (request_id, status, duree)

## Extension prevue
- Branche connecteur Teradata (teradatasql/ODBC)
- Evaluation execution plan (EXPLAIN)
- Prompt routing selon domaine metier