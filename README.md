# Teradata SQL Copilot (MVP)

Backend FastAPI pour generation, optimisation et documentation SQL orientee Teradata.

## Fonctionnalites
- POST /generate-sql
- POST /document-sql
- POST /optimize-sql
- POST /run-sql (offline DuckDB ou Teradata hebergee selon config)
- GET /db-health (verification rapide de la connexion DB)
- Format de reponse standard: request_id, status, data, warnings, errors
- Garde-fous SQL readonly (blocage DDL/DML)
- Logs avec correlation id et temps de traitement

## Installation locale
1. python -m venv .venv
2. .venv\\Scripts\\activate
3. pip install -r requirements.txt
4. copy .env.example .env
5. uvicorn backend.app.main:app --reload

## Lancement equipe avec Docker
1. copy .env.example .env
2. renseigner les variables locales
3. docker compose up --build
4. ouvrir `http://localhost:8000` pour l'API et `http://localhost:8501` pour l'UI

## Configuration execution SQL
- `DB_BACKEND=offline` (defaut): execution locale sur fichiers CSV/Parquet (`OFFLINE_DATA_DIR`).
- `DB_BACKEND=teradata`: execution sur Teradata hebergee.

Variables requises en mode Teradata:
- `TERADATA_HOST`
- `TERADATA_USER`
- `TERADATA_PASSWORD`
- `TERADATA_DATABASE` (optionnel)

## Configuration LLM online
- `LLM_MODE=openai`
- `OPENAI_API_KEY=<secret>`
- `OPENAI_MODEL=gpt-4.1-mini` (ou modele compatible)
- `OPENAI_BASE_URL=` (laisser vide pour OpenAI officiel, renseigner pour endpoint OpenAI-compatible)
- `LLM_TIMEOUT_S=30`
- `LLM_FALLBACK_TO_MOCK=true` (optionnel en dev)

Exemple test generation:
- `POST /generate-sql`
  - body: `{"question":"Top 10 clients par revenu mensuel"}`

## Auth
- Header obligatoire: x-api-key
- Valeur par defaut: changeme (modifiable dans .env)

## Exemples API
- GET /db-health
- POST /generate-sql
  - body: {"question": "Top 10 clients par chiffre d'affaires"}
- POST /document-sql
  - body: {"sql": "SELECT customer_id FROM sales.orders"}
- POST /optimize-sql
  - body: {"sql": "SELECT * FROM sales.orders"}

## Extraction metadata Teradata pour RAG
- Script: `scripts/extract_teradata_metadata.py`
- Exemple:
  - `python scripts/extract_teradata_metadata.py --database YOUR_DB --output-dir data_synth/metadata`
- Option multi-bases:
  - `python scripts/extract_teradata_metadata.py --databases DB1,DB2 --output-dir data_synth/metadata`

## Tests
- pytest -q

## Partage des donnees avec DVC
- Les dossiers `data_synth/raw` et `data_synth/metadata` doivent etre suivis via DVC
- Initialisation: `.\scripts\setup_dvc.ps1 -RemoteUrl "<URL_DU_REMOTE_DVC>"`
- Recuperation cote coequipier: `dvc pull`
- Envoi des mises a jour data: `dvc push`

## Fichiers a ne pas versionner
- `.env`
- `pfe key.txt`
- `scripts/teradata_tpt/tpt.local.config.ps1`
- `scripts/teradata_tpt/twbcfg_local.ini`
- `scripts/teradata_tpt/logs/`

## Guide equipe
- Voir `docs/team-sharing.md` pour le workflow Git + Docker + DVC

## Livrables projet
- docs/project-charter.md
- docs/backlog.md
- docs/architecture.md
- docs/runbook-demo.md
- prompts/generate_sql_v1.md
- prompts/generate_sql_v2.md
- postman/teradata_sql_copilot_collection.json
- postman/teradata_sql_copilot_environment.json

## Limites MVP
- Pas d'ecriture SQL sur serveur (read-only uniquement)
- Optimisation basee sur heuristiques (pas d'EXPLAIN)
