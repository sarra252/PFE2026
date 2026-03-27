# Runbook Demo (2 passages)

## Pre-requis
- Python 3.11+
- pip install -r requirements.txt
- Copier .env.example vers .env puis definir API_KEY

## Lancement
- uvicorn backend.app.main:app --reload

## Sequence demo
1. POST /generate-sql avec question Top 10 clients
2. POST /document-sql avec SQL de sortie
3. POST /optimize-sql avec requete contenant SELECT *
4. Montrer headers: X-Request-ID et X-Process-Time-Ms
5. Montrer erreur controlee (mauvaise API key ou SQL interdit)