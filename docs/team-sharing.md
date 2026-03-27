# Partage d'equipe: Git + Docker + DVC

Ce projet doit etre partage en separant clairement le code, l'environnement d'execution et les donnees.

## Ce qui est partage
- Le code applicatif via Git
- L'environnement local via Docker Compose
- Les jeux de donnees et metadonnees via DVC

## Ce qui reste local
- `.env`
- les cles API
- les fichiers Teradata locaux
- les logs

## Demarrage rapide pour un coequipier
1. Cloner le depot
2. Copier `.env.example` vers `.env`
3. Remplir les secrets dans `.env`
4. Recuperer les donnees avec `dvc pull`
5. Lancer `docker compose up --build`

## Initialisation DVC
Une fois DVC installe, lancer:

```powershell
.\scripts\setup_dvc.ps1 -RemoteUrl "<URL_DU_REMOTE_DVC>"
```

Exemples de remote DVC:
- dossier reseau partage
- bucket S3
- Azure Blob Storage
- Google Drive

## Services Docker
- API FastAPI: `http://localhost:8000`
- UI Streamlit: `http://localhost:8501`
- Qdrant optionnel avec le profil `rag-vector`

## Commandes utiles
```powershell
docker compose up --build
docker compose --profile rag-vector up --build
pytest -q
```
