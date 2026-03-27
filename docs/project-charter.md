# Project Charter - SQL Copilot Teradata

## Vision
Construire un chatbot backend capable de generer, optimiser et documenter des requetes SQL Teradata de maniere fiable et securisee.

## Utilisateurs cibles
- Etudiants et analysts SQL
- Equipes data ayant besoin d'un assistant de productivite

## Entrees / Sorties
- Entree: question metier (langage naturel) ou SQL existant
- Sortie:
  - SQL genere
  - explication concise
  - score de confiance
  - warnings de securite/performance

## Definition of Done (Sprint 2 semaines)
- API FastAPI avec endpoints: /generate-sql, /document-sql, /optimize-sql
- Format de reponse standardise avec request_id
- Validation SQL lecture seule + blocage des mots-cles interdits
- Tests unitaires de base verts
- Collection Postman pour demonstration

## Hors scope MVP
- Connexion active a un cluster Teradata reel
- Execution SQL serveur
- Gestion multi-utilisateur/roles avances