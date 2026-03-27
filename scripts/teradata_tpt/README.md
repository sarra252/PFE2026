# Chargement Teradata avec TPT (FastLoad)

Ce dossier charge les CSV de E:\PFE2026\data_synth\raw vers Teradata avec TPT Load Operator.

## Fichiers utilises

- create_staging_tables.sql
- jobs\load_*.tpt
- run_all_tpt_simple.ps1
- run_tpt_wrapper_simple.ps1
- tpt.local.config.ps1
- twbcfg_local.ini

## Etapes

1. Executer create_staging_tables.sql sur la base cible.
2. Verifier/mettre a jour tpt.local.config.ps1 (TdpId, UserName, UserPassword, TargetDatabase, SourceDir).
3. Lancer:

```powershell
cd E:\PFE2026\scripts\teradata_tpt
.\run_tpt_wrapper_simple.ps1
```

## Notes

- twbcfg_local.ini force les dossiers logs/checkpoint locaux.
- En cas d'echec FastLoad, supprimer les tables techniques *_log, *_e1, *_e2 avant relance.
