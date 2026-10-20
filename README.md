# Orchestration de pipelines : Airflow vs Prefect (pipeline météo Open-Meteo)

Projet de veille technologique - Sujet 10. Le même pipeline
**extraction -> nettoyage -> chargement -> rapport** est implémenté en Python pur,
puis avec Prefect et Airflow.

## Structure

```
pipeline/        code métier commun (extract, clean, load, report, config)
tests/           tests pytest (nettoyage, idempotence, panne simulée...)
run_local.py     exécution en Python pur (référence)
docker-compose.yml   PostgreSQL (base "prod")
prefect/ airflow/    (à venir)
```

## Démarrage (Windows, PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest -q                          # 6 tests doivent passer

# Environnement dev : SQLite, 1 ville, 7 jours
python run_local.py --env dev
# Panne simulée à l'étape de chargement
python run_local.py --env dev --fail-on-load
```

Le rapport est généré dans `data/reports/rapport_dev.html`.

## Environnement prod (PostgreSQL)

```powershell
Copy-Item .env.example .env        # puis éditer POSTGRES_PASSWORD
docker compose up -d
# Charger les variables du .env dans la session PowerShell :
Get-Content .env | Where-Object { $_ -match '^[^#].+=' } | ForEach-Object {
  $k,$v = $_ -split '=',2; Set-Item -Path "Env:$k" -Value $v }
python run_local.py --env prod     # 5 villes, 365 jours
```

## Choix de conception

- **Idempotence** : le chargement remplace la plage déjà présente pour la ville,
  donc une relance ne crée pas de doublons.
- **Étapes passant des chemins de fichiers** (et non des DataFrames) : adapté
  aux deux orchestrateurs et permet de relancer une seule étape.
- **Secrets** : mot de passe PostgreSQL uniquement via variables d'environnement
  (`.env` non versionné).
- **Période** : l'API d'archive Open-Meteo a quelques jours de latence ; la
  période se termine à J-7.
