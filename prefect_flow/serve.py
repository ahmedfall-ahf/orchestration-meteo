"""Planification : sert le flow avec un cron (tous les jours à 06:00 UTC).

Lancer :  python prefect_flow/serve.py
Les paramètres dev/prod se choisissent par déploiement.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from prefect import serve
from flow import meteo_pipeline

if __name__ == "__main__":
    dev = meteo_pipeline.to_deployment(
        name="meteo-dev", cron="0 6 * * *", parameters={"env": "dev"}
    )
    prod = meteo_pipeline.to_deployment(
        name="meteo-prod", cron="0 6 * * 1", parameters={"env": "prod"}
    )
    serve(dev, prod)
