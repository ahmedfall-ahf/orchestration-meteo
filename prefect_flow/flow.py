"""Pipeline météo orchestré avec Prefect 3.

- Chaque étape est une @task avec retries (reprise automatique).
- cache_policy=INPUTS + persist_result : une relance avec les mêmes paramètres
  ne ré-exécute PAS les tâches déjà réussies (extract, clean) ; seule la tâche
  en échec (load) est rejouée.
"""
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from prefect.cache_policies import INPUTS

from pipeline.config import get_settings
from pipeline.extract import extract as _extract
from pipeline.clean import clean as _clean
from pipeline.load import load as _load
from pipeline.report import report as _report

CACHE = dict(cache_policy=INPUTS, persist_result=True, cache_expiration=timedelta(hours=6))


@task(name="extract", retries=3, retry_delay_seconds=[5, 15, 30], **CACHE)
def extract_task(city: str, start: str, end: str, data_dir: str) -> str:
    get_run_logger().info(f"Extraction {city} {start} -> {end}")
    return _extract(city, start, end, data_dir)


@task(name="clean", **CACHE)
def clean_task(raw_path: str, data_dir: str) -> str:
    return _clean(raw_path, data_dir)


@task(name="load", retries=1, retry_delay_seconds=5)
def load_task(csv_path: str, db_url: str, fail_on_load: bool) -> int:
    n = _load(csv_path, db_url, fail_on_load)
    get_run_logger().info(f"{n} lignes chargées")
    return n


@task(name="report")
def report_task(db_url: str, env: str, data_dir: str, _after: list) -> str:
    return _report(db_url, env, data_dir)


@flow(name="meteo-pipeline", log_prints=True)
def meteo_pipeline(env: str = "dev", fail_on_load: bool = False, end_date: str | None = None):
    s = get_settings(env)
    from datetime import date
    start, end = s.period(date.fromisoformat(end_date) if end_date else None)

    loaded = []
    for city in s.cities:
        raw = extract_task(city, start, end, s.data_dir)
        csv = clean_task(raw, s.data_dir)
        loaded.append(load_task(csv, s.db_url, fail_on_load))
    return report_task(s.db_url, s.env, s.data_dir, loaded)


if __name__ == "__main__":
    # Exécution directe : python prefect_flow/flow.py
    meteo_pipeline()
