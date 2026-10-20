"""Pipeline météo orchestré avec Airflow 3 (TaskFlow API).

Deux DAGs générés depuis la même définition, pour refléter les deux
environnements : meteo_dev (quotidien) et meteo_prod (hebdomadaire).

Simulation de panne : Variable Airflow "meteo_fail_on_load" = "true".
Relance partielle : corriger la Variable puis "Clear task" sur `load`
(avec ses tâches en aval) : extract et clean ne sont pas rejouées.
"""
from datetime import date, datetime, timedelta

from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task

from pipeline.config import ARCHIVE_LAG_DAYS, CITIES
from pipeline.extract import extract as _extract
from pipeline.clean import clean as _clean
from pipeline.load import load as _load
from pipeline.report import report as _report

DATA_DIR = "/opt/airflow/data"
# (villes, nombre de jours) par environnement - mêmes valeurs que pipeline/config.py
ENV_CONF = {"dev": (["Dakar"], 7), "prod": (list(CITIES), 365)}


def _db_url(env: str) -> str:
    """dev : SQLite dans le volume ; prod : Connection Airflow `meteo_db` (secret)."""
    if env == "dev":
        return f"sqlite:///{DATA_DIR}/dev.db"
    c = BaseHook.get_connection("meteo_db")
    return f"postgresql+psycopg2://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}"


def build_dag(env: str, schedule: str):
    @dag(
        dag_id=f"meteo_{env}",
        schedule=schedule,
        start_date=datetime(2026, 10, 1),
        catchup=False,
        params={"end_date": Param(None, type=["null", "string"],
                                  description="AAAA-MM-JJ, vide = J-7")},
        default_args={"retries": 1, "retry_delay": timedelta(seconds=5)},
        tags=["meteo", env],
    )
    def meteo():
        @task
        def get_period() -> dict:
            cities, days = ENV_CONF[env]
            end = get_current_context()["params"].get("end_date")
            stop = date.fromisoformat(end) if end else date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
            start = stop - timedelta(days=days - 1)
            return {"start": start.isoformat(), "end": stop.isoformat()}

        @task(retries=3, retry_delay=timedelta(seconds=5))
        def extract(city: str, period: dict) -> str:
            return _extract(city, period["start"], period["end"], DATA_DIR)

        @task
        def clean(raw_path: str) -> str:
            return _clean(raw_path, DATA_DIR)

        @task(retries=1, retry_delay=timedelta(seconds=5))
        def load(csv_path: str) -> int:
            fail = Variable.get("meteo_fail_on_load", default="false").lower() == "true"
            return _load(csv_path, _db_url(env), fail_on_load=fail)

        @task
        def report(loaded: list) -> str:
            return _report(_db_url(env), env, DATA_DIR)

        period = get_period()
        loaded = [load(clean(extract(c, period))) for c in ENV_CONF[env][0]]
        report(loaded)

    return meteo()


build_dag("dev", "0 6 * * *")
build_dag("prod", "0 6 * * 1")
