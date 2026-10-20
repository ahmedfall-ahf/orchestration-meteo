"""Configuration du pipeline par environnement (dev / prod).

Les paramètres non secrets sont définis ici ; les secrets (mot de passe
PostgreSQL) viennent des variables d'environnement.
"""
import os
from dotenv import load_dotenv
from dataclasses import dataclass
from datetime import date, timedelta

load_dotenv()

CITIES = {
    "Dakar": (14.6937, -17.4441),
    "Paris": (48.8566, 2.3522),
    "Casablanca": (33.5731, -7.5898),
    "Abidjan": (5.3600, -4.0083),
    "Lyon": (45.7640, 4.8357),
}

# L'API d'archive a quelques jours de latence : on s'arrête à J-7.
ARCHIVE_LAG_DAYS = 7


@dataclass(frozen=True)
class Settings:
    env: str
    cities: tuple
    days: int
    db_url: str
    data_dir: str = "data"

    def period(self, end: date | None = None):
        end = end or (date.today() - timedelta(days=ARCHIVE_LAG_DAYS))
        start = end - timedelta(days=self.days - 1)
        return start.isoformat(), end.isoformat()


def get_settings(env: str | None = None) -> Settings:
    env = env or os.getenv("PIPELINE_ENV", "dev")
    if env == "dev":
        return Settings("dev", ("Dakar",), 7, "sqlite:///data/dev.db")
    if env == "prod":
        user = os.environ["POSTGRES_USER"]
        pwd = os.environ["POSTGRES_PASSWORD"]
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.environ["POSTGRES_DB"]
        url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
        return Settings("prod", tuple(CITIES), 365, url)
    raise ValueError(f"Environnement inconnu : {env}")
