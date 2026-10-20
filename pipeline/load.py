"""Étape 3 - Chargement (idempotent) dans SQLite (dev) ou PostgreSQL (prod)."""
import os

import pandas as pd
from sqlalchemy import DateTime, bindparam, create_engine, inspect, text

TABLE = "weather_hourly"


def load(csv_path: str, db_url: str, fail_on_load: bool = False) -> int:
    """Charge le CSV nettoyé. Renvoie le nombre de lignes insérées.

    fail_on_load=True simule une panne pour tester la reprise sur erreur.
    """
    if fail_on_load:
        raise RuntimeError("Echec volontaire de l'étape de chargement (fail_on_load=True)")

    if db_url.startswith("sqlite:///"):
        folder = os.path.dirname(db_url.replace("sqlite:///", ""))
        if folder:
            os.makedirs(folder, exist_ok=True)

    df = pd.read_csv(csv_path, parse_dates=["time"])
    engine = create_engine(db_url)
    with engine.begin() as conn:
        if inspect(conn).has_table(TABLE):
            # Idempotence : on remplace la plage déjà chargée pour cette ville
            conn.execute(
                text(f"DELETE FROM {TABLE} WHERE city = :c AND time BETWEEN :a AND :b")
                .bindparams(bindparam("a", type_=DateTime), bindparam("b", type_=DateTime)),
                {"c": df["city"].iloc[0], "a": df["time"].min().to_pydatetime(),
                 "b": df["time"].max().to_pydatetime()},
            )
        df.to_sql(TABLE, conn, if_exists="append", index=False, dtype={"time": DateTime})
    return len(df)
