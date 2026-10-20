"""Étape 2 - Nettoyage avec pandas."""
import json
import os

import pandas as pd

# Bornes physiques plausibles ; hors bornes -> valeur manquante
BOUNDS = {
    "temperature_c": (-90, 60),
    "precipitation_mm": (0, 500),
    "wind_kmh": (0, 400),
}
RENAME = {
    "temperature_2m": "temperature_c",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_kmh",
}


def clean(raw_path: str, data_dir: str = "data") -> str:
    """Lit le JSON brut, nettoie, écrit un CSV et renvoie son chemin."""
    with open(raw_path, encoding="utf-8") as f:
        payload = json.load(f)

    df = pd.DataFrame(payload["hourly"]).rename(columns=RENAME)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).drop_duplicates(subset=["time"])

    for col, (lo, hi) in BOUNDS.items():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[(df[col] < lo) | (df[col] > hi), col] = float("nan")

    # On retire les lignes sans aucune mesure
    df = df.dropna(subset=list(BOUNDS), how="all")
    df.insert(0, "city", payload["city"])
    df = df.sort_values("time").reset_index(drop=True)

    out_dir = os.path.join(data_dir, "clean")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, os.path.basename(raw_path).replace(".json", ".csv"))
    df.to_csv(out, index=False)
    return out
