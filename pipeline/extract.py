"""Étape 1 - Extraction : appel Open-Meteo, sauvegarde du JSON brut."""
import json
import os

import requests

from .config import CITIES

URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = "temperature_2m,precipitation,wind_speed_10m"


def extract(city: str, start: str, end: str, data_dir: str = "data") -> str:
    """Télécharge les données horaires et renvoie le chemin du JSON brut."""
    lat, lon = CITIES[city]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": HOURLY,
        "timezone": "UTC",
    }
    resp = requests.get(URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    payload["city"] = city

    raw_dir = os.path.join(data_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    path = os.path.join(raw_dir, f"{city}_{start}_{end}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return path
