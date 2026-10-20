"""Étape 4 - Rapport HTML (statistiques par ville + graphique)."""
import base64
import io
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

from .load import TABLE


def report(db_url: str, env: str, data_dir: str = "data") -> str:
    engine = create_engine(db_url)
    df = pd.read_sql(f"SELECT * FROM {TABLE}", engine, parse_dates=["time"])
    if df.empty:
        raise ValueError("Aucune donnée en base : rapport impossible")

    stats = df.groupby("city").agg(
        lignes=("time", "count"),
        temp_moy=("temperature_c", "mean"),
        temp_min=("temperature_c", "min"),
        temp_max=("temperature_c", "max"),
        pluie_totale_mm=("precipitation_mm", "sum"),
        vent_moy_kmh=("wind_kmh", "mean"),
    ).round(1)

    fig, ax = plt.subplots(figsize=(8, 3.5))
    for city, g in df.groupby("city"):
        daily = g.set_index("time")["temperature_c"].resample("D").mean()
        ax.plot(daily.index, daily.values, label=city)
    ax.set_ylabel("Température moyenne (°C)")
    ax.legend()
    fig.autofmt_xdate()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    img = base64.b64encode(buf.getvalue()).decode()

    html = f"""<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8">
<title>Rapport météo ({env})</title>
<style>body{{font-family:sans-serif;margin:2rem}}table{{border-collapse:collapse}}
td,th{{border:1px solid #ccc;padding:4px 10px}}</style></head><body>
<h1>Rapport météo - environnement {env}</h1>
<p>Période : {df['time'].min():%Y-%m-%d} au {df['time'].max():%Y-%m-%d}</p>
{stats.to_html()}
<img src="data:image/png;base64,{img}" alt="Températures">
</body></html>"""

    out_dir = os.path.join(data_dir, "reports")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"rapport_{env}.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    return out
