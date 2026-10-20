"""Test du flow Prefect (API simulée) : panne sur load puis relance partielle."""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "prefect_flow"))


def payload(city):
    return {"city": city, "hourly": {
        "time": ["2026-10-01T00:00", "2026-10-01T01:00"],
        "temperature_2m": [27.0, 26.0], "precipitation": [0.0, 0.1],
        "wind_speed_10m": [10.0, 11.0]}}


def test_failed_load_then_partial_rerun(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PREFECT_HOME", str(tmp_path / ".prefect"))
    import flow as f
    calls = {"extract": 0}

    def fake_extract(city, start, end, data_dir):
        calls["extract"] += 1
        os.makedirs(data_dir, exist_ok=True)
        p = os.path.join(data_dir, f"{city}.json")
        json.dump(payload(city), open(p, "w"))
        return p

    monkeypatch.setattr(f, "_extract", fake_extract)

    with pytest.raises(Exception):
        f.meteo_pipeline(env="dev", fail_on_load=True, end_date="2026-10-01")
    assert calls["extract"] == 1

    f.meteo_pipeline(env="dev", fail_on_load=False, end_date="2026-10-01")
    assert calls["extract"] == 1   # extract non rejoué : résultat en cache
    assert os.path.exists("data/reports/rapport_dev.html")
