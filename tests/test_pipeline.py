import json

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from pipeline import extract as extract_mod
from pipeline.clean import clean
from pipeline.config import get_settings
from pipeline.load import load
from pipeline.report import report


def fake_payload(city="Dakar"):
    return {
        "city": city,
        "hourly": {
            "time": ["2026-10-01T00:00", "2026-10-01T01:00", "2026-10-01T01:00",
                     "2026-10-01T02:00", "2026-10-01T03:00"],
            "temperature_2m": [27.0, 26.5, 26.5, 999.0, None],
            "precipitation": [0.0, 0.2, 0.2, -3.0, None],
            "wind_speed_10m": [12.0, 10.0, 10.0, 11.0, None],
        },
    }


@pytest.fixture
def raw_file(tmp_path):
    p = tmp_path / "raw.json"
    p.write_text(json.dumps(fake_payload()))
    return str(p)


def test_clean_removes_duplicates_outliers_and_empty_rows(raw_file, tmp_path):
    out = clean(raw_file, str(tmp_path))
    df = pd.read_csv(out)
    assert len(df) == 3                              # doublon et ligne vide retirés
    assert df["temperature_c"].isna().sum() == 1     # 999 -> manquant
    assert df["precipitation_mm"].isna().sum() == 1  # -3 -> manquant
    assert set(df.columns) >= {"city", "time", "temperature_c", "precipitation_mm", "wind_kmh"}


def test_load_is_idempotent(raw_file, tmp_path):
    csv = clean(raw_file, str(tmp_path))
    url = f"sqlite:///{tmp_path}/t.db"
    load(csv, url)
    load(csv, url)
    with create_engine(url).connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM weather_hourly")).scalar() == 3


def test_load_fail_on_load(raw_file, tmp_path):
    csv = clean(raw_file, str(tmp_path))
    with pytest.raises(RuntimeError):
        load(csv, f"sqlite:///{tmp_path}/t.db", fail_on_load=True)


def test_report_generated(raw_file, tmp_path):
    csv = clean(raw_file, str(tmp_path))
    url = f"sqlite:///{tmp_path}/t.db"
    load(csv, url)
    out = report(url, "test", str(tmp_path))
    assert "Dakar" in open(out, encoding="utf-8").read()


def test_extract_writes_raw_json(monkeypatch, tmp_path):
    class R:
        def raise_for_status(self): pass
        def json(self): return {"hourly": fake_payload()["hourly"]}
    monkeypatch.setattr(extract_mod.requests, "get", lambda *a, **k: R())
    path = extract_mod.extract("Dakar", "2026-10-01", "2026-10-07", str(tmp_path))
    assert json.load(open(path))["city"] == "Dakar"


def test_settings_dev_and_prod(monkeypatch):
    assert get_settings("dev").cities == ("Dakar",)
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "d")
    s = get_settings("prod")
    assert len(s.cities) == 5 and s.days == 365 and s.db_url.startswith("postgresql")
