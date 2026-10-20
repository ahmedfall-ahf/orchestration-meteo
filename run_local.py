"""Exécution du pipeline en Python pur (référence avant orchestration).

Exemples :
    python run_local.py --env dev
    python run_local.py --env dev --fail-on-load
"""
import argparse
import time

from pipeline.config import get_settings
from pipeline.extract import extract
from pipeline.clean import clean
from pipeline.load import load
from pipeline.report import report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None, choices=["dev", "prod"])
    ap.add_argument("--fail-on-load", action="store_true")
    args = ap.parse_args()

    s = get_settings(args.env)
    start, end = s.period()
    t0 = time.time()
    for city in s.cities:
        raw = extract(city, start, end, s.data_dir)
        csv = clean(raw, s.data_dir)
        n = load(csv, s.db_url, args.fail_on_load)
        print(f"[{city}] {n} lignes chargées")
    print("Rapport :", report(s.db_url, s.env, s.data_dir))
    print(f"Durée totale : {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
