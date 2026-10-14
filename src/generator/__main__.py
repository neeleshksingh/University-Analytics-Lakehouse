"""
Synthetic data generator for the University Analytics Lakehouse.

    python -m src.generator seed                  # 4+ years of history (scale 1.0, ~5M attendance rows)
    python -m src.generator seed --scale 0.1      # small, fast version for testing
    python -m src.generator delta                 # simulate the next day
    python -m src.generator delta --days 5        # simulate the next 5 days
    python -m src.generator delta --date 2026-10-05

Start from an empty database:  python -m src.source_db.create_schema --reset
"""

import argparse
import os
from datetime import date, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.generator.config import GeneratorConfig
from src.generator.delta import last_simulated_day, run_delta
from src.generator.seed import run_seed


def main():
    parser = argparse.ArgumentParser(prog="python -m src.generator")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_seed = sub.add_parser("seed", help="load the full history into an empty database")
    p_seed.add_argument("--scale", type=float, default=1.0)
    p_seed.add_argument("--seed", type=int, default=42)
    p_seed.add_argument("--until", type=date.fromisoformat, default=GeneratorConfig.seed_until)

    p_delta = sub.add_parser("delta", help="simulate one or more days after the existing data")
    p_delta.add_argument("--days", type=int, default=1)
    p_delta.add_argument("--date", type=date.fromisoformat, help="first day to simulate")
    p_delta.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()
    load_dotenv()
    url = os.environ["SOURCE_DB_URL"]
    engine = create_engine(url, fast_executemany=True) if url.startswith("mssql") else create_engine(url)

    if args.mode == "seed":
        cfg = GeneratorConfig(seed=args.seed, scale=args.scale, seed_until=args.until)
        print(f"Seeding: scale={cfg.scale}, seed={cfg.seed}, history up to {cfg.seed_until}")
        run_seed(engine, cfg)
    else:
        cfg = GeneratorConfig(seed=args.seed)
        with engine.connect() as conn:
            first = args.date or last_simulated_day(conn) + timedelta(days=1)
        for n in range(args.days):
            run_delta(engine, cfg, first + timedelta(days=n))


if __name__ == "__main__":
    main()
