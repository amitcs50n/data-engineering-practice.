from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import date, timedelta
from pathlib import Path

SECTORS = ["Tech", "Financials", "Healthcare", "Industrials", "Consumer"]


def _business_days(start: date, end: date):
    curr = start
    while curr <= end:
        if curr.weekday() < 5:
            yield curr
        curr += timedelta(days=1)


def make_data(output: str, n_tickers: int = 120) -> None:
    random.seed(7)
    start, end = date(2014, 1, 1), date(2025, 12, 31)
    dates = list(_business_days(start, end))

    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "ticker", "close", "sector", "book_to_market", "roe", "accruals"])

        for i in range(1, n_tickers + 1):
            ticker = f"STK{i:03d}"
            sector = SECTORS[(i - 1) % len(SECTORS)]

            drift = random.uniform(0.00005, 0.0003)
            vol = random.uniform(0.007, 0.02)
            px = 100.0

            btm_base = random.uniform(0.2, 1.1)
            roe_base = random.uniform(0.03, 0.24)
            accr_base = random.uniform(-0.02, 0.08)

            for d in dates:
                shock = random.gauss(drift, vol)
                px *= math.exp(shock)
                btm = btm_base + random.gauss(0.0, 0.05)
                roe = roe_base + random.gauss(0.0, 0.02)
                accr = accr_base + random.gauss(0.0, 0.015)
                w.writerow([
                    d.isoformat(),
                    ticker,
                    round(px, 6),
                    sector,
                    round(btm, 6),
                    round(roe, 6),
                    round(accr, 6),
                ])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output", default="data/synthetic_equities.csv")
    p.add_argument("--n-tickers", type=int, default=120)
    args = p.parse_args()

    make_data(args.output, args.n_tickers)
    print(f"Wrote synthetic data to {args.output}")


if __name__ == "__main__":
    main()
