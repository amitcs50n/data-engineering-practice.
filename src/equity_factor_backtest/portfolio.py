from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from .factors import MonthlyRecord


@dataclass
class WeightRecord:
    date: datetime
    ticker: str
    sector: str
    weight: float


def build_weights(
    monthly: list[MonthlyRecord],
    mode: str = "long_short",
    top_quantile: float = 0.2,
    bottom_quantile: float = 0.2,
) -> list[WeightRecord]:
    mode = mode.lower()
    if mode not in {"long_only", "long_short"}:
        raise ValueError("mode must be 'long_only' or 'long_short'")

    by_date: dict[datetime, list[MonthlyRecord]] = defaultdict(list)
    for r in monthly:
        by_date[r.date].append(r)

    out: list[WeightRecord] = []
    for dt in sorted(by_date):
        rows = by_date[dt]
        sorted_rows = sorted(rows, key=lambda x: x.combined_score)
        n = len(sorted_rows)
        n_long = max(1, int(round(n * top_quantile)))
        n_short = max(1, int(round(n * bottom_quantile)))

        long_names = {r.ticker for r in sorted_rows[-n_long:]}
        short_names = {r.ticker for r in sorted_rows[:n_short]}

        for r in rows:
            w = 0.0
            if r.ticker in long_names:
                w += 1.0 / n_long
            if mode == "long_short" and r.ticker in short_names:
                w -= 1.0 / n_short
            out.append(WeightRecord(date=dt, ticker=r.ticker, sector=r.sector, weight=w))

    out.sort(key=lambda x: (x.date, x.ticker))
    return out


def backtest(
    weights: list[WeightRecord], monthly: list[MonthlyRecord]
) -> tuple[list[tuple[datetime, float]], list[tuple[datetime, float]], list[tuple[datetime, str, float]]]:
    by_ticker = defaultdict(list)
    for r in monthly:
        by_ticker[r.ticker].append(r)
    for t in by_ticker:
        by_ticker[t].sort(key=lambda x: x.date)

    next_ret_lookup: dict[tuple[datetime, str], float] = {}
    for t, rows in by_ticker.items():
        for i, r in enumerate(rows[:-1]):
            nxt = rows[i + 1].ret_1m
            next_ret_lookup[(r.date, t)] = 0.0 if nxt is None else float(nxt)

    by_date_weight = defaultdict(list)
    for w in weights:
        by_date_weight[w.date].append(w)

    monthly_returns: list[tuple[datetime, float]] = []
    sector_expo: list[tuple[datetime, str, float]] = []

    # turnover
    all_tickers = sorted({w.ticker for w in weights})
    prev = {t: 0.0 for t in all_tickers}
    turnover: list[tuple[datetime, float]] = []

    for dt in sorted(by_date_weight):
        rows = by_date_weight[dt]
        rsum = 0.0
        sec = defaultdict(float)
        curr = {t: 0.0 for t in all_tickers}

        for w in rows:
            rsum += w.weight * next_ret_lookup.get((dt, w.ticker), 0.0)
            sec[w.sector] += w.weight
            curr[w.ticker] = w.weight

        monthly_returns.append((dt, rsum))
        for s, sw in sec.items():
            sector_expo.append((dt, s, sw))

        to = sum(abs(curr[t] - prev[t]) for t in all_tickers)
        turnover.append((dt, to))
        prev = curr

    return monthly_returns, turnover, sector_expo
