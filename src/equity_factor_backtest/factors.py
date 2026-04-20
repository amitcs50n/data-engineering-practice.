from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import mean, pstdev

from .data import Record


@dataclass
class MonthlyRecord:
    date: datetime
    ticker: str
    sector: str
    close: float
    ret_1m: float | None
    value_score: float
    momentum_score: float
    quality_score: float
    combined_score: float


def _month_key(dt: datetime) -> tuple[int, int]:
    return (dt.year, dt.month)


def _month_end(y: int, m: int) -> datetime:
    if m == 12:
        return datetime(y, 12, 31)
    from datetime import timedelta

    return datetime(y, m + 1, 1) - timedelta(days=1)


def _zscore(values: dict[str, float | None]) -> dict[str, float]:
    valid = [v for v in values.values() if v is not None]
    if not valid:
        return {k: 0.0 for k in values}
    mu = mean(valid)
    sigma = pstdev(valid)
    if sigma == 0:
        return {k: 0.0 for k in values}
    return {k: (0.0 if v is None else (v - mu) / sigma) for k, v in values.items()}


def compute_monthly_panel(records: list[Record]) -> list[MonthlyRecord]:
    by_ticker_month: dict[tuple[str, tuple[int, int]], Record] = {}
    for r in records:
        by_ticker_month[(r.ticker, _month_key(r.date))] = r  # keep month-end-ish last seen due sorted input

    ticker_months: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for ticker, month in by_ticker_month:
        ticker_months[ticker].append(month)
    for t in ticker_months:
        ticker_months[t] = sorted(set(ticker_months[t]))

    # raw scores indexed by date-end then ticker
    month_ticker_data: dict[datetime, dict[str, dict[str, float | None | str]]] = defaultdict(dict)

    for ticker, months in ticker_months.items():
        closes: list[float] = []
        month_ends: list[datetime] = []
        month_recs: list[Record] = []
        for ym in months:
            rec = by_ticker_month[(ticker, ym)]
            closes.append(rec.close)
            month_ends.append(_month_end(*ym))
            month_recs.append(rec)

        for i, rec in enumerate(month_recs):
            dt = month_ends[i]
            ret_1m = None if i == 0 else (closes[i] / closes[i - 1] - 1.0)
            mom = None if i < 12 else (closes[i - 1] / closes[i - 12] - 1.0)
            month_ticker_data[dt][ticker] = {
                "sector": rec.sector,
                "close": rec.close,
                "ret_1m": ret_1m,
                "value_raw": rec.book_to_market,
                "momentum_raw": mom,
                "quality_raw": rec.roe - rec.accruals,
            }

    out: list[MonthlyRecord] = []
    for dt in sorted(month_ticker_data):
        snap = month_ticker_data[dt]
        value_scores = _zscore({t: float(v["value_raw"]) for t, v in snap.items()})
        momentum_scores = _zscore({t: v["momentum_raw"] for t, v in snap.items()})
        quality_scores = _zscore({t: float(v["quality_raw"]) for t, v in snap.items()})

        for t, v in snap.items():
            combined = (value_scores[t] + momentum_scores[t] + quality_scores[t]) / 3.0
            out.append(
                MonthlyRecord(
                    date=dt,
                    ticker=t,
                    sector=str(v["sector"]),
                    close=float(v["close"]),
                    ret_1m=v["ret_1m"],
                    value_score=value_scores[t],
                    momentum_score=momentum_scores[t],
                    quality_score=quality_scores[t],
                    combined_score=combined,
                )
            )

    out.sort(key=lambda x: (x.date, x.ticker))
    return out
