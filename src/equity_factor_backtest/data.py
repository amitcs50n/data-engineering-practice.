from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Record:
    date: datetime
    ticker: str
    close: float
    sector: str
    book_to_market: float
    roe: float
    accruals: float


REQUIRED_COLUMNS = {
    "date",
    "ticker",
    "close",
    "sector",
    "book_to_market",
    "roe",
    "accruals",
}


def load_panel(path: str) -> list[Record]:
    p = Path(path)
    with p.open("r", newline="") as f:
        reader = csv.DictReader(f)
        cols = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - cols
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        out: list[Record] = []
        for row in reader:
            try:
                out.append(
                    Record(
                        date=datetime.strptime(row["date"], "%Y-%m-%d"),
                        ticker=row["ticker"],
                        close=float(row["close"]),
                        sector=row["sector"],
                        book_to_market=float(row["book_to_market"]),
                        roe=float(row["roe"]),
                        accruals=float(row["accruals"]),
                    )
                )
            except Exception:
                continue

    out.sort(key=lambda x: (x.ticker, x.date))
    return out
