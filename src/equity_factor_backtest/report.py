from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable


def _write_csv(path: Path, headers: list[str], rows: Iterable[list[object]]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)


def write_report(
    output_dir: str,
    perf: dict[str, float],
    monthly_returns: list[tuple],
    turnover: list[tuple],
    sector_expo: list[tuple],
) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    _write_csv(
        out / "performance_summary.csv",
        ["cagr", "sharpe", "max_drawdown", "months"],
        [[perf["cagr"], perf["sharpe"], perf["max_drawdown"], perf["months"]]],
    )
    _write_csv(out / "monthly_returns.csv", ["date", "portfolio_return"], [[d.date().isoformat(), r] for d, r in monthly_returns])
    _write_csv(out / "turnover.csv", ["date", "turnover"], [[d.date().isoformat(), t] for d, t in turnover])
    _write_csv(
        out / "sector_exposures.csv",
        ["date", "sector", "sector_weight"],
        [[d.date().isoformat(), s, w] for d, s, w in sector_expo],
    )
