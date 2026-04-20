from __future__ import annotations

import math
from datetime import datetime


def compute_performance(monthly_returns: list[tuple[datetime, float]]) -> dict[str, float]:
    rs = [r for _, r in monthly_returns]
    n = len(rs)
    if n == 0:
        return {"cagr": float("nan"), "sharpe": float("nan"), "max_drawdown": float("nan"), "months": 0}

    wealth = []
    w = 1.0
    for r in rs:
        w *= 1.0 + r
        wealth.append(w)

    years = n / 12.0
    cagr = wealth[-1] ** (1.0 / years) - 1.0 if years > 0 else float("nan")

    mu = sum(rs) / n
    var = sum((x - mu) ** 2 for x in rs) / n
    vol = math.sqrt(var)
    sharpe = (mu / vol) * math.sqrt(12.0) if vol > 0 else float("nan")

    peak = 1.0
    max_dd = 0.0
    for x in wealth:
        peak = max(peak, x)
        dd = x / peak - 1.0
        max_dd = min(max_dd, dd)

    return {"cagr": cagr, "sharpe": sharpe, "max_drawdown": max_dd, "months": float(n)}
