from __future__ import annotations

import argparse

from .data import load_panel
from .factors import compute_monthly_panel
from .metrics import compute_performance
from .portfolio import backtest, build_weights
from .report import write_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Equity factor backtest")
    p.add_argument("--input", required=True)
    p.add_argument("--mode", default="long_short", choices=["long_only", "long_short"])
    p.add_argument("--top-quantile", type=float, default=0.2)
    p.add_argument("--bottom-quantile", type=float, default=0.2)
    p.add_argument("--output-dir", default="output")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    panel = load_panel(args.input)
    monthly = compute_monthly_panel(panel)
    weights = build_weights(monthly, args.mode, args.top_quantile, args.bottom_quantile)
    monthly_returns, turnover, sector_expo = backtest(weights, monthly)
    perf = compute_performance(monthly_returns)
    write_report(args.output_dir, perf, monthly_returns, turnover, sector_expo)

    print("Backtest complete")
    print(perf)


if __name__ == "__main__":
    main()
