# Project 1: Equity Factor Backtest

This project builds and backtests **value**, **momentum**, and **quality** equity signals on a stock universe using monthly rebalancing portfolios.

## Features
- Factor construction
  - **Value**: cross-sectional z-score of `book_to_market` (higher is cheaper)
  - **Momentum**: 12-1 month price momentum (skip most recent month)
  - **Quality**: combines profitability (`roe`) and low accruals (`accruals`)
- Portfolio styles
  - **Long-only**: long top quantile
  - **Long-short**: long top quantile and short bottom quantile (dollar neutral)
- Monthly rebalancing
- Performance and risk report
  - CAGR
  - Sharpe ratio
  - Max drawdown
  - Turnover
  - Sector exposures

## Repository structure

- `src/equity_factor_backtest/data.py`: panel loading and validation
- `src/equity_factor_backtest/factors.py`: factor engineering
- `src/equity_factor_backtest/portfolio.py`: portfolio construction and backtest
- `src/equity_factor_backtest/metrics.py`: performance analytics
- `src/equity_factor_backtest/report.py`: report assembly
- `src/equity_factor_backtest/cli.py`: command-line entrypoint
- `examples/make_synthetic_data.py`: synthetic dataset generator

## Input schema

CSV columns expected:

- `date` (YYYY-MM-DD)
- `ticker`
- `close`
- `sector`
- `book_to_market`
- `roe`
- `accruals`

Rows should be daily observations per ticker. Signals are computed at month-end and portfolios rebalance monthly.

## Quick start

```bash
python examples/make_synthetic_data.py --output data/synthetic_equities.csv
PYTHONPATH=src python -m equity_factor_backtest.cli \
  --input data/synthetic_equities.csv \
  --mode long_short \
  --top-quantile 0.2 \
  --bottom-quantile 0.2 \
  --output-dir output
```

Artifacts:
- `output/performance_summary.csv`
- `output/monthly_returns.csv`
- `output/turnover.csv`
- `output/sector_exposures.csv`

## Notes

- Rebalancing happens at month-end. Portfolio return for month `t+1` uses weights formed at month-end `t`.
- The default combined signal equally weights standardized value, momentum, and quality scores.
