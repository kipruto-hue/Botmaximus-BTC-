"""Backtest harness (Decision & Execution Master Prompt §5).

Builds Gate 2 — a *trustworthy* backtester. Point-in-time evaluation (no
lookahead), a cost model that is never frictionless (§5.3), consultation of
the coverage ledger before it will evaluate a window (§2.4), and a validation
gate a strategy must pass gate-off before it can leave `candidate` (§5.5).

The strategy protocol here is the harness's input contract. Pass C's DSL
strategies (from the System Master Prompt v1.1) will conform to it or adapt
to it — no model-authored code executes here.
"""
