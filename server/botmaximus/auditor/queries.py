r"""The query set (§3, §4).

Every number that appears in a report is computed **here**, in SQL, and the
model quotes it (§1.3). That is the entire point of this module: an LLM asked
to work out a fill rate from a list of orders will usually get it right, and
the times it does not are indistinguishable from the times it does.

Each query returns rows *and* the SQL that produced them, so the citation
carries a re-runnable statement rather than a description of one. A citation
the operator cannot execute is a footnote, not evidence.

Every statement here is a SELECT against the §3 allowlist. Nothing reads
`market_records` (§1.6 — no live prices), raw quarantine payloads, or the LLM
blob columns; the `auditor_read` role would refuse anyway, which is the point of
having both.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from botmaximus.storage import postgres


@dataclass
class QueryResult:
    """One section's worth of ledger facts, with the SQL that produced them."""
    name: str
    sql: str
    rows: list[dict] = field(default_factory=list)

    @property
    def scalar(self):
        if not self.rows:
            return None
        return next(iter(self.rows[0].values()))

    def citation_for(self, column: str | None = None):
        """A `{table, query, quoted_value}` triple pointing at this result."""
        from botmaximus.auditor.citations import Citation
        value = (self.rows[0].get(column) if column and self.rows
                 else self.scalar)
        return Citation(table=self.name, record_id_or_query=self.sql,
                        quoted_value="" if value is None else str(value))


async def _run(name: str, sql: str, params: tuple) -> QueryResult:
    rows = await postgres.fetch(sql, params)
    # Inline the parameters so the citation is executable as written. Values
    # here are timestamps and integers the composer supplied, never model
    # output, so there is nothing to inject.
    literal = sql
    for p in params:
        literal = literal.replace("%s", _lit(p), 1)
    return QueryResult(name=name, sql=" ".join(literal.split()),
                       rows=[dict(r) for r in rows])


def _lit(v) -> str:
    if isinstance(v, datetime):
        return f"'{v.isoformat()}'"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, tuple)):
        inner = ",".join(_lit(x) for x in v)
        return f"ARRAY[{inner}]"
    return "'" + str(v).replace("'", "''") + "'"


# ---------------------------------------------------------------- §4.A.2

async def trading_summary(start: datetime, end: datetime) -> QueryResult:
    """Trades, fill rate and cost drift — computed in SQL, not by the model.

    `unfilled` and `reconciled` come from the realizations table rather than a
    status column, because §3.I derives status from row presence: a leg with no
    realization is pending, not filled-at-the-predicted-price.
    """
    return await _run("execution_ledger", """
        SELECT count(*)                                          AS legs,
               count(r.trade_id) FILTER (WHERE r.status = 'reconciled') AS reconciled,
               count(r.trade_id) FILTER (WHERE r.status = 'unfilled')   AS unfilled,
               count(*) - count(r.trade_id)                       AS pending,
               ROUND(100.0 * count(r.trade_id) FILTER (WHERE r.status='reconciled')
                     / NULLIF(count(*), 0), 2)                    AS fill_rate_pct,
               ROUND(COALESCE(sum(d.cost_drift_usd), 0)::numeric, 4) AS cost_drift_usd,
               ROUND(COALESCE(avg(d.slippage_bps_drift), 0)::numeric, 3) AS avg_slippage_drift_bps
          FROM execution_ledger_predictions p
          LEFT JOIN execution_ledger_realizations r USING (trade_id, leg)
          LEFT JOIN execution_ledger_drift       d USING (trade_id, leg)
         WHERE p.decision_time >= %s AND p.decision_time < %s
    """, (start, end))


async def fills_summary(start: datetime, end: datetime) -> QueryResult:
    return await _run("fills", """
        SELECT count(*) AS fills,
               ROUND(COALESCE(sum(fee), 0)::numeric, 4)     AS fees,
               ROUND(COALESCE(sum(funding), 0)::numeric, 6) AS funding
          FROM fills WHERE fill_time >= %s AND fill_time < %s
    """, (start, end))


# ---------------------------------------------------------------- §4.A.3

async def strategy_pool() -> QueryResult:
    return await _run("strategies", """
        SELECT lifecycle_state, count(*) AS n
          FROM strategies GROUP BY lifecycle_state ORDER BY lifecycle_state
    """, ())


async def lifecycle_transitions(start: datetime, end: datetime) -> QueryResult:
    return await _run("strategy_lifecycle_events", """
        SELECT strategy_id, from_state, to_state, reason, actor, at
          FROM strategy_lifecycle_events
         WHERE at >= %s AND at < %s ORDER BY at
    """, (start, end))


async def decay_events(start: datetime, end: datetime) -> QueryResult:
    return await _run("strategy_events", """
        SELECT strategy_id, detail->>'cause' AS cause, at
          FROM strategy_events
         WHERE event = 'decay' AND at >= %s AND at < %s ORDER BY at
    """, (start, end))


# ---------------------------------------------------------------- §4.A.4

async def risk_events(start: datetime, end: datetime) -> QueryResult:
    return await _run("kill_events", """
        SELECT level, reason, scope, at
          FROM kill_events WHERE at >= %s AND at < %s ORDER BY at
    """, (start, end))


# ---------------------------------------------------------------- §4.A.5

async def data_quality(start: datetime, end: datetime) -> QueryResult:
    """Aggregate counts and failing check NAMES. §3 withholds the raw
    quarantined payloads, and the names are what a report can act on anyway."""
    return await _run("quality_events", """
        SELECT failing_check, count(*) AS n
          FROM quality_events WHERE at >= %s AND at < %s
         GROUP BY failing_check ORDER BY n DESC
    """, (start, end))


async def coverage_gaps(start: datetime, end: datetime) -> QueryResult:
    return await _run("coverage_ledger", """
        SELECT feed, count(*) FILTER (WHERE state <> 'complete') AS incomplete,
               count(*) AS slots
          FROM coverage_ledger WHERE slot >= %s AND slot < %s
         GROUP BY feed ORDER BY feed
    """, (start, end))


async def degraded_events(start: datetime, end: datetime) -> QueryResult:
    return await _run("telemetry_events", """
        SELECT label, count(*) AS n
          FROM telemetry_events
         WHERE kind = 'degraded' AND at >= %s AND at < %s
         GROUP BY label ORDER BY n DESC
    """, (start, end))


# ---------------------------------------------------------------- §4.A.6

async def generator_activity(start: datetime, end: datetime) -> QueryResult:
    return await _run("generations", """
        SELECT count(*) AS proposed, count(DISTINCT strategy_id) AS distinct_ideas
          FROM generations WHERE at >= %s AND at < %s
    """, (start, end))


async def trial_ledger() -> QueryResult:
    """Lifetime, not windowed — that is the number the deflated Sharpe uses,
    and a window would understate the search intensity it corrects for."""
    return await _run("trials", """
        SELECT count(*) AS lifetime_trials,
               count(DISTINCT sig_hash) AS distinct_signatures FROM trials
    """, ())


# ---------------------------------------------------------------- §4.A.7

async def scrutiny_activity(start: datetime, end: datetime) -> QueryResult:
    return await _run("scrutiny_events", """
        SELECT count(*) AS verdicts,
               count(*) FILTER (WHERE verdict = 'VETO')    AS vetoes,
               ROUND(100.0 * count(*) FILTER (WHERE verdict='VETO')
                     / NULLIF(count(*), 0), 2)             AS veto_rate_pct,
               ROUND(COALESCE(avg(latency_ms), 0)::numeric, 1) AS avg_latency_ms
          FROM scrutiny_events WHERE at >= %s AND at < %s
    """, (start, end))


async def arbiter_refusals(start: datetime, end: datetime) -> QueryResult:
    """'Why didn't it trade?' is unanswerable from a log of trades that did."""
    return await _run("arbiter_events", """
        SELECT reason, count(*) AS n
          FROM arbiter_events WHERE at >= %s AND at < %s
         GROUP BY reason ORDER BY n DESC
    """, (start, end))


# ---------------------------------------------------------------- assembly

DAILY_SECTIONS = (
    ("trading_summary", trading_summary),
    ("fills", fills_summary),
    ("strategy_pool", None),
    ("lifecycle_transitions", lifecycle_transitions),
    ("decay_events", decay_events),
    ("risk_events", risk_events),
    ("data_quality", data_quality),
    ("coverage", coverage_gaps),
    ("degraded", degraded_events),
    ("generator", generator_activity),
    ("trials", None),
    ("scrutiny", scrutiny_activity),
    ("arbiter", arbiter_refusals),
)


async def daily_set(start: datetime, end: datetime) -> dict[str, QueryResult]:
    """Every fact a daily rundown may cite. Empty results are returned as empty
    lists, not omitted — §5.B: the model writes "no events" from `[]`, not from
    a missing key it might fill in itself."""
    out: dict[str, QueryResult] = {}
    for name, fn in DAILY_SECTIONS:
        if fn is None:
            out[name] = await (strategy_pool() if name == "strategy_pool"
                               else trial_ledger())
        else:
            out[name] = await fn(start, end)
    return out
