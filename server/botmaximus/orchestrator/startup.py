r"""TradeLoop startup: construction, preconditions, and the log lines (§4–§6).

§5 calls the startup logging "the whole point", and it is not decoration. The
audit that produced this module found a decision chain with no caller and
nothing anywhere saying so — the system was inert by design and inert by
accident in exactly the same way, and only reading the source could tell them
apart. These lines make every boot narrate which state it is in and why.

Two rules shape everything here:

**Disabled constructs nothing.** When the flag is off, no RiskCore, no
PaperEngine, no ScrutinyGate, no Arbiter are built. Boot behaviour is preserved
byte-for-byte from before this module existed, so turning the loop off can
never itself be the cause of a new problem.

**Enabled fails loudly or not at all.** If any layer cannot be constructed, or
`require_trading()` refuses, startup aborts. A process that comes up with a
half-wired trade loop is worse than one that refuses to come up, because the
first looks healthy.
"""
from __future__ import annotations

import logging

from botmaximus.config import settings
from botmaximus.orchestrator.trade_loop import TRADING_STATES, TradeLoop

log = logging.getLogger(__name__)


class TradeLoopStartupError(RuntimeError):
    """Enabled, but the system cannot honestly start it."""


async def pool_census() -> dict[str, int]:
    """Strategies by lifecycle state, for the boot log.

    Read-only and failure-tolerant: an unreachable database at this moment is
    reported by the storage layer, and a census that raised would take the
    whole process down over a log line.
    """
    try:
        from botmaximus.storage import postgres
        rows = await postgres.fetch(
            "SELECT lifecycle_state, count(*) AS n FROM strategies "
            "GROUP BY lifecycle_state")
        return {r["lifecycle_state"]: r["n"] for r in rows}
    except Exception as e:                              # noqa: BLE001
        log.warning("[TradeLoop] could not read the strategy pool: %s", e)
        return {}


def _census_line(census: dict[str, int]) -> str:
    order = ("candidate", "paper", "micro", "full", "retired")
    return ", ".join(f"{census.get(s, 0)} {s}" for s in order)


def _tradable(census: dict[str, int]) -> int:
    return sum(census.get(s, 0) for s in TRADING_STATES)


async def build(pipeline=None, risk_core=None) -> TradeLoop | None:
    """Construct and wire the loop, or explain why there isn't one.

    Returns the TradeLoop when enabled, else None. Emits exactly one of the
    four §5 lines either way.
    """
    census = await pool_census()

    if not settings.trade_loop_enabled:
        _log_disabled(census)
        return None

    # Enabled: preconditions first, and a failure here stops the process.
    try:
        settings.require_trading()
    except Exception as e:                              # noqa: BLE001
        raise TradeLoopStartupError(
            f"[TradeLoop] STARTUP ABORTED. TRADE_LOOP_ENABLED=true but "
            f"require_trading() failed: {e} Fix .env and restart.") from e

    try:
        from botmaximus.arbiter.core import Arbiter
        from botmaximus.execution.paper_engine import PaperEngine
        from botmaximus.execution.session import Session
        from botmaximus.risk.core import RiskCore
        from botmaximus.scrutiny.gate import ScrutinyGate

        risk = risk_core or RiskCore()
        session = Session.from_settings()
        loop = TradeLoop(
            risk_core=risk,
            arbiter=Arbiter(risk, session=session),
            scrutiny=ScrutinyGate(risk),
            paper_engine=PaperEngine(risk),
            session=session,
            enabled=True,
        )
    except TradeLoopStartupError:
        raise
    except Exception as e:                              # noqa: BLE001
        raise TradeLoopStartupError(
            f"[TradeLoop] STARTUP ABORTED. TRADE_LOOP_ENABLED=true but a "
            f"decision layer could not be constructed: {e}. Refusing to start "
            f"a half-wired trade loop — it would look healthy.") from e

    if pipeline is not None:
        pipeline.subscribe_bar_close(loop.handle_bar_close)

    window = session.label if session else "unset (no window enforced)"
    log.info(
        "[TradeLoop] ENABLED. Subscribed to bar-close events. Tradable "
        "strategies: %d. Cooldown: %ss. Trading window: %s. First bar will be "
        "processed at the next 1m close. Pool: %s",
        _tradable(census), settings.arbiter_cooldown_s, window,
        _census_line(census))
    return loop


def _log_disabled(census: dict[str, int]) -> None:
    tradable = _tradable(census)
    if tradable:
        log.info(
            "[TradeLoop] DISABLED (TRADE_LOOP_ENABLED=false) but the pool has "
            "%d strategy(ies) in a trading state. Operator action required: "
            "review them, then set TRADE_LOOP_ENABLED=true in .env and "
            "restart. Pool: %s", tradable, _census_line(census))
    else:
        log.info(
            "[TradeLoop] DISABLED (TRADE_LOOP_ENABLED=false). Reason: no "
            "strategy is in a paper/micro/full state, so there is nothing to "
            "trade. To enable: set TRADE_LOOP_ENABLED=true in .env and "
            "restart. Pool: %s", _census_line(census))


def log_stopped() -> None:
    log.info("[TradeLoop] STOPPED.")
