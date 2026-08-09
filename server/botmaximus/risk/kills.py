"""Kill stack (§4.3): L1 strategy → L2 portfolio → L3 master.

State is persisted to Mongo on every change and reloaded on init, so a
process restart can never silently disarm a kill (§4.6). There is no code
path that suppresses L3; clearing it requires the operator reset token —
a deliberate action outside normal flow (§2.1, §2.8).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

KILL_DOC_ID = "kill_stack"

L3_RESET_TOKEN = "CONFIRM-RESET-L3"   # must be typed by the operator, never automated


class KillStack:
    def __init__(self, db=None) -> None:
        # `db` is accepted and ignored so existing call sites and tests keep
        # working through the storage migration. State lives in Postgres now.
        self.l1_suspended: dict[str, str] = {}   # strategy_id → reason
        self.l2_halted: str | None = None        # reason, or None
        self.l3_killed: str | None = None        # reason, or None

    # ---- persistence ----
    async def load(self) -> None:
        from botmaximus.storage import postgres
        row = await postgres.fetchrow(
            "SELECT state FROM risk_state WHERE id = %s", (KILL_DOC_ID,))
        if row:
            doc = row["state"]
            self.l1_suspended = doc.get("l1_suspended", {})
            self.l2_halted = doc.get("l2_halted")
            self.l3_killed = doc.get("l3_killed")
            if self.l3_killed:
                log.critical("L3 MASTER KILL is armed from persisted state: %s", self.l3_killed)

    async def _persist(self) -> None:
        import json

        from botmaximus.storage import postgres
        await postgres.execute(
            "INSERT INTO risk_state (id, state, updated_at) "
            "VALUES (%s, %s, now()) "
            "ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, "
            "  updated_at = now()",
            (KILL_DOC_ID, json.dumps({
                "l1_suspended": self.l1_suspended,
                "l2_halted": self.l2_halted,
                "l3_killed": self.l3_killed,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })))

    # ---- triggers (lowering is always allowed; raising never happens here) ----
    async def suspend_strategy(self, strategy_id: str, reason: str) -> None:
        self.l1_suspended[strategy_id] = reason
        log.warning("L1 kill: strategy %s suspended (%s)", strategy_id, reason)
        await self._persist()

    async def halt_portfolio(self, reason: str) -> None:
        self.l2_halted = reason
        log.error("L2 kill: portfolio halted (%s)", reason)
        await self._persist()

    #: Set by the execution layer at startup. When present, `master_kill`
    #: FLATTENS as well as blocking (audit B1) — a kill that only blocks new
    #: entries leaves full exposure on during the event that triggered it,
    #: which is a pause, not a kill. Kept as an injected hook so the risk core
    #: keeps no dependency on the venue client.
    flatten_hook = None

    async def master_kill(self, reason: str) -> dict | None:
        self.l3_killed = reason
        log.critical("L3 MASTER KILL (%s) — cancel all, flatten all, halt", reason)
        await self._persist()

        if self.flatten_hook is None:
            log.critical("no flatten hook registered — the kill BLOCKS but does "
                         "NOT flatten; open exposure remains")
            from botmaximus.obs import degradation
            await degradation.record(
                "master_kill_without_flatten",
                "L3 fired with no execution layer attached: entries are blocked "
                "but any open position is still on")
            return None
        return await self.flatten_hook(reason)

    async def check_equity(self, drawdown_pct: float, day_pnl_pct: float,
                           max_dd_kill_pct: float, daily_loss_limit_pct: float) -> None:
        """Run on EVERY equity update (§4.3). Cannot be suppressed."""
        if self.l3_killed is None and drawdown_pct >= max_dd_kill_pct:
            await self.master_kill(
                f"max drawdown {drawdown_pct:.2f}% >= kill threshold {max_dd_kill_pct}%"
            )
        if self.l2_halted is None and day_pnl_pct <= -daily_loss_limit_pct:
            await self.halt_portfolio(
                f"daily loss {day_pnl_pct:.2f}% <= -{daily_loss_limit_pct}%"
            )

    # ---- state queries ----
    def blocks_trading(self) -> str | None:
        if self.l3_killed:
            return f"L3_master_kill:{self.l3_killed}"
        if self.l2_halted:
            return f"L2_portfolio_halt:{self.l2_halted}"
        return None

    def strategy_suspended(self, strategy_id: str) -> str | None:
        return self.l1_suspended.get(strategy_id)

    # ---- operator-only resets ----
    async def resume_strategy(self, strategy_id: str) -> None:
        self.l1_suspended.pop(strategy_id, None)
        await self._persist()

    async def clear_l2(self) -> None:
        self.l2_halted = None
        await self._persist()

    async def reset_l3(self, token: str) -> bool:
        """Deliberate operator action only. Wrong token → stays killed."""
        if token != L3_RESET_TOKEN:
            log.error("L3 reset attempted with wrong token — kill remains armed")
            return False
        log.warning("L3 master kill reset by operator")
        self.l3_killed = None
        await self._persist()
        return True

    def snapshot(self) -> dict:
        return {
            "l1_suspended": dict(self.l1_suspended),
            "l2_halted": self.l2_halted,
            "l3_killed": self.l3_killed,
        }
