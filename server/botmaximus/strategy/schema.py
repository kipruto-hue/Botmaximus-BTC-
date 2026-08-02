"""StrategyDefinition (§3) — the declarative object a strategy *is*.

Two absences are load-bearing:

- **There is no size field, anywhere.** Position size is an output of the risk
  core, derived from equity, risk-per-trade and the stop distance. A strategy
  states *where the stop goes*; it cannot state how much to buy. Parsing is
  strict, so a generator that invents `"size": 0.5` is rejected rather than
  having the key quietly ignored.
- **`exit.stop` is required.** A definition without it does not parse. There is
  no "stopless" branch to disable, no default of None to fall through.

`stop_basis` in §3 and `exit.stop` describe the same thing — how the stop
distance is computed. Storing them as two fields would let them disagree, so
`stop_basis` is a read-only alias for `exit.stop` and cannot drift from it.

Beyond §3 this adds one field: `direction` (long | short). §4.1's Predicate is
boolean — it says *when*, not *which way* — so without it a short-side seed such
as funding-extreme contrarian is unwritable. It is a closed enum of two values
and grants no new power.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from botmaximus.strategy.grammar import ParseError, Predicate, parse_predicate, to_dict

LIFECYCLE_STATES = ("candidate", "paper", "micro", "full", "retired")
DIRECTIONS = ("long", "short")
STOP_KINDS = ("atr", "percent", "structural")
TARGET_KINDS = ("r_multiple", "feature")
FEEDS = ("ohlcv", "funding", "oi", "orderbook", "liquidations", "ticks")


def _strict(d: dict, allowed: set[str], path: str) -> None:
    if not isinstance(d, dict):
        raise ParseError(f"expected_object:{type(d).__name__}", path)
    extra = set(d) - allowed
    if extra:
        raise ParseError(f"unknown_keys:{sorted(extra)}", path)


def _num(d: dict, key: str, path: str, required: bool = True, default=None):
    if key not in d:
        if required:
            raise ParseError(f"missing:{key}", path)
        return default
    v = d[key]
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ParseError(f"{key}_not_numeric:{v!r}", path)
    return v


@dataclass(frozen=True)
class StopBasis:
    """How the stop distance is computed — the input risk sizing needs (§3)."""
    kind: str                       # atr | percent | structural
    mult: float = 1.5               # ATR multiple, or lookback multiplier
    timeframe: str = "1m"
    n: int = 14                     # ATR period, or structural lookback in bars

    @staticmethod
    def parse(d: dict, path: str = "exit.stop") -> "StopBasis":
        _strict(d, {"kind", "mult", "timeframe", "n"}, path)
        kind = d.get("kind")
        if kind not in STOP_KINDS:
            raise ParseError(f"unknown_stop_kind:{kind!r}", path)
        return StopBasis(
            kind=kind,
            mult=float(_num(d, "mult", path, required=False, default=1.5)),
            timeframe=d.get("timeframe", "1m"),
            n=int(_num(d, "n", path, required=False, default=14)),
        )

    def to_dict(self) -> dict:
        return {"kind": self.kind, "mult": self.mult,
                "timeframe": self.timeframe, "n": self.n}


@dataclass(frozen=True)
class Target:
    kind: str = "r_multiple"
    r: float = 1.0

    @staticmethod
    def parse(d: dict, path: str = "exit.target") -> "Target":
        _strict(d, {"kind", "r"}, path)
        kind = d.get("kind", "r_multiple")
        if kind not in TARGET_KINDS:
            raise ParseError(f"unknown_target_kind:{kind!r}", path)
        if kind == "feature":
            # Deliberately not implemented: a feature-priced target needs an
            # exit-side evaluator the engine does not have, and half-building it
            # would mean a strategy whose target silently never triggers.
            raise ParseError("feature_target_not_supported_in_c1", path)
        return Target(kind=kind, r=float(_num(d, "r", path, required=False, default=1.0)))

    def to_dict(self) -> dict:
        return {"kind": self.kind, "r": self.r}


@dataclass(frozen=True)
class Trailing:
    kind: str = "atr"
    mult: float = 2.0
    timeframe: str = "1m"
    n: int = 14

    @staticmethod
    def parse(d: dict, path: str = "exit.trailing") -> "Trailing":
        _strict(d, {"kind", "mult", "timeframe", "n"}, path)
        kind = d.get("kind", "atr")
        if kind != "atr":
            raise ParseError(f"unknown_trailing_kind:{kind!r}", path)
        return Trailing(kind=kind,
                        mult=float(_num(d, "mult", path, required=False, default=2.0)),
                        timeframe=d.get("timeframe", "1m"),
                        n=int(_num(d, "n", path, required=False, default=14)))

    def to_dict(self) -> dict:
        return {"kind": self.kind, "mult": self.mult,
                "timeframe": self.timeframe, "n": self.n}


@dataclass(frozen=True)
class ExitSpec:
    stop: StopBasis                         # REQUIRED — §4.3
    target: Target | None = None
    trailing: Trailing | None = None
    time_exit: int | None = None            # bars; bounded by max_holding_bars_cap
    regime_invalidation: bool = True

    @staticmethod
    def parse(d: dict, path: str = "exit") -> "ExitSpec":
        _strict(d, {"stop", "target", "trailing", "time_exit",
                    "regime_invalidation"}, path)
        if "stop" not in d:
            raise ParseError("missing_stop", path)     # §4.3 / §5.5
        te = d.get("time_exit")
        if te is not None and (isinstance(te, bool) or not isinstance(te, int)):
            raise ParseError(f"time_exit_not_integer:{te!r}", path)
        ri = d.get("regime_invalidation", True)
        if not isinstance(ri, bool):
            raise ParseError(f"regime_invalidation_not_bool:{ri!r}", path)
        return ExitSpec(
            stop=StopBasis.parse(d["stop"], f"{path}.stop"),
            target=Target.parse(d["target"], f"{path}.target") if d.get("target") else None,
            trailing=Trailing.parse(d["trailing"], f"{path}.trailing") if d.get("trailing") else None,
            time_exit=te,
            regime_invalidation=ri,
        )

    def to_dict(self) -> dict:
        return {
            "stop": self.stop.to_dict(),
            "target": self.target.to_dict() if self.target else None,
            "trailing": self.trailing.to_dict() if self.trailing else None,
            "time_exit": self.time_exit,
            "regime_invalidation": self.regime_invalidation,
        }


@dataclass(frozen=True)
class ParamSpec:
    """A named tunable with its own bounds — mutation and validation both read
    these, so a repaired or mutated strategy cannot wander outside them."""
    name: str
    value: float
    lo: float
    hi: float

    @staticmethod
    def parse(name: str, d: dict, path: str) -> "ParamSpec":
        _strict(d, {"value", "lo", "hi"}, path)
        v, lo, hi = (_num(d, "value", path), _num(d, "lo", path), _num(d, "hi", path))
        if lo > hi:
            raise ParseError(f"bounds_inverted:{lo}>{hi}", path)
        return ParamSpec(name, float(v), float(lo), float(hi))

    def to_dict(self) -> dict:
        return {"value": self.value, "lo": self.lo, "hi": self.hi}


@dataclass(frozen=True)
class StrategyDefinition:
    id: str
    version: int
    rationale: str                          # §1.7 — required, never decoration
    direction: str
    timeframes: tuple[str, ...]
    required_feeds: tuple[str, ...]
    regime_scope: tuple[str, ...]
    entry: Predicate
    exit: ExitSpec
    params: dict[str, ParamSpec] = field(default_factory=dict)
    lifecycle_state: str = "candidate"
    origin: str = "seed"                    # seed | generated | repair

    #: §3 names `stop_basis` separately; it is the same object as exit.stop, so
    #: it is exposed as an alias rather than a second field that could disagree.
    @property
    def stop_basis(self) -> StopBasis:
        return self.exit.stop

    ALLOWED_KEYS = {"id", "version", "rationale", "direction", "timeframes",
                    "required_feeds", "regime_scope", "entry", "exit", "params",
                    "lifecycle_state", "origin"}

    @staticmethod
    def parse(d: dict) -> "StrategyDefinition":
        """§5.1 structural validation. Strict: an unknown key is a rejection.
        This is what stops `{"size": 0.5}` or `{"leverage": 10}` from being
        silently dropped and looking accepted."""
        _strict(d, StrategyDefinition.ALLOWED_KEYS, "strategy")

        for key in ("id", "rationale", "direction"):
            if not isinstance(d.get(key), str) or not d[key].strip():
                raise ParseError(f"missing_or_empty:{key}", "strategy")
        if d["direction"] not in DIRECTIONS:
            raise ParseError(f"unknown_direction:{d['direction']!r}", "strategy")

        version = d.get("version", 1)
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ParseError(f"version_invalid:{version!r}", "strategy")

        state = d.get("lifecycle_state", "candidate")
        if state not in LIFECYCLE_STATES:
            raise ParseError(f"unknown_lifecycle_state:{state!r}", "strategy")

        def _strlist(key: str) -> tuple[str, ...]:
            v = d.get(key)
            if not isinstance(v, list) or not v:
                raise ParseError(f"missing_or_empty:{key}", "strategy")
            if not all(isinstance(x, str) for x in v):
                raise ParseError(f"{key}_not_strings", "strategy")
            return tuple(dict.fromkeys(v))          # de-dup, order preserved

        raw_params = d.get("params", {})
        if not isinstance(raw_params, dict):
            raise ParseError("params_not_object", "strategy")
        params = {k: ParamSpec.parse(k, v, f"params.{k}") for k, v in raw_params.items()}

        if "entry" not in d:
            raise ParseError("missing:entry", "strategy")
        if "exit" not in d:
            raise ParseError("missing:exit", "strategy")

        return StrategyDefinition(
            id=d["id"].strip(),
            version=version,
            rationale=d["rationale"].strip(),
            direction=d["direction"],
            timeframes=_strlist("timeframes"),
            required_feeds=_strlist("required_feeds"),
            regime_scope=_strlist("regime_scope"),
            entry=parse_predicate(d["entry"]),
            exit=ExitSpec.parse(d["exit"]),
            params=params,
            lifecycle_state=state,
            origin=d.get("origin", "seed"),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "version": self.version,
            "rationale": self.rationale,
            "direction": self.direction,
            "timeframes": list(self.timeframes),
            "required_feeds": list(self.required_feeds),
            "regime_scope": list(self.regime_scope),
            "entry": to_dict(self.entry),
            "exit": self.exit.to_dict(),
            "params": {k: v.to_dict() for k, v in self.params.items()},
            "lifecycle_state": self.lifecycle_state,
            "origin": self.origin,
        }
