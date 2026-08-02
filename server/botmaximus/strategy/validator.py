"""Parser / validator (§5.1–5.7). Everything a proposal must survive before it
is allowed anywhere near the backtester.

Rejections are **structured**, not prose: each is a typed reason string that the
Pass-C2 generator gets back verbatim for retry, and that the population store
logs. "It didn't work" teaches a generator nothing; `unknown_feature:supertrend`
teaches it exactly one thing.

Note the division of labour. Parsing (§5.1) raises on the first structural
error, because a malformed object cannot be inspected further. Validation
(§5.2–5.7) collects *every* violation, because a generator retrying one fix at a
time burns a cycle per mistake.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from botmaximus.config import settings
from botmaximus.features.frames import TIMEFRAME_MINUTES
from botmaximus.features.registry import FEATURE_REGISTRY, FEED_HISTORY_DAYS
from botmaximus.strategy import grammar
from botmaximus.strategy.grammar import Comparison, ConstTerm, FeatureTerm, ParamTerm
from botmaximus.strategy.schema import FEEDS, StrategyDefinition


@dataclass
class ValidationResult:
    ok: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    signature: str = ""

    def __bool__(self) -> bool:
        return self.ok


# --------------------------------------------------------------------------
# §5.2 registry · §5.3 bounds
# --------------------------------------------------------------------------
def _check_features(defn: StrategyDefinition, reasons: list[str],
                    warnings: list[str]) -> set[str]:
    """Every referenced feature must exist, with valid args and timeframe."""
    used_feeds: set[str] = set()
    terms = grammar.feature_terms(defn.entry)
    # the stop and trailing read ATR, so they pull the ohlcv feed in too
    for extra in (defn.exit.stop, defn.exit.trailing):
        if extra is not None:
            used_feeds.add("ohlcv")
            if extra.timeframe not in TIMEFRAME_MINUTES:
                reasons.append(f"unknown_timeframe:{extra.timeframe}")

    for t in terms:
        ref = t.ref
        spec = FEATURE_REGISTRY.get(ref.name)
        if spec is None:                                        # §5.2
            reasons.append(f"unknown_feature:{ref.name}")
            continue
        used_feeds.add(spec.feed)

        if spec.kind == "categorical":
            # regime_label has no ordering, so gt/lt/cross_* are meaningless on
            # it. Regimes are declared through regime_scope instead.
            reasons.append(f"categorical_feature_in_predicate:{ref.name}")
            continue

        if spec.timeframed:
            if ref.timeframe is None:
                reasons.append(f"missing_timeframe:{ref.name}")
            elif ref.timeframe not in TIMEFRAME_MINUTES:
                reasons.append(f"unknown_timeframe:{ref.timeframe}")
            elif ref.timeframe not in spec.allowed_timeframes:
                reasons.append(f"timeframe_not_allowed:{ref.name}@{ref.timeframe}")
        elif ref.timeframe is not None:
            reasons.append(f"timeframe_not_applicable:{ref.name}")

        given = {k for k, _ in ref.args}
        expected = {a.name for a in spec.args}
        for missing in sorted(expected - given):
            reasons.append(f"missing_arg:{ref.name}.{missing}")
        for unexpected in sorted(given - expected):
            reasons.append(f"unknown_arg:{ref.name}.{unexpected}")
        for a in spec.args:                                     # §5.3
            if a.name in given:
                err = a.check(ref.arg(a.name))
                if err:
                    reasons.append(f"{ref.name}:{err}")

        days = FEED_HISTORY_DAYS[spec.feed]
        if days == 0:
            warnings.append(
                f"forward_coverage_only:{ref.name}({spec.feed}) - no venue history; "
                "backtestable only over forward-collected coverage")
        elif days is not None:
            warnings.append(f"history_capped_{days}d:{ref.name}({spec.feed})")

    return used_feeds


def _check_params(defn: StrategyDefinition, reasons: list[str]) -> None:
    """§5.3: every param inside its own bounds, and every reference resolvable."""
    for name, p in defn.params.items():
        if not (p.lo <= p.value <= p.hi):
            reasons.append(
                f"param_out_of_bounds:{name}={p.value}_not_in[{p.lo},{p.hi}]")
    referenced = grammar.param_names(defn.entry)
    for name in sorted(referenced - set(defn.params)):
        reasons.append(f"undefined_param_ref:{name}")
    for name in sorted(set(defn.params) - referenced):
        reasons.append(f"unused_param:{name}")


def _check_consistency(defn: StrategyDefinition, used_feeds: set[str],
                       reasons: list[str]) -> None:
    """§5.4: features ↔ required_feeds ↔ timeframes must agree.

    Under-declaring is the dangerous direction: an undeclared feed is a feed the
    coverage ledger is never asked about, which is exactly how a strategy gets
    backtested over a window where its data did not exist.
    """
    for f in defn.required_feeds:
        if f not in FEEDS:
            reasons.append(f"unknown_feed:{f}")
    for f in sorted(used_feeds - set(defn.required_feeds)):
        reasons.append(f"undeclared_feed:{f}")
    for f in sorted(set(defn.required_feeds) - used_feeds - {"ohlcv"}):
        reasons.append(f"declared_unused_feed:{f}")

    used_tfs = {t.ref.timeframe for t in grammar.feature_terms(defn.entry)
                if t.ref.timeframe}
    used_tfs |= {defn.exit.stop.timeframe}
    if defn.exit.trailing:
        used_tfs |= {defn.exit.trailing.timeframe}
    for tf in sorted(used_tfs - set(defn.timeframes)):
        reasons.append(f"undeclared_timeframe:{tf}")
    for tf in defn.timeframes:
        if tf not in TIMEFRAME_MINUTES:
            reasons.append(f"unknown_timeframe:{tf}")


def _check_exit(defn: StrategyDefinition, reasons: list[str]) -> None:
    """§5.5 mandatory stop, plus the MAX_HOLDING_BARS bound from §2."""
    stop = defn.exit.stop
    if stop.kind == "atr" and stop.mult <= 0:
        reasons.append(f"stop_mult_nonpositive:{stop.mult}")
    if stop.kind == "percent" and not (0 < stop.mult < 50):
        reasons.append(f"stop_percent_out_of_bounds:{stop.mult}")
    if stop.n < 1:
        reasons.append(f"stop_n_invalid:{stop.n}")

    te = defn.exit.time_exit
    if te is not None:
        if te < 1:
            reasons.append(f"time_exit_nonpositive:{te}")
        elif te > settings.max_holding_bars_cap:
            reasons.append(
                f"time_exit_exceeds_cap:{te}>{settings.max_holding_bars_cap}")
    if defn.exit.target is not None and defn.exit.target.r <= 0:
        reasons.append(f"target_r_nonpositive:{defn.exit.target.r}")
    if defn.exit.trailing is not None and defn.exit.trailing.mult <= 0:
        reasons.append(f"trailing_mult_nonpositive:{defn.exit.trailing.mult}")


def _check_regime_scope(defn: StrategyDefinition, reasons: list[str],
                        warnings: list[str]) -> None:
    from botmaximus.backtest.regimes import REGIME_BUCKETS
    for r in defn.regime_scope:
        if r not in REGIME_BUCKETS:
            reasons.append(f"unknown_regime_bucket:{r}")
    if len(set(defn.regime_scope)) == len(REGIME_BUCKETS) and defn.exit.regime_invalidation:
        # Trading every regime is a legitimate choice, so this is not a
        # rejection — but it makes regime_invalidation a no-op, and a guard that
        # can never fire should not be mistaken for one that works.
        warnings.append("regime_scope_universal:regime_invalidation_cannot_fire")


def _check_lookahead(defn: StrategyDefinition, reasons: list[str]) -> None:
    """§5.6 — defence in depth.

    The grammar has no production that can name a future bar, so this cannot
    fail on a parsed definition. It is asserted anyway: if someone later adds a
    grammar production with an offset or shift argument, this check is where
    that mistake surfaces instead of silently becoming free alpha.
    """
    for node in grammar.walk(defn.entry):
        if not isinstance(node, Comparison):
            continue
        for term in (node.left, node.right, node.right2):
            if term is None:
                continue
            if isinstance(term, FeatureTerm):
                for arg_name, arg_val in term.ref.args:
                    if arg_name in ("shift", "offset", "lead", "forward", "ahead"):
                        reasons.append(f"lookahead_construct:{term.ref.name}.{arg_name}")
                    if arg_name == "n" and isinstance(arg_val, (int, float)) and arg_val < 0:
                        reasons.append(f"negative_lookback:{term.ref.name}.n={arg_val}")
            elif not isinstance(term, (ConstTerm, ParamTerm)):
                reasons.append(f"unknown_term_type:{type(term).__name__}")


# --------------------------------------------------------------------------
# §5.7 diversity / dedupe
# --------------------------------------------------------------------------
def signature(defn: StrategyDefinition) -> frozenset[str]:
    """A structural fingerprint: which features (with timeframe and rounded
    args) the entry uses, the direction, and the exit shape.

    Deliberately *not* the exact param values — two strategies differing only in
    an EMA length of 50 vs 51 are the same idea, and letting both through is how
    a population becomes a hundred copies of one bet while the multiple-testing
    correction is told they were a hundred independent trials.
    """
    tokens = {f"dir:{defn.direction}"}
    for t in grammar.feature_terms(defn.entry):
        ref = t.ref
        args = ",".join(f"{k}~{_bucket(v)}" for k, v in ref.args)
        tokens.add(f"f:{ref.name}@{ref.timeframe or '-'}({args})")
    for node in grammar.walk(defn.entry):
        if isinstance(node, Comparison):
            tokens.add(f"op:{node.left.ref.name}.{node.op}")
    tokens.add(f"stop:{defn.exit.stop.kind}")
    tokens.add(f"trail:{bool(defn.exit.trailing)}")
    return frozenset(tokens)


def _bucket(v: float) -> str:
    """Round an arg to a coarse bucket so near-identical lengths collide."""
    if v <= 0:
        return "0"
    import math
    return str(round(math.log(abs(v) + 1) * 3))


def similarity(a: frozenset[str], b: frozenset[str]) -> float:
    """Jaccard overlap of two signatures."""
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def check_diversity(defn: StrategyDefinition, population,
                    threshold: float | None = None) -> list[str]:
    """§5.7: reject a proposal too close to something already in the population.

    `population` is an iterable of (id, signature).
    """
    threshold = settings.diversity_threshold if threshold is None else threshold
    sig = signature(defn)
    out = []
    for other_id, other_sig in population:
        if other_id == defn.id:
            continue
        s = similarity(sig, other_sig)
        if s >= threshold:
            out.append(f"near_duplicate_of:{other_id}:{s:.2f}>={threshold}")
    return out


# --------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------
def validate(defn: StrategyDefinition, population=()) -> ValidationResult:
    """§5.2–5.7 on an already-parsed definition. Collects every violation."""
    reasons: list[str] = []
    warnings: list[str] = []

    used_feeds = _check_features(defn, reasons, warnings)
    _check_params(defn, reasons)
    _check_consistency(defn, used_feeds, reasons)
    _check_exit(defn, reasons)
    _check_regime_scope(defn, reasons, warnings)
    _check_lookahead(defn, reasons)
    reasons.extend(check_diversity(defn, population))

    if not defn.rationale or len(defn.rationale) < 20:          # §1.7
        reasons.append("rationale_missing_or_trivial")

    return ValidationResult(ok=not reasons, reasons=reasons, warnings=warnings,
                            signature="|".join(sorted(signature(defn))))


def parse_and_validate(payload: dict, population=()) -> tuple[
        StrategyDefinition | None, ValidationResult]:
    """§5.1 then §5.2–5.7. The single door a proposal comes through."""
    try:
        defn = StrategyDefinition.parse(payload)
    except grammar.ParseError as e:
        return None, ValidationResult(ok=False, reasons=[f"parse:{e.path}:{e.reason}"])
    return defn, validate(defn, population)
