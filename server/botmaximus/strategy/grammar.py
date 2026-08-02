"""The predicate grammar (§4.1) — data, never code.

    Predicate  := Comparison | And(Predicate+) | Or(Predicate+) | Not(Predicate)
    Comparison := Feature Op (Value | Feature)
    Op         := gt | lt | gte | lte | cross_above | cross_below | between
    Value      := constant | param_ref
    Feature    := registry_feature(timeframe, args...)

What is absent matters more than what is present. There is no arithmetic, no
function call, no string expression, no index into a bar array — so a generated
strategy cannot write "buy at the low", reach a future bar, read equity, or
smuggle Python through a field. Dangerous behaviour is **inexpressible**, which
is a different and much stronger property than "rejected by a check": a check
can be missed, an absent grammar production cannot.

The left side of a comparison is always a Feature. A bare `5 > 3` is not a
market condition, and allowing constant-only comparisons would let a generator
emit predicates that are trivially always-true.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from botmaximus.features.compute import FeatureRef

OPS = ("gt", "lt", "gte", "lte", "cross_above", "cross_below", "between")
CROSS_OPS = ("cross_above", "cross_below")


class ParseError(Exception):
    """Structured, machine-readable — the reason is fed back to the generator."""

    def __init__(self, reason: str, path: str = "") -> None:
        self.reason = reason
        self.path = path
        super().__init__(f"{path}: {reason}" if path else reason)


# --------------------------------------------------------------------------
# Terms
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class FeatureTerm:
    ref: FeatureRef

    @property
    def key(self) -> str:
        return self.ref.key


@dataclass(frozen=True)
class ConstTerm:
    value: float


@dataclass(frozen=True)
class ParamTerm:
    """Reference to a named tunable in `params` — resolved at compile time, so a
    param can be mutated without touching the predicate tree."""
    name: str


Term = Union[FeatureTerm, ConstTerm, ParamTerm]


# --------------------------------------------------------------------------
# Predicates
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Comparison:
    left: FeatureTerm
    op: str
    right: Term
    right2: Term | None = None          # only for `between`


@dataclass(frozen=True)
class And:
    children: tuple["Predicate", ...]


@dataclass(frozen=True)
class Or:
    children: tuple["Predicate", ...]


@dataclass(frozen=True)
class Not:
    child: "Predicate"


Predicate = Union[Comparison, And, Or, Not]


# --------------------------------------------------------------------------
# Parsing (strict — unknown keys are a rejection, never ignored)
# --------------------------------------------------------------------------
def _strict(d: dict, allowed: set[str], path: str) -> None:
    if not isinstance(d, dict):
        raise ParseError(f"expected_object:{type(d).__name__}", path)
    extra = set(d) - allowed
    if extra:
        raise ParseError(f"unknown_keys:{sorted(extra)}", path)


def parse_feature(d: dict, path: str) -> FeatureTerm:
    _strict(d, {"feature", "timeframe", "args"}, path)
    name = d.get("feature")
    if not isinstance(name, str) or not name:
        raise ParseError("feature_name_missing", path)
    tf = d.get("timeframe")
    if tf is not None and not isinstance(tf, str):
        raise ParseError(f"timeframe_not_string:{tf!r}", path)
    raw_args = d.get("args", {})
    if not isinstance(raw_args, dict):
        raise ParseError("args_not_object", path)
    for k, v in raw_args.items():
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ParseError(f"arg_not_numeric:{k}={v!r}", path)
    args = tuple(sorted((k, v) for k, v in raw_args.items()))
    return FeatureTerm(FeatureRef(name=name, timeframe=tf, args=args))


def parse_term(d, path: str) -> Term:
    """A term is a feature, a constant, or a param reference — nothing else."""
    if isinstance(d, bool):
        raise ParseError(f"bool_not_a_term:{d!r}", path)
    if isinstance(d, (int, float)):
        return ConstTerm(float(d))
    if not isinstance(d, dict):
        raise ParseError(f"invalid_term:{type(d).__name__}", path)
    if "feature" in d:
        return parse_feature(d, path)
    if "param" in d:
        _strict(d, {"param"}, path)
        if not isinstance(d["param"], str) or not d["param"]:
            raise ParseError("param_name_invalid", path)
        return ParamTerm(d["param"])
    if "const" in d:
        _strict(d, {"const"}, path)
        v = d["const"]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ParseError(f"const_not_numeric:{v!r}", path)
        return ConstTerm(float(v))
    raise ParseError(f"unknown_term_keys:{sorted(d)}", path)


def parse_predicate(d, path: str = "entry") -> Predicate:
    if not isinstance(d, dict):
        raise ParseError(f"expected_object:{type(d).__name__}", path)

    if "and" in d or "or" in d:
        key = "and" if "and" in d else "or"
        _strict(d, {key}, path)
        kids = d[key]
        if not isinstance(kids, list) or len(kids) < 1:
            raise ParseError(f"{key}_needs_children", path)
        parsed = tuple(parse_predicate(c, f"{path}.{key}[{i}]")
                       for i, c in enumerate(kids))
        return And(parsed) if key == "and" else Or(parsed)

    if "not" in d:
        _strict(d, {"not"}, path)
        return Not(parse_predicate(d["not"], f"{path}.not"))

    _strict(d, {"left", "op", "right", "right2"}, path)
    op = d.get("op")
    if op not in OPS:
        raise ParseError(f"unknown_op:{op!r}", path)
    if "left" not in d:
        raise ParseError("comparison_missing_left", path)
    left = parse_term(d["left"], f"{path}.left")
    if not isinstance(left, FeatureTerm):
        # §4.1: the left side is a Feature. Constant-vs-constant is not a market
        # condition and would let a generator emit always-true entries.
        raise ParseError("left_must_be_feature", path)
    if "right" not in d:
        raise ParseError("comparison_missing_right", path)
    right = parse_term(d["right"], f"{path}.right")

    if op == "between":
        if "right2" not in d:
            raise ParseError("between_needs_right2", path)
        return Comparison(left, op, right, parse_term(d["right2"], f"{path}.right2"))
    if "right2" in d:
        raise ParseError(f"right2_only_valid_for_between:{op}", path)
    return Comparison(left, op, right)


# --------------------------------------------------------------------------
# Traversal
# --------------------------------------------------------------------------
def walk(p: Predicate):
    yield p
    if isinstance(p, (And, Or)):
        for c in p.children:
            yield from walk(c)
    elif isinstance(p, Not):
        yield from walk(p.child)


def feature_terms(p: Predicate) -> list[FeatureTerm]:
    out: list[FeatureTerm] = []
    for node in walk(p):
        if isinstance(node, Comparison):
            out.append(node.left)
            for t in (node.right, node.right2):
                if isinstance(t, FeatureTerm):
                    out.append(t)
    return out


def param_names(p: Predicate) -> set[str]:
    return {t.name for node in walk(p) if isinstance(node, Comparison)
            for t in (node.right, node.right2) if isinstance(t, ParamTerm)}


def to_dict(p: Predicate) -> dict:
    """Round-trip back to JSON — the stored form and the signature both use it."""
    if isinstance(p, And):
        return {"and": [to_dict(c) for c in p.children]}
    if isinstance(p, Or):
        return {"or": [to_dict(c) for c in p.children]}
    if isinstance(p, Not):
        return {"not": to_dict(p.child)}
    out = {"left": _term_dict(p.left), "op": p.op, "right": _term_dict(p.right)}
    if p.right2 is not None:
        out["right2"] = _term_dict(p.right2)
    return out


def _term_dict(t: Term) -> dict | float:
    if isinstance(t, ConstTerm):
        return t.value
    if isinstance(t, ParamTerm):
        return {"param": t.name}
    d: dict = {"feature": t.ref.name}
    if t.ref.timeframe:
        d["timeframe"] = t.ref.timeframe
    if t.ref.args:
        d["args"] = dict(t.ref.args)
    return d
