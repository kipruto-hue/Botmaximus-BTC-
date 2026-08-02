"""Strategy DSL layer (Strategy DSL & Generation Master Prompt v1.0, Pass C1).

The safety boundary: a constrained language for expressing strategies, and the
validator/compiler that closes it. Pass C2 adds the generator that proposes
inside this boundary — and the boundary is built first on purpose (§0, §11).
"""
from botmaximus.strategy.compiler import CompiledStrategy, compile_strategy
from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.validator import (
    ValidationResult, parse_and_validate, signature, validate,
)

__all__ = [
    "StrategyDefinition", "validate", "parse_and_validate", "ValidationResult",
    "signature", "compile_strategy", "CompiledStrategy",
]
