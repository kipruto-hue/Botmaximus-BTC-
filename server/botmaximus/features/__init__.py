"""Feature layer (Strategy DSL §4.2, §9).

The only thing the DSL is allowed to read. Everything here is causal by
construction — see `compute.py` for the two rules that make that true.
"""
from botmaximus.features.compute import FeatureContext, FeatureRef, build_context
from botmaximus.features.registry import FEATURE_REGISTRY, FeatureSpec, resolve

__all__ = [
    "FEATURE_REGISTRY", "FeatureSpec", "resolve",
    "FeatureContext", "FeatureRef", "build_context",
]
