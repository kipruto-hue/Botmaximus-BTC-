r"""Versioned system prompts (§2.B, §3.B).

Prompts live on disk as markdown, not in string literals, because they are
operator-owned content that changes on a different cadence from the code that
sends them. Each carries a `prompt_version:` line, and its SHA is stamped into
every provenance record and every scrutiny event.

That stamping is what makes a change measurable: comparing verdicts before and
after a prompt edit is the only way to know whether the edit helped. A prompt
change with no version bump silently invalidates that comparison.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PROMPT = _ROOT / "strategy" / "prompts" / "generator_system.md"
SCRUTINY_PROMPT = _ROOT / "scrutiny" / "prompts" / "scrutiny_system.md"
AUDITOR_PROMPT = _ROOT / "auditor" / "prompts" / "auditor_system.md"

_VERSION = re.compile(r"^prompt_version:\s*(\S+)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class SystemPrompt:
    role: str
    version: str
    text: str
    sha: str
    path: Path


def load(path: Path, role: str) -> SystemPrompt:
    if not path.exists():
        raise FileNotFoundError(
            f"system prompt for {role} missing at {path}. Refusing to fall back "
            f"to a built-in default: an unversioned prompt makes every "
            f"before/after comparison meaningless.")
    text = path.read_text(encoding="utf-8")
    m = _VERSION.search(text)
    if not m:
        raise ValueError(
            f"{path.name} has no `prompt_version:` line. Every prompt edit must "
            f"be versioned so its effect on verdicts can be measured.")
    sha = hashlib.sha256(text.encode()).hexdigest()[:16]
    return SystemPrompt(role=role, version=m.group(1), text=text, sha=sha,
                        path=path)


def generator() -> SystemPrompt:
    return load(GENERATOR_PROMPT, "generator")


def scrutiny() -> SystemPrompt:
    return load(SCRUTINY_PROMPT, "scrutiny")


def auditor() -> SystemPrompt:
    return load(AUDITOR_PROMPT, "auditor")
