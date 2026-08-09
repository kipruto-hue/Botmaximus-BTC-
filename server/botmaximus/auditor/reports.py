r"""Report composition and storage (§4, §7, §8, §10).

The Auditor's shape is: **queries produce facts → a model turns facts into
prose → the prose is verified against the facts → the whole thing is stored as
an immutable record.**

Step three is what makes step two safe. Without verification, a report is an
assertion by a language model about numbers it was shown; with it, every figure
in the prose is traceable to a statement the operator can re-run.

## No key, no report — never a fake one

§10: *"report failure recorded, no fake report generated."* When `AUDITOR_LLM`
is unset there is no provider, and `generate()` raises. It does not fall back to
a template that stitches the query results into sentences. That fallback would
be indistinguishable from a real report in the dashboard, in the archive, and in
six months — and the one thing worse than no analyst is one whose provenance
says a model wrote something no model ever saw.

## Reports are never edited

§7 and §14: a report that quoted a value later superseded gets a **follow-up**
report with `supersedes` set. Both remain queryable forever. The Auditor's
database role has INSERT and no UPDATE on its own tables, so this is a
permission rather than a convention.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from botmaximus.auditor import citations as cit
from botmaximus.auditor import guards, queries
from botmaximus.config import settings
from botmaximus.storage import postgres
from botmaximus.storage import records as store

log = logging.getLogger(__name__)

UTC = timezone.utc


class AuditorUnavailable(RuntimeError):
    """No model is configured, or the provider failed. No report is written."""


@dataclass
class Section:
    title: str
    prose: str
    citations: list[cit.Citation] = field(default_factory=list)


@dataclass
class Report:
    report_id: str
    report_type: str
    window_start: datetime
    window_end: datetime
    trigger: str
    sections: list[Section] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    supersedes: str | None = None

    @property
    def prose(self) -> str:
        return "\n\n".join(f"## {s.title}\n{s.prose}" for s in self.sections)

    @property
    def all_citations(self) -> list[cit.Citation]:
        return [c for s in self.sections for c in s.citations]

    @property
    def word_count(self) -> int:
        return cit.word_count(self.prose)


@dataclass
class QualityCheck:
    """§8 metrics on the report itself, computed at write time."""
    word_count: int
    within_bounds: bool
    citation_density: float
    uncited_numbers: list[str]
    uncited_sections: list[str]
    output_violations: list[str]

    @property
    def passed(self) -> bool:
        return (self.within_bounds
                and self.citation_density >= settings.aud_min_citation_density
                and len(self.uncited_numbers) <= settings.aud_max_uncited_numbers
                and not self.uncited_sections
                and not self.output_violations)

    def to_dict(self) -> dict:
        return {
            "word_count": self.word_count,
            "within_bounds": self.within_bounds,
            "citation_density": self.citation_density,
            "uncited_numbers": self.uncited_numbers,
            "uncited_sections": self.uncited_sections,
            "output_violations": self.output_violations,
            "passed": self.passed,
        }


def check_quality(report: Report) -> QualityCheck:
    """Everything §8 asks for, computed without touching the database.

    `allow` exempts the window's own dates: a report is entitled to say which
    day it covers without citing a ledger row for the number 2026.
    """
    allow = set()
    for d in (report.window_start, report.window_end, report.generated_at):
        allow.update({str(d.year), str(d.month), str(d.day),
                      str(d.hour), f"{d:%Y%m%d}"})
    allow.update({"0", "1", "7", "24", "30", "100"})   # counts and windows

    prose = report.prose
    return QualityCheck(
        word_count=report.word_count,
        within_bounds=cit.within_bounds(report.report_type, report.word_count),
        citation_density=cit.density(prose, report.all_citations),
        uncited_numbers=cit.uncited_numbers(prose, report.all_citations, allow),
        # §14: "a section with prose but no citations is a broken section."
        uncited_sections=[s.title for s in report.sections
                          if s.prose.strip() and not s.citations],
        output_violations=guards.check_output(prose),
    )


# ---------------------------------------------------------------- generation

async def build_context(report_type: str, start: datetime,
                        end: datetime) -> dict:
    """Assemble the §5.B context: window, query results, prior headlines.

    Runs both walls before returning. The lookahead check matters more than it
    looks — a daily rundown quoting an event from after its own window is a
    report about now wearing yesterday's date.
    """
    qs = await queries.daily_set(start, end)
    ctx = {
        "report_type": report_type,
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "facts": {name: {"sql": q.sql, "rows": q.rows} for name, q in qs.items()},
        "prior_headlines": await prior_headlines(limit=7),
    }
    guards.assert_clean(ctx)
    guards.assert_no_lookahead(qs, end)
    return ctx


async def prior_headlines(limit: int = 7) -> list[dict]:
    rows = await postgres.fetch(
        "SELECT report_type, generated_at, sections->0->>'title' AS headline "
        "FROM auditor_reports ORDER BY generated_at DESC LIMIT %s", (limit,))
    return [json.loads(json.dumps(dict(r), default=str)) for r in rows]


def provider():
    """The Auditor's model. Unreachable until AUDITOR_LLM and a key are set.

    Mirrors the Generator's `LiveProposer`: the live path is unreachable by
    construction rather than by convention, so a half-configured deployment
    fails loudly instead of quietly writing something.
    """
    if not settings.auditor_llm:
        raise AuditorUnavailable(
            "AUDITOR_LLM is not set. Refusing to produce a report: §10 is "
            "explicit that a failure is recorded and no fake report is "
            "generated. A templated stand-in would be indistinguishable from a "
            "real report in the dashboard and in the archive.")
    raise AuditorUnavailable(
        f"no provider wired for AUDITOR_LLM={settings.auditor_llm!r}. The live "
        f"LLM path is not reachable in this build.")


async def generate(report_type: str, start: datetime, end: datetime,
                   trigger: str = "scheduled",
                   composer=None) -> tuple[Report, QualityCheck]:
    """Produce and verify a report. `composer` is injected in tests.

    A composer receives the assembled context and returns sections; production
    passes the model. Either way the output goes through the same quality
    checks — the verification is not a test harness, it is the contract.
    """
    ctx = await build_context(report_type, start, end)
    compose = composer or (lambda _c: provider())
    sections = compose(ctx)

    report = Report(report_id=str(uuid.uuid4()), report_type=report_type,
                    window_start=start, window_end=end, trigger=trigger,
                    sections=sections)
    return report, check_quality(report)


# ---------------------------------------------------------------- storage

async def save(report: Report, quality: QualityCheck,
               provenance_id: str | None = None,
               profile=None, prompt_version: str | None = None,
               context_hash: str | None = None) -> str:
    """Prose to Parquet, metadata and citations to Postgres (§7)."""
    key = (f"auditor_reports/year={report.generated_at:%Y}"
           f"/month={report.generated_at:%m}/day={report.generated_at:%d}"
           f"/{report.report_id}.parquet")
    table = pa.table({
        "section": pa.array([s.title for s in report.sections], pa.string()),
        "prose": pa.array([s.prose for s in report.sections], pa.string()),
    })
    written = store.archive().write_blob(
        key, table, dataset_id="auditor_report",
        partition=key.rsplit("/", 1)[0])
    await store._record_manifest(written)

    prov_id = provenance_id
    if profile is not None:
        prov_id = prov_id or str(uuid.uuid4())
        await postgres.execute(
            "INSERT INTO auditor_provenance (provenance_id, model_id, "
            " prompt_version, context_hash, profile_fingerprint, seed, "
            " temperature, top_p, max_output_tokens, output_response_hash, "
            " code_version, producer) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (prov_id, settings.auditor_llm, prompt_version, context_hash,
             profile.fingerprint(), profile.seed, profile.temperature,
             profile.top_p, profile.max_output_tokens,
             hashlib.sha256(report.prose.encode()).hexdigest()[:16],
             _code_version(), _producer()))

    await postgres.execute(
        "INSERT INTO auditor_reports (report_id, report_type, window_start, "
        " window_end, trigger, generated_at, citations, sections, word_count, "
        " prose_blob_key, supersedes, provenance_ref) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (report.report_id, report.report_type, report.window_start,
         report.window_end, report.trigger, report.generated_at,
         json.dumps([c.to_dict() for c in report.all_citations]),
         json.dumps([{"title": s.title, "words": cit.word_count(s.prose),
                      "citations": len(s.citations)} for s in report.sections]),
         report.word_count, written.key, report.supersedes, prov_id))

    await _record_quality(report, quality)
    return report.report_id


async def follow_up(original_id: str, report: Report,
                    quality: QualityCheck) -> str:
    """§7: a report that quoted a since-corrected value is superseded by a new
    one. The original is not edited and not deleted; both stay queryable."""
    report.supersedes = original_id
    return await save(report, quality)


async def _record_quality(report: Report, q: QualityCheck) -> None:
    """§8 metrics into telemetry, pass or fail.

    Recorded on every report so a rising uncited-number rate is visible as a
    trend rather than as a sudden alarm.
    """
    await postgres.execute(
        "INSERT INTO telemetry_events (kind, label, reason, context) "
        "VALUES ('auditor', %s, %s, %s)",
        ("report_quality" if q.passed else "report_quality_failed",
         f"{report.report_type} report {report.report_id}",
         json.dumps(q.to_dict(), default=str)))


async def latest(report_type: str | None = None, limit: int = 20) -> list[dict]:
    sql = "SELECT * FROM auditor_reports "
    params: tuple = (limit,)
    if report_type:
        sql += "WHERE report_type = %s "
        params = (report_type, limit)
    sql += "ORDER BY generated_at DESC LIMIT %s"
    rows = await postgres.fetch(sql, params)
    return [json.loads(json.dumps(dict(r), default=str)) for r in rows]


async def get(report_id: str) -> dict | None:
    row = await postgres.fetchrow(
        "SELECT * FROM auditor_reports WHERE report_id = %s", (report_id,))
    return json.loads(json.dumps(dict(row), default=str)) if row else None


def yesterday_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(UTC)
    end = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return end - timedelta(days=1), end


def last_week_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    now = now or datetime.now(UTC)
    end = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return end - timedelta(days=7), end


def _code_version() -> str:
    from botmaximus.storage.envelope import _code_version as cv
    return cv()


def _producer() -> str:
    from botmaximus.storage.envelope import producer
    return producer()
