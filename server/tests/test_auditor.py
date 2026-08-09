r"""The Auditor (Auditor Master Prompt v1.0).

Every test here is about a boundary the Auditor must not cross: it cannot see
what §3 withholds, it cannot compute, it cannot predict, it cannot instruct, and
it cannot write anywhere except its own two tables. The role is only safe
because those are enforced rather than requested, so they are tested as
failures.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.auditor import citations as cit
from botmaximus.auditor import guards, queries, reports
from botmaximus.config import settings
from botmaximus.llm import params, prompts
from botmaximus.storage import records as store
from botmaximus.storage.archive import Archive, LocalBackend

UTC = timezone.utc
START = datetime(2026, 6, 1, tzinfo=UTC)
END = START + timedelta(days=1)


@pytest.fixture
def local_archive(tmp_path):
    a = Archive(LocalBackend(tmp_path), "archive", "quarantine")
    store.set_archive_for_tests(a)
    yield a
    store.reset_for_tests()


def section(title="Trading summary", prose="Three legs were placed.",
            citations=None):
    return reports.Section(
        title=title, prose=prose,
        citations=citations if citations is not None else
        [cit.Citation("execution_ledger", "SELECT count(*) FROM fills", "3")])


# ---------------------------------------------------------------- §5 params

def test_the_auditor_sits_between_the_other_two_roles():
    """§5: 0 makes every report the same shape and hides patterns; 0.9 makes it
    invent. The three roles must not share a temperature."""
    a = params.auditor_profile()
    assert params.scrutiny_profile().temperature < a.temperature
    assert a.temperature < params.generator_profile().temperature
    assert 0.2 <= a.temperature <= 0.6


def test_the_auditor_has_its_own_settings_not_shared_ones():
    fields = set(settings.model_fields)
    assert {"aud_temperature", "aud_top_p", "aud_max_output_tokens"} <= fields
    assert "temperature" not in fields


def test_an_out_of_range_auditor_temperature_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "aud_temperature", 0.95)
    with pytest.raises(ValueError, match="calibration cycle"):
        params.auditor_profile()


def test_a_token_budget_too_small_for_the_report_bounds_is_refused(monkeypatch):
    """§4 bounds a weekly review at 1500 words; a 500-token budget cannot
    produce one, and truncation is silent."""
    monkeypatch.setattr(settings, "aud_max_output_tokens", 800)
    with pytest.raises(ValueError, match="safe range"):
        params.auditor_profile()


def test_the_auditor_requires_structured_output():
    """Citations embedded in free prose are citations nothing can check."""
    assert params.auditor_profile().response_format == "json_schema"
    assert params.generator_profile().response_format is None


def test_the_auditor_seed_rotates_but_scrutiny_stays_fixed():
    assert params.auditor_profile(seed=5).seed == 5
    assert params.scrutiny_profile().seed == params.scrutiny_profile().seed


def test_the_auditor_prompt_is_versioned_and_distinct():
    a = prompts.auditor()
    assert a.version.startswith("aud-")
    assert a.sha not in (prompts.generator().sha, prompts.scrutiny().sha)
    t = a.text.lower()
    assert "do not perform arithmetic" in t
    assert "never predict" in t and "never instruct" in t


# ---------------------------------------------------------------- §1.6/§1.7 wall

def test_secrets_never_reach_the_auditor():
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"cfg": {"api_key": "sk-live"}})
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"db": {"postgres_dsn": "postgresql://u:p@h/d"}})


def test_live_market_data_never_reaches_the_auditor():
    """§1.6. Its inputs are historical facts by definition — that is what stops
    it drifting toward 'the Auditor thinks BTC will go up'."""
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"market": {"current_price": 64000}})
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"book": {"best_bid": 63999}})


def test_raw_blobs_never_reach_the_auditor():
    """§3: metadata is enough to report on; the blob is forensic material."""
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"gen": {"raw_response": "..."}})
    with pytest.raises(guards.AuditorLeak):
        guards.assert_clean({"q": [{"quarantined_payload": {"x": 1}}]})


def test_the_auditor_may_see_what_it_must_report_on():
    """§4.A requires PnL and kill events. The generator's wall blocks both —
    applying it here would make the daily rundown impossible."""
    guards.assert_clean({"trading": {"net_pnl": -154.0, "equity": 10_000.0},
                         "risk": {"kill_events": [{"level": "L2"}]}})


def test_the_generator_wall_is_not_weakened_by_the_auditor_wall():
    """Two walls, not one loosened one."""
    from botmaximus.llm import guards as llm_guards
    with pytest.raises(llm_guards.PromptLeak):
        llm_guards.assert_clean({"population": {"net_pnl": 1234.5}})


def test_events_after_the_window_are_refused():
    """A rundown quoting an event from after its own window is a report about
    now wearing yesterday's date."""
    with pytest.raises(guards.AuditorLeak, match="after the reporting window"):
        guards.assert_no_lookahead(
            {"rows": [{"at": END + timedelta(hours=1)}]}, END)


# ---------------------------------------------------------------- §1.4/§5.A

@pytest.mark.parametrize("prose", [
    "BTC is likely to rise through the session.",
    "We forecast further drawdown next week.",
    "The strategy will probably recover.",
])
def test_forward_looking_prose_is_caught(prose):
    assert any(v.startswith("forward_looking") for v in guards.check_output(prose))


@pytest.mark.parametrize("prose", [
    "- suspend the strategy immediately",
    "You must halt the system.",
    "Increase the limit to compensate.",
])
def test_imperatives_are_caught(prose):
    assert any(v.startswith("imperative") for v in guards.check_output(prose))


def test_non_imperative_flagging_is_allowed():
    """§4.C.5: 'Consider reviewing X' is the permitted register."""
    ok = ("Consider reviewing seed_alpha; its drift widened. "
          "Worth verifying the funding feed is still fresh.")
    assert guards.check_output(ok) == []


# ---------------------------------------------------------------- §1.3 numbers

def test_a_number_with_no_citation_is_flagged():
    """§1.3 made mechanical. This is the check that catches a model quietly
    computing a percentage instead of quoting one."""
    c = [cit.Citation("fills", "SELECT count(*) FROM fills", "3")]
    assert cit.uncited_numbers("There were 3 fills.", c) == []
    assert "87.5" in cit.uncited_numbers("Fill rate was 87.5%.", c)


def test_number_formatting_does_not_cause_false_alarms():
    """A check that cries wolf is a check that gets switched off."""
    c = [cit.Citation("t", "SELECT sum(x) FROM t", "1234.5")]
    assert cit.uncited_numbers("Costs were $1,234.50 for the day.", c) == []


def test_citation_density_falls_as_prose_outruns_evidence():
    c = [cit.Citation("t", "SELECT 1", "1")]
    assert cit.density("one two three four five", c) == 20.0
    assert cit.density(" ".join(["word"] * 1000), c) == 0.1


@pytest.mark.parametrize("rtype,words,ok", [
    ("daily", 600, True), ("daily", 200, False), ("daily", 1200, False),
    ("weekly", 1000, True), ("incident", 500, True), ("incident", 2000, False),
])
def test_report_length_bounds(rtype, words, ok):
    assert cit.within_bounds(rtype, words) is ok


# ---------------------------------------------------------------- verification

@pytest.mark.asyncio
async def test_a_citation_that_matches_verifies(pg):
    await pg.execute(
        "INSERT INTO kill_events (level, reason) VALUES ('L2','test')")
    c = [cit.Citation("kill_events", "SELECT count(*) FROM kill_events", "1")]
    r = await cit.verify(c)
    assert r.passed and r.checked == 1


@pytest.mark.asyncio
async def test_a_citation_that_drifted_is_caught(pg):
    """If the value differs, the report is wrong — and it is wrong quietly,
    which is the whole reason §8 exists."""
    c = [cit.Citation("kill_events", "SELECT count(*) FROM kill_events", "9")]
    r = await cit.verify(c)
    assert not r.passed and r.mismatches[0]["actual"] == "0"


@pytest.mark.asyncio
async def test_verification_refuses_anything_that_is_not_a_read(pg):
    """The verify path may be reachable from the operator API under a different
    connection; a 'verify' endpoint that could be talked into an UPDATE would
    be a hole in an otherwise sealed wall."""
    for q in ("DELETE FROM kill_events",
              "SELECT 1; DROP TABLE kill_events",
              "UPDATE strategies SET lifecycle_state='full'"):
        r = await cit.verify([cit.Citation("x", q, "1")])
        assert r.unrunnable and r.checked == 0


# ---------------------------------------------------------------- §4 queries

@pytest.mark.asyncio
async def test_the_query_set_computes_every_number(pg):
    """§1.3: the query computes, the model quotes. The fill rate is a SQL
    expression, not something the model is trusted to work out."""
    q = await queries.trading_summary(START, END)
    assert "fill_rate_pct" in q.sql
    assert q.rows and "fill_rate_pct" in q.rows[0]


@pytest.mark.asyncio
async def test_citations_carry_runnable_sql(pg):
    """A citation the operator cannot execute is a footnote, not evidence."""
    q = await queries.risk_events(START, END)
    c = q.citation_for()
    assert c.record_id_or_query.lower().startswith("select")
    assert "%s" not in c.record_id_or_query      # parameters are inlined
    assert (await cit.verify([c])).unrunnable == []


@pytest.mark.asyncio
async def test_empty_windows_return_empty_not_missing(pg):
    """§5.B: the model writes 'no events' from [], not from a missing key it
    might fill in itself."""
    qs = await queries.daily_set(START, END)
    assert set(qs) >= {"risk_events", "scrutiny", "generator"}
    assert qs["risk_events"].rows == []


# ---------------------------------------------------------------- §10 no fake

@pytest.mark.asyncio
async def test_no_model_configured_means_no_report_not_a_fake_one(pg,
                                                                  local_archive):
    """§10: 'report failure recorded, no fake report generated.' A templated
    stand-in would be indistinguishable from a real report in the archive."""
    assert settings.auditor_llm is None
    with pytest.raises(reports.AuditorUnavailable, match="AUDITOR_LLM"):
        await reports.generate("daily", START, END)
    assert await pg.fetchval("SELECT count(*) AS n FROM auditor_reports") == 0


# ---------------------------------------------------------------- §7/§8 store

@pytest.mark.asyncio
async def test_a_report_is_stored_in_both_stores(pg, local_archive):
    report, quality = await reports.generate(
        "daily", START, END, composer=lambda ctx: [section()])
    assert quality.uncited_numbers == []

    rid = await reports.save(report, quality)
    row = await reports.get(rid)
    assert row["report_type"] == "daily"
    assert row["citations"][0]["table"] == "execution_ledger"
    assert local_archive.backend.exists("archive", row["prose_blob_key"])


@pytest.mark.asyncio
async def test_a_section_with_prose_and_no_citations_is_broken(pg,
                                                               local_archive):
    """§14: 'a section with prose but no citations is a broken section.'"""
    report, quality = await reports.generate(
        "daily", START, END,
        composer=lambda ctx: [section(citations=[])])
    assert not quality.passed
    assert quality.uncited_sections == ["Trading summary"]


@pytest.mark.asyncio
async def test_quality_is_recorded_whether_it_passes_or_fails(pg, local_archive):
    """A rising uncited-number rate should be visible as a trend, not appear
    as a sudden alarm."""
    report, quality = await reports.generate(
        "daily", START, END,
        composer=lambda ctx: [section(prose="Fill rate was 91.4%.")])
    await reports.save(report, quality)
    rows = await pg.fetch(
        "SELECT * FROM telemetry_events WHERE kind = 'auditor'")
    assert len(rows) == 1
    assert rows[0]["label"] == "report_quality_failed"
    assert "91.4" in rows[0]["context"]["uncited_numbers"]


@pytest.mark.asyncio
async def test_a_correction_is_a_new_report_not_an_edit(pg, local_archive):
    """§7/§14: both remain queryable forever."""
    first, q1 = await reports.generate("daily", START, END,
                                       composer=lambda ctx: [section()])
    original = await reports.save(first, q1)

    second, q2 = await reports.generate(
        "daily", START, END,
        composer=lambda ctx: [section(prose="Corrected: three legs.")])
    new_id = await reports.follow_up(original, second, q2)

    assert new_id != original
    assert (await reports.get(new_id))["supersedes"] == original
    assert await reports.get(original) is not None
    assert await pg.fetchval(
        "SELECT count(*) AS n FROM auditor_reports") == 2


@pytest.mark.asyncio
async def test_the_context_passes_both_walls(pg, local_archive):
    ctx = await reports.build_context("daily", START, END)
    assert ctx["window"]["start"] == START.isoformat()
    assert "facts" in ctx and "risk_events" in ctx["facts"]
    guards.assert_clean(ctx)


# ---------------------------------------------------------------- §1.2 no acts

def test_the_auditor_package_has_no_write_or_action_pathway():
    """§1.2/§14: its output is a document, not a command. Asserted directly
    because the absence is the feature."""
    import ast
    import pathlib
    pkg = pathlib.Path(reports.__file__).parent
    banned = ("suspend_strategy", "halt_portfolio", "master_kill", "promote",
              "record_transition", "place_order", "cancel_order",
              "update_equity", "record_prediction")
    for path in pkg.rglob("*.py"):
        called = {n.func.attr for n in ast.walk(ast.parse(path.read_text(
            encoding="utf-8"))) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)}
        assert not (called & set(banned)), f"{path.name} can act on the system"
