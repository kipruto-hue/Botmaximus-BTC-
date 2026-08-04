"""Single source of truth for "is the collector actually working?".

Both supervisors call this — `supervise.ps1` on Windows and the systemd
watchdog timer on the VPS — so there is exactly one definition of unhealthy
rather than two that drift.

## Liveness is the wrong question

The failure this project has already hit twice is *working, but wrong*. The
2026-04-23 Binance routing migration left the socket connected and SUBSCRIBE
acking success while `/market` streams pushed nothing at all. A supervisor that
asks "is the process up?" sees a perfectly healthy service and lets the silence
run for hours — and for liquidations and order book, which have no history
endpoint, every one of those hours is permanently lost data.

So the probe is keyed on **data freshness**: has each feed stored a record
inside its own budget? `telemetry.snapshot()` already computes this per feed,
correctly leaving event-driven feeds (liquidations) out of it — their budget is
`None` because silence there is genuinely not absence.

## Stateless on purpose

This reports the state *now* and exits. It does not decide whether to restart:
one stale sample during a venue hiccup is not a reason to bounce a process and
trigger a reconnect storm. Counting consecutive failures — and backing off — is
the supervisor's job, because only the supervisor knows how many times it has
already tried.

Exit codes:
    0  healthy
    1  unhealthy (reason on stdout)
    2  unreachable — the API is not answering at all
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "http://127.0.0.1:8300"


def probe(base: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(f"{base}/api/telemetry", timeout=timeout) as r:
            tele = json.loads(r.read())
        with urllib.request.urlopen(f"{base}/api/health", timeout=timeout) as r:
            health = json.loads(r.read())
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        return 2, f"unreachable: {e}"
    except json.JSONDecodeError as e:
        return 2, f"malformed response: {e}"

    if not health.get("mongo"):
        return 1, "mongo not responding to ping"

    stale = [f["dataset_id"] for f in tele.get("feeds", []) if f.get("stale")]
    if stale:
        ages = {f["dataset_id"]: f["fresh"] for f in tele["feeds"] if f.get("stale")}
        return 1, f"stale feeds {stale} (ages ms: {ages})"

    down = [s for s, up in (tele.get("ws_sources") or {}).items() if not up]
    if down:
        return 1, f"websocket sources down: {down}"

    # A process that has never stored anything is not healthy just because
    # nothing has gone stale yet — an empty telemetry snapshot has no stale feeds.
    if not tele.get("feeds") or all(f.get("fresh") is None for f in tele["feeds"]):
        return 1, "no feed has stored a record yet"

    # ASCII only: this string lands in supervisor logs and the Windows console,
    # where a non-cp1252 character comes out as a replacement glyph.
    return 0, (f"ok: {len(tele['feeds'])} feeds fresh, "
               f"uptime {tele.get('uptime_s')}s, "
               f"{tele.get('counts', {}).get('stored', 0):,} stored")


def main() -> int:
    ap = argparse.ArgumentParser(description="BOTMAXIMUS freshness probe")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    code, reason = probe(a.url, a.timeout)
    if not a.quiet or code != 0:
        print(reason)
    return code


if __name__ == "__main__":
    sys.exit(main())
