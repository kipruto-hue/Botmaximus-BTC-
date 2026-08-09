r"""Which venue a record came from (Storage v2.0, and the venue-separation rule).

Binance and Bybit disagree about price, funding, liquidity and even what a bar
contains. That separation used to be enforced by writing each era to a different
*database*. Once both eras share one Postgres, the only thing keeping them apart
is this discriminator — so it is derived from the record's own `source`, never
from `settings.venue`.

That distinction is the whole point. During the Mongo carryover the process is
configured for `bybit` while writing Binance-era history; a venue read from
config would stamp every one of those million candles `bybit` and silently
merge the two series into one. `source` travels with the record and cannot lie
about where it came from.

Unknown sources raise. A default here would be a guess about which exchange a
price came from, and a wrong guess is indistinguishable from real data once
written.
"""
from __future__ import annotations

#: Must match the CHECK constraint in schema.sql. Adding a venue means adding
#: it in both places — deliberately, because a new venue needs its own history,
#: its own coverage and its own answer to "may these be compared".
VENUES = ("bybit", "binance")


class UnknownVenue(ValueError):
    """A record's source does not identify a known venue."""


def venue_of(source: str) -> str:
    """The venue a `source` string belongs to.

    Sources look like `bybit_v5_ws`, `bybit_v5_rest`, `binance_futures_ws`.
    Matching on prefix keeps new transports (a second websocket, a REST
    backfill) from needing a code change here, while a genuinely unknown
    exchange still fails loudly.
    """
    s = (source or "").strip().lower()
    for v in VENUES:
        if s.startswith(v):
            return v
    raise UnknownVenue(
        f"cannot determine the venue for source {source!r}. Every record needs "
        f"an explicit venue: the Binance and Bybit series disagree about price, "
        f"funding and liquidity, and nothing downstream filters on `source`. "
        f"Known venues: {', '.join(VENUES)}.")


def require_venue(venue: str) -> str:
    """Validate a venue supplied by a caller (a read path, a migration)."""
    if venue not in VENUES:
        raise UnknownVenue(
            f"{venue!r} is not a known venue; expected one of "
            f"{', '.join(VENUES)}")
    return venue
