r"""Trading-session window ("killzone") filter.

Loaded from `config.yaml`, because the hours are an operator decision and
burying them in code is how they drift out of step with intent.

## Two things the spec got tangled, recorded here rather than silently resolved

**The window is not the London Killzone.** 15:00-17:00 `Africa/Nairobi` is
12:00-14:00 UTC, which is 08:00-10:00 New York -- the *New York* killzone. The
London Killzone (07:00-10:00 London) would be 09:00-12:00 Nairobi. The config
uses the hours that were given and names them `new_york_killzone`, for what
they actually are.

**15:00-17:00 and "3:30PM-5:30PM" are different windows.** The explicit
`start_hour: 15 / end_hour: 17` is implemented. If 15:30-17:30 was meant, it is
a one-line config change -- but it must be a decision, not a guess.

## Entries are gated; exits never are

A session filter that blocks *exits* is not a filter, it is an unbounded hold:
a position opened at 16:58 would sit through the night because the clock rolled
past the window. Stops, targets, time exits and kills must always be allowed to
act. `may_enter()` is the gate; there is deliberately no `may_exit()`.

## Naive local time is not used anywhere

`datetime.now()` on a UTC-clocked VPS is not Nairobi time, and the same code
would behave differently on the desktop and on the server. Everything here is
timezone-aware and converted explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Session:
    enabled: bool
    tz: str
    start: time
    end: time
    label: str = ""

    @classmethod
    def from_config(cls, cfg: dict) -> "Session":
        s = (cfg or {}).get("session", {}) or {}
        return cls(
            enabled=bool(s.get("enabled", True)),
            tz=s.get("timezone", "Africa/Nairobi"),
            start=_parse_hhmm(s.get("start", "15:00")),
            end=_parse_hhmm(s.get("end", "17:00")),
            label=s.get("label", ""),
        )

    @classmethod
    def from_settings(cls) -> "Session | None":
        """Build from `TRADE_WINDOW_LOCAL`, e.g. `Africa/Nairobi:15:30-17:30`.

        Returns None when unset. The setting has documented that format since it
        was introduced and nothing ever parsed it — the TradeLoop is the first
        caller, and a window nobody reads is a window that is not enforced.

        A malformed value raises rather than defaulting to "always open":
        silently trading around the clock because a colon was missing is
        exactly the class of configuration accident this project refuses.
        """
        from botmaximus.config import settings

        raw = (settings.trade_window_local or "").strip()
        if not raw:
            return None
        try:
            tz, _, span = raw.partition(":")
            start_s, _, end_s = span.partition("-")
            return cls(enabled=True, tz=tz.strip(),
                       start=_parse_hhmm(start_s.strip()),
                       end=_parse_hhmm(end_s.strip()), label=raw)
        except Exception as e:                          # noqa: BLE001
            raise ValueError(
                f"TRADE_WINDOW_LOCAL={raw!r} is not "
                f"'<Area/City>:<HH:MM>-<HH:MM>' ({e}). Refusing to fall back to "
                f"an always-open window — that would trade around the clock "
                f"because of a typo.") from e

    @property
    def zone(self) -> ZoneInfo:
        # Raises immediately on an unknown zone rather than silently falling
        # back to UTC, which would shift the window by three hours and look
        # like a strategy that stopped taking trades.
        return ZoneInfo(self.tz)

    def local_now(self, now: datetime | None = None) -> datetime:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError(
                "naive datetime passed to the session filter — on a UTC-clocked "
                "VPS that silently means something different than on the desktop")
        return now.astimezone(self.zone)

    def may_enter(self, now: datetime | None = None) -> bool:
        """True when a NEW position may be opened. Exits are never gated."""
        if not self.enabled:
            return True
        t = self.local_now(now).time()
        if self.start <= self.end:
            return self.start <= t < self.end
        # Window crossing midnight (e.g. 22:00-02:00): inside if past the start
        # OR before the end. The naive `start <= t < end` is empty for these.
        return t >= self.start or t < self.end

    def seconds_until_open(self, now: datetime | None = None) -> int:
        """How long to sleep before the window opens. 0 when already inside.

        Lets the runner idle to the edge of the session instead of waking every
        few seconds through eighteen closed hours.
        """
        if not self.enabled or self.may_enter(now):
            return 0
        local = self.local_now(now)
        target = local.replace(hour=self.start.hour, minute=self.start.minute,
                               second=0, microsecond=0)
        if target <= local:
            target = target.replace(day=local.day) + _ONE_DAY
        return max(1, int((target - local).total_seconds()))

    def describe(self, now: datetime | None = None) -> str:
        local = self.local_now(now)
        state = "OPEN" if self.may_enter(now) else "CLOSED"
        return (f"session {self.label or self.tz} {self.start:%H:%M}-{self.end:%H:%M} "
                f"{self.tz}: {state} (local {local:%Y-%m-%d %H:%M %Z})")


from datetime import timedelta as _td  # noqa: E402

_ONE_DAY = _td(days=1)


def _parse_hhmm(v) -> time:
    """Accepts "15:00", "15", or an int hour. YAML unquoted 15:00 can parse as
    a sexagesimal int in some loaders, so ints are handled rather than
    crashing on a config that looks correct in the file."""
    if isinstance(v, time):
        return v
    if isinstance(v, int):
        return time(hour=v)
    s = str(v).strip()
    if ":" not in s:
        return time(hour=int(s))
    hh, mm = s.split(":", 1)
    return time(hour=int(hh), minute=int(mm[:2]))
