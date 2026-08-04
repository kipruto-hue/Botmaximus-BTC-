# BOTMAXIMUS — Operations

Covers the two failures that destroy work rather than merely interrupt it:
**the collector going quiet** and **the database disappearing**. Both are
addressed in `ops/`. Neither is fully solved on a desktop.

Written 2026-08-04. Closes §3.1 (partially) and §3.2 of `STRUCTURAL_RISKS.md`.

---

## Why supervision is keyed on freshness, not liveness

Every supervisor here restarts the collector when it goes **silent**, not only
when it dies. The reason is a failure this project has already hit: the
2026-04-23 Binance websocket routing migration left the socket connected and
`SUBSCRIBE` acking success while `/market` streams delivered nothing. A
`Restart=always` policy sees that as a perfectly healthy service.

That matters more here than in most systems, because some of this data cannot
be re-fetched at any price:

| Feed | Recoverable after an outage? |
|---|---|
| `btc_ohlcv_1m` | Yes — REST klines, paginated |
| `btc_funding_8h` | Yes — settled series endpoint |
| `btc_oi_5m` | **Only within 30 days** — venue hard limit |
| `btc_liquidations` | **Never** — no history endpoint exists |
| `btc_orderbook` | **Never** — no history endpoint exists |

An hour of downtime is an hour of liquidation and order-book history that is
gone permanently, and a hole the coverage gate will correctly refuse to backtest
across forever after.

`ops/healthcheck.py` is the single definition of "unhealthy", called by both
supervisors so the desktop and the VPS cannot drift apart. Exit `0` healthy,
`1` stale/degraded, `2` API unreachable.

It is stateless by design. One bad sample is not a reason to restart — deciding
that requires counting consecutive failures and knowing how many restarts have
already been attempted, which is the supervisor's job.

---

## Windows (current, a stopgap)

```powershell
# install autostart + daily backup, then start it now
powershell -ExecutionPolicy Bypass -File ops\install-tasks.ps1 -BackupDest D:\botmaximus-backups
Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass',`
  '-WindowStyle','Hidden','-File','ops\supervise.ps1','-BackupDest','D:\botmaximus-backups'
```

`ops/supervise.ps1` keeps mongod and the collector up, probes every 30s,
restarts after **4 consecutive** bad probes, and caps restarts at **6/hour**
before halting and demanding an operator. That ceiling is deliberate: a tight
restart loop reconnecting to Binance risks a rate-limit ban, which would cost
far more coverage than the outage it is trying to fix.

Log: `data/supervisor.log`. A healthy supervisor prints a heartbeat every 30
minutes — without it, "alive and fine" and "died silently" both look like
silence, which is how a crashed supervisor went unnoticed during this very
build.

### Known limits of the Windows path

- **Starts at logon, not at boot.** Registering a Scheduled Task needs admin;
  without it the installer falls back to a Startup-folder shortcut. An
  unattended reboot leaves the collector down until someone signs in.
- **The Startup-shortcut fallback does not restart the supervisor if it exits.**
  Re-run `install-tasks.ps1` from an elevated shell to get the real task.
- **The daily backup runs inline** in the supervisor (a separate task would need
  admin too), so there is a ~4 minute window each day with no health probing.
  On the VPS this is properly split into its own timer.
- **The machine sleeps.** Nothing here prevents that, and sleep is downtime.

None of this is fixed by more code. It is why the VPS path exists.

### A gap the strike counter does not cover

Strikes reset on any healthy probe, so a feed that **flaps** — stale, fresh,
stale again — never accumulates the 4 consecutive failures needed to trigger a
restart, while still being degraded. This is visible in `data/supervisor.log`
from the first hours of running: `btc_ohlcv_1m` reached 458s stale and
recovered on the next probe.

That is the intended trade-off rather than an oversight: flapping of that shape
is almost always venue-side delivery jitter, and restarting the collector would
not fix it while costing a reconnect. But it means **flapping is invisible to
the restart policy and only shows up in the log**. The right response is an
alert, not a restart, and no alerting is wired up — checking the log is
currently a manual job.

---

## VPS (the actual fix)

Units in `ops/systemd/`. Install to `/opt/botmaximus`, user `botmaximus`:

```bash
cp ops/systemd/*.service ops/systemd/*.timer /etc/systemd/system/
mkdir -p /etc/botmaximus
echo 'BACKUP_DEST=/mnt/backups/botmaximus' > /etc/botmaximus/backup.env
systemctl daemon-reload
systemctl enable --now botmaximus.service
systemctl enable --now botmaximus-watchdog.timer
systemctl enable --now botmaximus-backup.timer
```

| Unit | Does |
|---|---|
| `botmaximus.service` | Runs the collector. `Restart=always`, 6 starts/hour ceiling |
| `botmaximus-watchdog.timer` | Probes freshness every 2 min, restarts on failure |
| `botmaximus-backup.timer` | Daily verified dump at 03:20 UTC, keeps 7 |

`After=mongod.service` is ordering only, not `Requires=` — a hard dependency
would take the collector down every time mongod restarts, and the collector's
own retry loop already handles a database that is briefly unavailable.

The watchdog timer waits 3 minutes after boot before its first probe: the
collector runs a 7-day backfill scan on startup, and probing before it has
stored anything reads as unhealthy and restart-loops it.

---

## Backups

`ops/mongo_backup.py`. Not `mongodump` — that tool is not in the portable
MongoDB zip this project ships, and more importantly the high-frequency series
are **time-series collections**, which `mongodump` dumps at the internal
`system.buckets.*` level. That bucket layout is version-coupled, and restoring
into a different server version is exactly the situation a disaster recovery is
for. So this dumps **measurements** and recreates the time-series collections
from their captured options on restore.

```powershell
python ops\mongo_backup.py dump    --dest D:\botmaximus-backups --keep 7
python ops\mongo_backup.py verify  --path D:\botmaximus-backups\botmaximus-<stamp>
python ops\mongo_backup.py restore --path <...> --into botmaximus_restored
python ops\mongo_backup.py self-test
```

- **Dumps verify themselves** by default — every file is re-read and its
  checksum re-derived. An unverified backup is an assumption, and a dump that
  cannot be read back is worse than none because it is trusted.
- **Pruning happens only after a successful verify**, so a failed backup can
  never delete the last good one.
- **Writes to `.partial` and renames**, so an interrupted run cannot leave
  something that looks complete.
- **Restore refuses a non-empty target** without `--force`.
- **`self-test` round-trips a real dump** through a scratch database and asserts
  that measurements, time-series options, TTLs and unique indexes all survived.
  Run it after touching that file — a hand-rolled format that rots silently is
  the main risk of not using `mongodump`.

Current baseline: **2,144,148 documents, 72 MB gzipped, ~4 minutes**.

### Off-machine, or it doesn't count

The default destination is `D:\botmaximus-backups`. On this machine D: is
physical disk 0 and the database is on disk 1, so it survives a disk failure —
the failure that would otherwise take the entire 2-year history.

It does **not** survive theft, fire, ransomware or a wiped machine, because it is
still the same box. Syncing that directory to remote storage is the remaining
step and it is not automated here.

---

## Restoring

```powershell
python ops\mongo_backup.py verify  --path <backup>          # always first
python ops\mongo_backup.py restore --path <backup> --into botmaximus_restored
# inspect, then point the server at it or rename the database
```

Restore into a **new** database name and inspect before switching. Restoring
over a live database with `--force` during an incident is how a bad backup
becomes a total loss.

After any restore, run the coverage reconcile on boot (it happens automatically)
and check `GET /api/coverage` before trusting a backtest — the coverage ledger
is restored data like anything else, and the whole point of it is that it
matches reality.
