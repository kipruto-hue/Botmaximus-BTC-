-- BOTMAXIMUS — Postgres schema (Storage Architecture v2.0 §1.A, §3)
--
-- Postgres owns everything the system decides or commits. The design leans on
-- constraints deliberately: an invariant enforced by the database fails at
-- write time, where it is cheap and obvious, instead of being discovered later
-- in a report that quietly disagrees with itself.
--
-- Nothing here is ever UPDATEd in place except to close a system-time interval
-- (valid_to_sys). Corrections are new rows with `supersedes` set.

CREATE SCHEMA IF NOT EXISTS bmx;
SET search_path TO bmx, public;

-- =====================================================================
-- market data hot window (§3.A) — time-partitioned, one partition per day
-- =====================================================================
CREATE TABLE IF NOT EXISTS market_records (
    record_id            uuid        NOT NULL,
    dataset_id           text        NOT NULL,
    source               text        NOT NULL,
    -- Binance and Bybit disagree about price, funding, liquidity and even what
    -- a bar contains. That separation used to be enforced by using a different
    -- DATABASE per venue; once both eras live in one store, the only thing
    -- standing between them is this column. Nothing downstream ever filtered on
    -- `source`, so a nullable or free-text venue would silently blend two
    -- venues into one series — the allowlist makes a third phantom venue a
    -- write-time error instead of a quiet corruption.
    venue                text        NOT NULL CHECK (venue IN ('bybit','binance')),
    symbol               text,
    event_time           timestamptz NOT NULL,
    collection_time      timestamptz NOT NULL,
    ingest_time          timestamptz NOT NULL,
    valid_from_sys       timestamptz NOT NULL,
    valid_to_sys         timestamptz,
    supersedes           uuid,
    correction_reason    text,
    producer             text        NOT NULL,
    code_version         text        NOT NULL,
    schema_version       int         NOT NULL,
    config_hash          text,
    quality_flags        text[]      NOT NULL DEFAULT '{}',
    quality_ok           boolean     NOT NULL,
    quality_gate_version int         NOT NULL,
    -- Advisory marks on records that PASSED (single_source, illiquid_window,
    -- backfill). Kept apart from quality_flags so "clean" and "annotated"
    -- stay distinguishable — the gate marks every record single_source, and
    -- folding that into quality_flags would quarantine the entire feed.
    annotations          text[]      NOT NULL DEFAULT '{}',
    stage_latency_ms     jsonb,
    payload              jsonb       NOT NULL,

    -- The partition key must be part of every unique constraint, so identity
    -- is (record_id, event_time) rather than record_id alone.
    PRIMARY KEY (record_id, event_time),

    -- Only clean records reach this table; failures go to quarantine and never
    -- enter the store the backtester reads (P4).
    CONSTRAINT market_records_quality_ok CHECK (quality_ok),
    -- A clean record cannot also be flagged; that ambiguity is how bad data
    -- gets treated as good.
    CONSTRAINT market_records_flags_empty CHECK (cardinality(quality_flags) = 0),
    -- An open interval must not close before it opens.
    CONSTRAINT market_records_sys_interval
        CHECK (valid_to_sys IS NULL OR valid_to_sys > valid_from_sys)
) PARTITION BY RANGE (event_time);

-- Venue leads both indexes because every read path is required to specify one;
-- a query that forgot to would not merely be slow, it would be wrong.
CREATE INDEX IF NOT EXISTS market_records_dataset_event
    ON market_records (venue, dataset_id, event_time DESC);
-- Partial index: as-of queries overwhelmingly ask for current truth, and
-- indexing only open intervals keeps that lookup small as corrections pile up.
CREATE INDEX IF NOT EXISTS market_records_current
    ON market_records (venue, dataset_id, event_time DESC)
    WHERE valid_to_sys IS NULL;

-- The real dedupe key. `record_id` is minted per observation, so two sightings
-- of the same candle (a websocket reconnect replaying the last closed bar, a
-- backfill overlapping live data) get two ids and the primary key would happily
-- store both. What must be unique is the FACT: one current record per venue,
-- dataset and event time.
--
-- Partial on `valid_to_sys IS NULL` so corrections still work — a superseded
-- row leaves the index and the replacement takes its place, which is exactly
-- the §2 supersession model. Uniqueness holds globally despite partitioning
-- because event_time is the partition key, so equal event times always land in
-- the same partition.
CREATE UNIQUE INDEX IF NOT EXISTS market_records_natural_key
    ON market_records (venue, dataset_id, event_time)
    WHERE valid_to_sys IS NULL;

-- =====================================================================
-- coverage ledger (§3.B)
-- =====================================================================
-- Venue is part of the key, not a detail: "we have complete OHLCV for this
-- minute" is a different claim on Bybit than on Binance, and a single row per
-- (feed, slot) would let one venue's uptime vouch for the other's data.
CREATE TABLE IF NOT EXISTS coverage_ledger (
    venue       text        NOT NULL CHECK (venue IN ('bybit','binance')),
    feed        text        NOT NULL,
    slot        timestamptz NOT NULL,
    state       text        NOT NULL CHECK (state IN ('complete','partial','missing')),
    source      text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (venue, feed, slot)
);
CREATE INDEX IF NOT EXISTS coverage_state
    ON coverage_ledger (venue, feed, state, slot);

-- =====================================================================
-- strategies (§3.E) — retirement is a state, never a deletion
-- =====================================================================
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id     text        NOT NULL,
    version         int         NOT NULL,
    definition_hash text        NOT NULL,
    lifecycle_state text        NOT NULL
        CHECK (lifecycle_state IN ('candidate','paper','micro','full','retired')),
    origin          text        NOT NULL,
    parent_id       text,
    lineage_depth   int         NOT NULL DEFAULT 0,
    signature       text[]      NOT NULL DEFAULT '{}',
    rationale       text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, version)
);

CREATE TABLE IF NOT EXISTS strategy_definitions_blob (
    definition_hash text PRIMARY KEY,
    definition      jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Append-only strategy event log. Separate from `strategy_lifecycle_events`
-- because these are not state transitions and must NOT carry that table's
-- foreign key: the sealed-holdout burn is recorded for a candidate that may
-- never become a row in `strategies`, and a burn that fails to write because
-- of a missing parent would hand out a second look at the holdout — which is
-- selection wearing validation's clothes.
CREATE TABLE IF NOT EXISTS strategy_events (
    event_id    bigserial PRIMARY KEY,
    strategy_id text        NOT NULL,
    event       text        NOT NULL,
    at          timestamptz NOT NULL DEFAULT now(),
    detail      jsonb
);
CREATE INDEX IF NOT EXISTS strategy_events_by_strategy
    ON strategy_events (strategy_id, event);

CREATE TABLE IF NOT EXISTS strategy_lifecycle_events (
    event_id    bigserial PRIMARY KEY,
    strategy_id text        NOT NULL,
    version     int         NOT NULL,
    from_state  text,
    to_state    text        NOT NULL,
    reason      text        NOT NULL,
    actor       text        NOT NULL,      -- 'operator' | 'auto'
    verdict     jsonb,
    at          timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (strategy_id, version) REFERENCES strategies (strategy_id, version)
);
CREATE INDEX IF NOT EXISTS lifecycle_by_strategy
    ON strategy_lifecycle_events (strategy_id, at DESC);

-- =====================================================================
-- trials (§3.F) — statistical honesty; nothing is ever deleted
-- =====================================================================
CREATE TABLE IF NOT EXISTS trials (
    sig_hash    text        NOT NULL,
    config_hash text        NOT NULL,
    strategy_id text        NOT NULL,
    first_seen  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now(),
    replays     int         NOT NULL DEFAULT 1,
    PRIMARY KEY (sig_hash, config_hash)
);

-- =====================================================================
-- decision trail: arbiter, scrutiny, generation provenance (§3.G)
-- =====================================================================
CREATE TABLE IF NOT EXISTS arbiter_events (
    event_id  bigserial PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now(),
    regime    text,
    reason    text        NOT NULL,
    inputs    jsonb       NOT NULL,
    scores    jsonb,
    dropped   jsonb,
    intent    jsonb
);
CREATE INDEX IF NOT EXISTS arbiter_recent ON arbiter_events (at DESC);

CREATE TABLE IF NOT EXISTS scrutiny_events (
    intent_id            text PRIMARY KEY,
    strategy_id          text        NOT NULL,
    at                   timestamptz NOT NULL DEFAULT now(),
    direction            text        NOT NULL,
    verdict              text        NOT NULL CHECK (verdict IN ('APPROVE','VETO')),
    reason               text        NOT NULL,
    provider             text        NOT NULL,
    provider_version     text        NOT NULL,
    latency_ms           double precision,
    state_key_hash       text,
    conviction           double precision,   -- logged, NOT used for sizing (§3.F)
    evidence             jsonb,
    prompt_blob_key      text,               -- Parquet key; blob lives in archive
    realized_known       boolean     NOT NULL DEFAULT false,
    realized_adverse     boolean,
    realized_return_pct  double precision,
    realized_at          timestamptz
);
CREATE INDEX IF NOT EXISTS scrutiny_recent ON scrutiny_events (at DESC);
CREATE INDEX IF NOT EXISTS scrutiny_state_key ON scrutiny_events (state_key_hash);

CREATE TABLE IF NOT EXISTS generations (
    generation_id         uuid PRIMARY KEY,
    strategy_id           text        NOT NULL,
    at                    timestamptz NOT NULL DEFAULT now(),
    proposer              text        NOT NULL,
    model_id              text,
    prompt_version        text,
    system_prompt_hash    text,
    context_hash          text,
    profile_fingerprint   text,
    temperature           double precision,
    top_p                 double precision,
    max_output_tokens     int,
    seed                  bigint,
    parent_id             text,
    lineage_depth         int         NOT NULL DEFAULT 0,
    feature_registry_hash text,
    dsl_schema_hash       text,
    -- Full prompt and response live in Parquet; this is the pointer. A
    -- candidate whose provenance blob is missing must not enter validation.
    provenance_blob_key   text
);
CREATE INDEX IF NOT EXISTS generations_by_strategy ON generations (strategy_id, at DESC);

-- §3.E fourth table: the join from an LLM-authored strategy to the manifest of
-- the generation that produced it. Separate from `strategies` because
-- hand-written and seed strategies have no provenance row, and a nullable
-- column on `strategies` would make "no LLM involved" and "provenance lost"
-- the same value.
CREATE TABLE IF NOT EXISTS strategy_provenance (
    strategy_id   text   NOT NULL,
    version       int    NOT NULL,
    generation_id uuid   NOT NULL REFERENCES generations (generation_id),
    PRIMARY KEY (strategy_id, version),
    FOREIGN KEY (strategy_id, version) REFERENCES strategies (strategy_id, version)
);

-- =====================================================================
-- feature-set catalog (§3.D) — metadata only; vectors live in Parquet
-- =====================================================================
-- A feature-set version is immutable once written. Recomputing a version in
-- place would silently change what every past backtest measured, so a
-- definition change is a NEW version with its own Parquet directory and both
-- coexist until the operator retires the old one (§10).
CREATE TABLE IF NOT EXISTS feature_sets (
    feature_set_version int         PRIMARY KEY,
    registry_hash       text        NOT NULL,   -- hash of FEATURE_REGISTRY
    code_version        text        NOT NULL,
    definition          jsonb       NOT NULL,
    parquet_prefix      text        NOT NULL,   -- archive key prefix for vectors
    created_at          timestamptz NOT NULL DEFAULT now(),
    retired_at          timestamptz             -- operator action; never deletion
);

-- =====================================================================
-- money records (§3.I) — ACID-critical, joins are hard requirements
-- =====================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id       text PRIMARY KEY,          -- venue order id
    client_order_id text UNIQUE NOT NULL,     -- our orderLinkId; idempotency key
    trade_id       text        NOT NULL,
    strategy_id    text        NOT NULL,
    symbol         text        NOT NULL,
    side           text        NOT NULL CHECK (side IN ('Buy','Sell')),
    order_type     text        NOT NULL,
    qty            numeric     NOT NULL CHECK (qty > 0),
    price          numeric,
    reduce_only    boolean     NOT NULL DEFAULT false,
    status         text        NOT NULL,
    placed_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id     bigserial PRIMARY KEY,
    order_id    text        NOT NULL REFERENCES orders (order_id),
    trade_id    text        NOT NULL,
    leg         text        NOT NULL CHECK (leg IN ('entry','exit')),
    fill_time   timestamptz NOT NULL,
    price       numeric     NOT NULL CHECK (price > 0),
    qty         numeric     NOT NULL CHECK (qty > 0),
    fee         numeric     NOT NULL,
    funding     numeric     NOT NULL DEFAULT 0
);

-- The ledger is three tables, not one with nullable columns (§3.I). The split
-- is what makes the honesty check cheap: a prediction exists the instant a
-- decision is made, a realization only after the venue confirms, and drift is
-- derived from both. Collapsed into one row, "not yet reconciled" and "filled
-- at exactly the predicted price" are told apart only by convention. As
-- separate tables, an unreconciled trade is a missing row — countable by the
-- §11 parity check, and impossible to mistake for a perfect fill.

CREATE TABLE IF NOT EXISTS execution_ledger_predictions (
    trade_id                text        NOT NULL,
    leg                     text        NOT NULL CHECK (leg IN ('entry','exit')),
    strategy_id             text        NOT NULL,
    direction               text        NOT NULL,
    symbol                  text        NOT NULL,
    qty                     numeric     NOT NULL CHECK (qty > 0),
    decision_time           timestamptz NOT NULL,
    reference_price         numeric     NOT NULL CHECK (reference_price > 0),
    predicted_fill          numeric     NOT NULL CHECK (predicted_fill > 0),
    predicted_fee           numeric     NOT NULL,
    predicted_slippage_bps  numeric     NOT NULL,
    predicted_latency_ms    numeric     NOT NULL,
    predicted_funding       numeric     NOT NULL DEFAULT 0,
    client_order_id         text,
    PRIMARY KEY (trade_id, leg)
);
CREATE INDEX IF NOT EXISTS ledger_pred_by_strategy
    ON execution_ledger_predictions (strategy_id, decision_time DESC);

CREATE TABLE IF NOT EXISTS execution_ledger_realizations (
    trade_id        text        NOT NULL,
    leg             text        NOT NULL CHECK (leg IN ('entry','exit')),
    -- A realization without its prediction is unattributable: there is nothing
    -- to measure the fill against, which is the entire purpose of the ledger.
    FOREIGN KEY (trade_id, leg)
        REFERENCES execution_ledger_predictions (trade_id, leg),
    status          text        NOT NULL CHECK (status IN ('reconciled','unfilled')),
    realized_fill   numeric     CHECK (realized_fill IS NULL OR realized_fill > 0),
    realized_fee    numeric,
    realized_funding numeric,
    realized_latency_ms numeric,
    unfilled_reason text,
    reconciled_at   timestamptz NOT NULL DEFAULT now(),
    -- An unfilled leg has a reason and no fill; a reconciled leg has a fill.
    -- Neither state can be half-written.
    CONSTRAINT realization_shape CHECK (
        (status = 'reconciled' AND realized_fill IS NOT NULL)
     OR (status = 'unfilled'  AND realized_fill IS NULL
                              AND unfilled_reason IS NOT NULL)),
    PRIMARY KEY (trade_id, leg)
);

-- Columns mirror the `Drift` dataclass exactly. Every field is cost-positive:
-- a positive number always means worse than predicted, so a calibration report
-- can be read without remembering which way each sign points.
CREATE TABLE IF NOT EXISTS execution_ledger_drift (
    trade_id                text        NOT NULL,
    leg                     text        NOT NULL CHECK (leg IN ('entry','exit')),
    FOREIGN KEY (trade_id, leg)
        REFERENCES execution_ledger_predictions (trade_id, leg),
    strategy_id             text        NOT NULL,
    slippage_bps_predicted  numeric     NOT NULL,
    slippage_bps_realized   numeric     NOT NULL,
    slippage_bps_drift      numeric     NOT NULL,
    fee_drift               numeric     NOT NULL,
    latency_ms_drift        numeric     NOT NULL,
    qty_shortfall           numeric     NOT NULL,
    funding_drift           numeric     NOT NULL,
    cost_drift_usd          numeric     NOT NULL,
    partial                 boolean     NOT NULL,
    computed_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_id, leg)
);
CREATE INDEX IF NOT EXISTS ledger_drift_recent
    ON execution_ledger_drift (computed_at DESC);

-- Everything the cost model needs to be audited against reality, in one place.
-- `status` is DERIVED, not stored on the prediction. A leg with no realization
-- row is pending — not because someone remembered to write 'pending', but
-- because the fact has not arrived yet. That is the honest state after a crash
-- between placing an order and seeing its fill.
CREATE OR REPLACE VIEW execution_ledger AS
SELECT p.*,
       COALESCE(r.status, 'pending') AS status,
       r.realized_fill, r.realized_fee, r.realized_funding,
       r.realized_latency_ms, r.unfilled_reason, r.reconciled_at,
       to_jsonb(d.*) - 'trade_id' - 'leg' AS drift
FROM execution_ledger_predictions p
LEFT JOIN execution_ledger_realizations r USING (trade_id, leg)
LEFT JOIN execution_ledger_drift       d USING (trade_id, leg);

-- Persisted risk state. A restart must never reset equity, peak equity, or an
-- armed kill — "turn it off and on again" is exactly how a kill gets disarmed
-- by accident, so the stack is reloaded from here on boot.
CREATE TABLE IF NOT EXISTS risk_state (
    id         text        PRIMARY KEY,     -- 'portfolio' | 'kill_stack'
    state      jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Every pre-trade verdict, accepted or rejected, with the reason. §2.7.
CREATE TABLE IF NOT EXISTS risk_events (
    event_id    bigserial PRIMARY KEY,
    at          timestamptz NOT NULL DEFAULT now(),
    kind        text        NOT NULL,
    strategy_id text,
    detail      jsonb
);
CREATE INDEX IF NOT EXISTS risk_events_recent ON risk_events (at DESC);

CREATE TABLE IF NOT EXISTS kill_events (
    event_id bigserial PRIMARY KEY,
    level    text        NOT NULL CHECK (level IN ('L1','L2','L3')),
    reason   text        NOT NULL,
    scope    text,                            -- strategy_id for L1
    flatten  jsonb,
    at       timestamptz NOT NULL DEFAULT now()
);

-- =====================================================================
-- operational (§3, §11)
-- =====================================================================
CREATE TABLE IF NOT EXISTS quality_events (
    event_id      bigserial PRIMARY KEY,
    dataset_id    text        NOT NULL,
    failing_check text        NOT NULL,       -- NAME only, never a margin
    gate_version  int         NOT NULL,
    quarantine_key text,                      -- Parquet key in the quarantine bucket
    at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id bigserial PRIMARY KEY,
    kind     text        NOT NULL,            -- 'degraded' | 'snapshot' | ...
    label    text,
    reason   text,
    context  jsonb,
    at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS telemetry_recent ON telemetry_events (kind, at DESC);

-- Indexes every Parquet file the system produces. The weekly checksum audit
-- walks object storage and reconciles against this table; a mismatch is bit rot
-- or a lost object, and both are silent without it.
CREATE TABLE IF NOT EXISTS storage_manifest (
    bucket      text        NOT NULL,
    object_key  text        NOT NULL,
    dataset_id  text        NOT NULL,
    partition   text        NOT NULL,
    rows        bigint      NOT NULL,
    bytes       bigint      NOT NULL,
    sha256      text        NOT NULL,
    written_at  timestamptz NOT NULL DEFAULT now(),
    verified_at timestamptz,
    PRIMARY KEY (bucket, object_key)
);
CREATE INDEX IF NOT EXISTS manifest_by_dataset
    ON storage_manifest (dataset_id, written_at DESC);

CREATE TABLE IF NOT EXISTS tier_out_events (
    event_id      bigserial PRIMARY KEY,
    dataset_id    text        NOT NULL,
    partition_day date        NOT NULL,
    pg_rows       bigint,
    parquet_rows  bigint,
    checksum_ok   boolean,
    dropped       boolean     NOT NULL DEFAULT false,
    detail        text,
    at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    backtest_run_id uuid PRIMARY KEY,
    strategy_id     text        NOT NULL,
    config_hash     text        NOT NULL,
    -- §6: a backtest that does not pin as_of is a defect. NOT NULL makes it
    -- impossible to record one that did not.
    as_of           timestamptz NOT NULL,
    window_start    timestamptz NOT NULL,
    window_end      timestamptz NOT NULL,
    n_trials        int         NOT NULL,
    passed          boolean     NOT NULL,
    reasons         text[]      NOT NULL DEFAULT '{}',
    metrics         jsonb       NOT NULL,
    coverage        jsonb,
    curve_blob_key  text,                     -- Parquet: equity curve + trades
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS backtest_by_strategy
    ON backtest_runs (strategy_id, created_at DESC);

-- =====================================================================
-- integrity & backup observability (§8, §11)
-- =====================================================================
-- The storage layer is worthless if you cannot tell when it is lying. Each
-- hourly check writes its result here whether it passed or not: a check that
-- only records failures is indistinguishable from a check that stopped running.
CREATE TABLE IF NOT EXISTS integrity_events (
    event_id   bigserial PRIMARY KEY,
    check_name text        NOT NULL,
    dataset_id text,
    passed     boolean     NOT NULL,
    observed   jsonb,                       -- counts/keys that failed the check
    detail     text,
    at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS integrity_recent
    ON integrity_events (check_name, at DESC);
CREATE INDEX IF NOT EXISTS integrity_failures
    ON integrity_events (at DESC) WHERE NOT passed;

-- §8: "a backup that has never been restored is not a backup". Both the backup
-- and the restore drill are recorded, so dashboard "backup age" and "last
-- successful restore drill" come from evidence rather than from a cron
-- schedule that may have silently stopped firing.
CREATE TABLE IF NOT EXISTS backup_events (
    event_id  bigserial PRIMARY KEY,
    kind      text        NOT NULL
        CHECK (kind IN ('wal_archive','full','incremental','restore_drill',
                        'mirror_sync','checksum_audit')),
    succeeded boolean     NOT NULL,
    target    text,                          -- bucket/key or replica name
    bytes     bigint,
    detail    text,
    at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS backup_recent ON backup_events (kind, at DESC);

-- =====================================================================
-- schema versioning (§10) — schemas are versioned, never mutated
-- =====================================================================
-- Records which version this database was brought up to. Bootstrap is
-- idempotent and additive; there is deliberately no down-migration, because
-- §10's rule is that a breaking change becomes a NEW object alongside the old
-- rather than a rewrite of what is already there.
CREATE TABLE IF NOT EXISTS schema_version (
    version     int         PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    code_version text
);

-- ---------------------------------------------------------------------
-- Additive migrations
-- ---------------------------------------------------------------------
-- `CREATE TABLE IF NOT EXISTS` is a no-op against a database that already has
-- the table, so a column added to a definition above would never reach an
-- existing deployment — bootstrap would report success and the column would
-- simply not be there. §10 allows backward-compatible additions, so they are
-- restated here as idempotent ALTERs.
--
-- Only ever ADD, and only ever nullable-or-defaulted. A breaking change is a
-- new object alongside the old (§10, §14), never a rewrite of this one.
ALTER TABLE market_records
    ADD COLUMN IF NOT EXISTS annotations text[] NOT NULL DEFAULT '{}';

-- Validator warnings recorded at registration, and the most recent gate
-- verdict. `last_verdict` is insert-only from the registration path:
-- re-registering a strategy is not a new verdict, and overwriting it with null
-- would erase the gate result the dashboard reads and the generator learns
-- from.
ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS warnings     text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS last_verdict jsonb;
