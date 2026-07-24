import { useState, useEffect, useRef } from "react";
import { Power, Activity, AlertTriangle, Radio, ShieldAlert, Cpu, Gauge as GaugeIcon, Waypoints } from "lucide-react";
import useLive from "./useLive.js";

/* ============================ design tokens ============================ */
const C = {
  bg: "#0D1017", bgDeep: "#080A0F", panel: "#141A22", panelHi: "#1A222D",
  line: "#232C38", lineSoft: "#1C242F",
  txt: "#E7EDF3", mut: "#8492A0", dim: "#586573",
  green: "#3FB950", red: "#FF5C57", amber: "#E3B341", blue: "#5AA9FF", cyan: "#39D6DE",
};
const F = {
  disp: "'Space Grotesk', system-ui, -apple-system, sans-serif",
  mono: "'JetBrains Mono', ui-monospace, 'SF Mono', monospace",
};

/* ============================ helpers ============================ */
const fmt = (n, d = 0) => n.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
const rnd = (a, b) => a + Math.random() * (b - a);
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

const LIFECYCLE = {
  full: { c: C.green, label: "FULL" },
  micro: { c: C.cyan, label: "MICRO" },
  paper: { c: C.blue, label: "PAPER" },
  candidate: { c: C.mut, label: "CAND" },
  retired: { c: C.dim, label: "RETIRED" },
};
const POSTURE = {
  "RISK-ON": C.green, NEUTRAL: C.mut, "RISK-OFF": C.amber, BLACKOUT: C.red,
};

/* ============================ small components ============================ */
function Panel({ title, icon, right, sim, children, style }) {
  return (
    <div style={{
      background: C.panel, border: `1px solid ${C.line}`, borderRadius: 10,
      display: "flex", flexDirection: "column", minHeight: 0, ...style,
    }}>
      <div style={{
        display: "flex", alignItems: "center", gap: 8, padding: "9px 12px",
        borderBottom: `1px solid ${C.lineSoft}`,
      }}>
        {icon}
        <span style={{ fontFamily: F.disp, fontSize: 11, letterSpacing: 1.4, color: C.mut, textTransform: "uppercase", fontWeight: 600 }}>{title}</span>
        {sim && (
          <span style={{ fontFamily: F.mono, fontSize: 8.5, color: C.amber, border: `1px solid ${C.amber}44`, borderRadius: 3, padding: "1px 5px", letterSpacing: 0.8 }}>SIM</span>
        )}
        <div style={{ marginLeft: "auto" }}>{right}</div>
      </div>
      <div style={{ padding: 12, flex: 1, minHeight: 0, overflow: "auto" }}>{children}</div>
    </div>
  );
}

function Dot({ c, pulse }) {
  return <span style={{
    width: 8, height: 8, borderRadius: "50%", background: c, display: "inline-block",
    boxShadow: `0 0 8px ${c}`, animation: pulse ? "bmx-pulse 1.4s ease-in-out infinite" : "none",
  }} />;
}

function Bar({ v, max, c, h = 6 }) {
  return (
    <div style={{ background: C.bgDeep, borderRadius: 3, height: h, overflow: "hidden" }}>
      <div style={{ width: `${Math.min(100, (v / max) * 100)}%`, height: "100%", background: c, transition: "width .5s ease" }} />
    </div>
  );
}

/* signature: drawdown-to-kill ring gauge */
function RiskGauge({ dd, kill }) {
  const r = 78, cx = 100, cy = 100, circ = 2 * Math.PI * r;
  const ratio = Math.min(1, dd / kill);
  const c = ratio < 0.5 ? C.green : ratio < 0.8 ? C.amber : C.red;
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%" }}>
      <svg viewBox="0 0 200 200" width="188" height="188">
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={C.bgDeep} strokeWidth="14" />
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={c} strokeWidth="14" strokeLinecap="round"
          strokeDasharray={circ} strokeDashoffset={circ * (1 - ratio)}
          transform={`rotate(-90 ${cx} ${cy})`} style={{ transition: "stroke-dashoffset .6s ease, stroke .6s" }} />
        {/* kill tick at top */}
        <line x1={cx} y1={cy - r - 9} x2={cx} y2={cy - r + 9} stroke={C.red} strokeWidth="2.5" />
        <text x={cx} y={cy - 8} textAnchor="middle" style={{ fontFamily: F.mono, fill: c, fontSize: 34, fontWeight: 700 }}>{dd.toFixed(1)}</text>
        <text x={cx} y={cy + 14} textAnchor="middle" style={{ fontFamily: F.disp, fill: C.mut, fontSize: 11, letterSpacing: 1.5 }}>% DRAWDOWN</text>
        <text x={cx} y={cy + 42} textAnchor="middle" style={{ fontFamily: F.mono, fill: C.dim, fontSize: 11 }}>KILL @ {kill.toFixed(0)}%</text>
      </svg>
      <div style={{ fontFamily: F.mono, fontSize: 11, color: C.dim, marginTop: 2 }}>
        headroom <span style={{ color: c }}>{(kill - dd).toFixed(1)}%</span> to hard kill
      </div>
    </div>
  );
}

/* ============================ main ============================ */
export default function Dashboard() {
  const { connected, snap, risk } = useLive();

  const [running, setRunning] = useState(true);
  const [confirmKill, setConfirmKill] = useState(false);
  const [hb, setHb] = useState(true);
  const [uptime, setUptime] = useState(0);
  const [btc, setBtc] = useState(64210);

  const [positions, setPositions] = useState([
    { id: "P-1", side: "LONG", size: 0.018, entry: 63980, stop: 63620, pnl: 41 },
    { id: "P-2", side: "SHORT", size: 0.011, entry: 64460, stop: 64720, pnl: 27 },
  ]);

  const [strats, setStrats] = useState([
    { id: "MR-BAND-01", state: "full", alloc: 34, decay: 0.08 },
    { id: "MOM-BREAK-03", state: "full", alloc: 28, decay: 0.15 },
    { id: "FUND-SKEW-02", state: "micro", alloc: 12, decay: 0.05 },
    { id: "LIQ-SWEEP-05", state: "micro", alloc: 9, decay: 0.41 },
    { id: "SESS-OPEN-01", state: "paper", alloc: 0, decay: 0.02 },
    { id: "OI-DIVG-07", state: "candidate", alloc: 0, decay: 0.0 },
    { id: "VWAP-REV-02", state: "retired", alloc: 0, decay: 0.93 },
  ]);

  const [events, setEvents] = useState([
    { label: "FOMC statement", eta: 372, action: "BLACKOUT" },
    { label: "US CPI (m/m)", eta: 5220, action: "RISK-OFF" },
    { label: "Options expiry (Deribit)", eta: 18400, action: "RISK-OFF" },
  ]);
  const [posture, setPosture] = useState("NEUTRAL");

  const [scrutiny, setScrutiny] = useState([
    { id: 1, t: "18:22:04", v: "TRADE", tf: "LONG", conv: 0.71, thesis: "4H bias up · 15m breakout held · funding neutral · bid-heavy book", contra: [] },
    { id: 2, t: "18:21:11", v: "SKIP", tf: "—", conv: 0.34, thesis: "chop regime · conviction below threshold", contra: ["low ADX"] },
    { id: 3, t: "18:20:38", v: "SKIP", tf: "—", conv: 0.62, thesis: "long setup but hawkish FOMC inside window", contra: ["FOMC T-6m"] },
  ]);
  const idRef = useRef(4);

  const TRADE_TH = [
    ["LONG", "4H bias up · 15m breakout held · funding neutral · no event in window"],
    ["SHORT", "momentum decay · liquidation cluster above · DXY firming"],
    ["LONG", "OI building · spot-ETF net inflow · book bid-heavy"],
    ["SHORT", "rejection at liquidity pocket · negative funding flip"],
  ];
  const SKIP_TH = [
    ["chop regime · conviction below threshold", ["low ADX"]],
    ["spread wider than edge margin", ["thin book"]],
    ["BTC/ETH correlation break contradicts bias", ["corr break"]],
    ["macro release inside blackout window", ["CPI T-3m"]],
    ["stale funding feed · standing aside", ["stale feed"]],
  ];

  useEffect(() => {
    const iv = setInterval(() => {
      setHb((h) => !h);
      setUptime((u) => u + 2);
      if (!running) return;

      let move = rnd(-55, 55);
      setBtc((b) => Math.max(1000, b + move));
      setPositions((ps) => ps.map((p) => ({ ...p, pnl: Math.round(p.pnl + (p.side === "LONG" ? move : -move) * p.size * 1.4) })));

      setStrats((ss) => ss.map((s) => ({ ...s, decay: Math.max(0, Math.min(1, s.decay + rnd(-0.02, 0.025))) })));

      setEvents((evs) => {
        const next = evs.map((e) => ({ ...e, eta: e.eta - 2 })).filter((e) => e.eta > 0);
        return next.length ? next : [{ label: "US NFP", eta: 8200, action: "RISK-OFF" }, ...next];
      });

      const near = events[0] && events[0].eta < 420;
      setPosture(near ? "BLACKOUT" : pick(["NEUTRAL", "NEUTRAL", "RISK-ON", "RISK-OFF"]));

      if (Math.random() < 0.55) {
        const trade = !near && Math.random() < 0.4;
        const now = new Date();
        const ts = now.toTimeString().slice(0, 8);
        let entry;
        if (trade) {
          const [tf, th] = pick(TRADE_TH);
          entry = { id: idRef.current++, t: ts, v: "TRADE", tf, conv: +rnd(0.62, 0.86).toFixed(2), thesis: th, contra: [] };
        } else {
          const [th, contra] = pick(SKIP_TH);
          entry = { id: idRef.current++, t: ts, v: "SKIP", tf: "—", conv: +rnd(0.28, 0.6).toFixed(2), thesis: near ? "event blackout · standing aside" : th, contra: near ? ["blackout"] : contra };
        }
        setScrutiny((sc) => [entry, ...sc].slice(0, 9));
      }
    }, 1600);
    return () => clearInterval(iv);
  }, [running, events]);

  const doKill = () => {
    setRunning(false); setConfirmKill(false);
    setPositions([]);
    setScrutiny((sc) => [{ id: idRef.current++, t: new Date().toTimeString().slice(0, 8), v: "KILL", tf: "—", conv: 1, thesis: "MASTER KILL engaged · all positions flattened · system halted", contra: [] }, ...sc].slice(0, 9));
  };
  const restart = () => { setRunning(true); };

  /* ---- live values from the data layer (fall back to sim when disconnected) ---- */
  const liveBtc = snap?.price ?? btc;
  const liveUptime = snap?.uptime_s ?? uptime;
  const wsUp = connected && snap?.ws_connected;
  const feedRows = snap?.feeds?.length ? snap.feeds : null;
  const freshLabel = (f) => f == null ? "—" : f > 120000 ? `${(f / 60000).toFixed(0)}m` : f > 9999 ? `${(f / 1000).toFixed(0)}s` : `${Math.round(f)}ms`;

  /* ---- REAL risk core (/api/risk). null when engine is unreachable → SIM. ---- */
  const riskLive = !!risk;
  const dd = risk?.drawdown_pct ?? 0;
  const kill = risk?.limits?.max_drawdown_kill_pct ?? 15;
  const equity = risk?.equity ?? 10000;
  const dayPnlPct = risk?.day_pnl_pct ?? 0;
  const openRiskUsd = risk?.open_risk_usd ?? 0;
  const openRiskPct = equity ? (openRiskUsd / equity) * 100 : 0;
  const riskPerTrade = risk?.limits?.risk_per_trade_pct ?? 0.25;
  const kstack = risk?.kills;
  const killState = kstack?.l3_killed ? "L3 KILLED" : kstack?.l2_halted ? "L2 HALTED"
    : (kstack && Object.keys(kstack.l1_suspended || {}).length) ? "L1 active" : "all clear";

  const uh = Math.floor(liveUptime / 3600), um = Math.floor((liveUptime % 3600) / 60), us = liveUptime % 60;
  const eta = (s) => `${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;

  return (
    <div style={{ background: C.bg, color: C.txt, fontFamily: F.disp, padding: 14, minHeight: 640 }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');
        @keyframes bmx-pulse{0%,100%{opacity:1}50%{opacity:.25}}
        @keyframes bmx-in{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:none}}
        @keyframes bmx-scan{0%{opacity:.4}50%{opacity:1}100%{opacity:.4}}
        *::-webkit-scrollbar{width:7px;height:7px}
        *::-webkit-scrollbar-thumb{background:${C.line};border-radius:4px}
        *::-webkit-scrollbar-track{background:transparent}
        @media (prefers-reduced-motion: reduce){*{animation:none!important;transition:none!important}}
      `}</style>

      {/* ===== status bar ===== */}
      <div style={{
        display: "flex", alignItems: "center", gap: 16, background: C.panel,
        border: `1px solid ${C.line}`, borderRadius: 10, padding: "11px 16px", marginBottom: 12,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
          <Dot c={running ? C.green : C.red} pulse={running} />
          <span style={{ fontFamily: F.disp, fontWeight: 700, letterSpacing: 0.5, fontSize: 15 }}>BOTMAXIMUS</span>
          <span style={{ fontFamily: F.mono, color: C.cyan, fontSize: 13 }}>(BTC)</span>
        </div>
        <span style={{
          fontFamily: F.mono, fontSize: 10, color: connected ? C.green : C.amber,
          border: `1px solid ${(connected ? C.green : C.amber)}55`,
          borderRadius: 4, padding: "2px 7px", letterSpacing: 1,
        }}>{connected ? "LIVE DATA" : "OFFLINE · SIM"}</span>
        <span style={{ fontFamily: F.mono, fontSize: 12, color: running ? C.green : C.red, letterSpacing: 1 }}>
          {running ? "● RUNNING" : "■ HALTED"}
        </span>

        <div style={{ display: "flex", alignItems: "center", gap: 18, marginLeft: 8, fontFamily: F.mono, fontSize: 11, color: C.mut }}>
          <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Activity size={13} color={hb && running ? C.green : C.dim} /> watchdog
          </span>
          <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <Radio size={13} color={wsUp ? C.green : C.red} /> binance ws
          </span>
          <span>uptime {String(uh).padStart(2, "0")}:{String(um).padStart(2, "0")}:{String(us).padStart(2, "0")}</span>
          <span>BTC/USD <span style={{ color: C.txt }}>${fmt(liveBtc)}</span></span>
        </div>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          {running ? (
            !confirmKill ? (
              <button onClick={() => setConfirmKill(true)} style={killBtn(false)}>
                <Power size={15} /> MASTER KILL
              </button>
            ) : (
              <>
                <span style={{ fontFamily: F.mono, fontSize: 11, color: C.red }}>flatten all &amp; halt?</span>
                <button onClick={doKill} style={killBtn(true)}>CONFIRM</button>
                <button onClick={() => setConfirmKill(false)} style={ghostBtn}>cancel</button>
              </>
            )
          ) : (
            <button onClick={restart} style={{ ...killBtn(false), background: "transparent", borderColor: C.green, color: C.green }}>
              deliberate restart
            </button>
          )}
        </div>
      </div>

      {!running && (
        <div style={{
          background: `${C.red}18`, border: `1px solid ${C.red}55`, borderRadius: 8,
          padding: "10px 14px", marginBottom: 12, display: "flex", alignItems: "center", gap: 10,
          fontFamily: F.mono, fontSize: 12.5, color: C.red,
        }}>
          <ShieldAlert size={16} /> SYSTEM HALTED — all positions flattened · L3 kill engaged · restart requires deliberate action
        </div>
      )}

      {/* ===== grid ===== */}
      <div style={{ display: "grid", gridTemplateColumns: "232px 1fr 320px", gap: 12, alignItems: "stretch" }}>
        {/* left column */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <Panel title="Risk to kill" icon={<GaugeIcon size={14} color={C.mut} />} sim={!riskLive} style={{ height: 300 }}>
            <RiskGauge dd={dd} kill={kill} />
          </Panel>
          <Panel title="Account" icon={<Cpu size={14} color={C.mut} />} sim={!riskLive}
            right={<span style={{ fontFamily: F.mono, fontSize: 9, color: C.dim }}>paper</span>}
            style={{ flex: 1 }}>
            <Stat label="Equity" val={`$${fmt(equity, 2)}`} c={C.txt} />
            <Stat label="Day PnL" val={`${dayPnlPct >= 0 ? "+" : ""}${dayPnlPct.toFixed(2)}%`} c={dayPnlPct >= 0 ? C.green : C.red} />
            <Stat label="Open risk" val={`${openRiskPct.toFixed(2)}% eq`} c={C.mut} />
            <Stat label="Risk / trade" val={`${riskPerTrade}%`} c={C.mut} />
            <Stat label="Kill stack" val={killState} c={killState === "all clear" ? C.green : C.red} />
            <div style={{ marginTop: 10, fontFamily: F.mono, fontSize: 10, color: C.dim, lineHeight: 1.5 }}>
              L1 strat · L2 daily-loss · L3 max-DD<br />all armed · LLM cannot override
            </div>
          </Panel>
        </div>

        {/* center column */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12, minWidth: 0 }}>
          <Panel title="Pipeline telemetry" icon={<Waypoints size={14} color={C.mut} />}
            right={<span style={{ fontFamily: F.mono, fontSize: 10, color: C.dim }}>gather · parse · quality · store  (p50 ms)</span>}
            style={{ height: 300 }}>
            {!feedRows ? (
              <div style={{ fontFamily: F.mono, fontSize: 12, color: C.dim, textAlign: "center", padding: "40px 0" }}>
                — data layer offline · start the server to see live telemetry —
              </div>
            ) : (
              <>
                <div style={{ display: "grid", gridTemplateColumns: "112px 1fr 82px", gap: "0 12px", alignItems: "center" }}>
                  {feedRows.map((f) => {
                    const stale = f.stale ?? (f.budget != null && f.fresh != null && f.fresh > f.budget);
                    const warn = f.budget != null && f.fresh != null && f.fresh > f.budget * 0.6;
                    const fc = f.fresh == null ? C.dim : stale ? C.red : warn ? C.amber : C.green;
                    const tot = Math.max(1, f.g + f.p + f.q + f.s);
                    return (
                      <div key={f.name} style={{ display: "contents" }}>
                        <div style={{ fontFamily: F.mono, fontSize: 11.5, color: C.txt, padding: "5px 0" }}>{f.name}</div>
                        <div style={{ display: "flex", gap: 3, alignItems: "center" }}>
                          {[["g", C.blue], ["p", C.cyan], ["q", C.amber], ["s", C.green]].map(([k, col]) => (
                            <div key={k} title={`${k}: ${f[k]}ms`} style={{ height: 9, width: `${(f[k] / tot) * 100}%`, background: col, borderRadius: 2, minWidth: 3 }} />
                          ))}
                          <span style={{ fontFamily: F.mono, fontSize: 10, color: C.dim, marginLeft: 4 }}>{(f.g + f.p + f.q + f.s).toFixed(1)}ms</span>
                        </div>
                        <div style={{ fontFamily: F.mono, fontSize: 10.5, color: fc, textAlign: "right", display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 5 }}>
                          <Dot c={fc} pulse={stale} />
                          {freshLabel(f.fresh)}
                        </div>
                      </div>
                    );
                  })}
                </div>
                <div style={{ marginTop: 12, paddingTop: 10, borderTop: `1px solid ${C.lineSoft}`, display: "flex", gap: 18, fontFamily: F.mono, fontSize: 10.5, color: C.dim }}>
                  <span>end-to-end p95 <span style={{ color: C.green }}>{snap?.e2e_p95_ms ?? 0}ms</span></span>
                  <span>stored <span style={{ color: C.txt }}>{fmt(snap?.counts?.stored ?? 0)}</span></span>
                  <span>quarantined <span style={{ color: (snap?.counts?.quarantined ?? 0) > 0 ? C.amber : C.dim }}>{snap?.counts?.quarantined ?? 0}</span></span>
                  <span>stale feeds <span style={{ color: (snap?.stale_feeds ?? 0) > 0 ? C.red : C.green }}>{snap?.stale_feeds ?? 0}</span></span>
                  <span>ws reconnects <span style={{ color: C.dim }}>{snap?.ws_reconnects ?? 0}</span></span>
                </div>
              </>
            )}
          </Panel>

          <Panel title="Open positions" icon={<Activity size={14} color={C.mut} />} sim style={{ flex: 1, minHeight: 150 }}>
            {positions.length === 0 ? (
              <div style={{ fontFamily: F.mono, fontSize: 12, color: C.dim, textAlign: "center", padding: "24px 0" }}>
                — flat · no open exposure —
              </div>
            ) : (
              <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: F.mono, fontSize: 12 }}>
                <thead>
                  <tr style={{ color: C.dim, fontSize: 10, textAlign: "left" }}>
                    <th style={th}>SIDE</th><th style={th}>SIZE</th><th style={th}>ENTRY</th><th style={th}>STOP (broker)</th><th style={{ ...th, textAlign: "right" }}>PnL</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.map((p) => (
                    <tr key={p.id} style={{ borderTop: `1px solid ${C.lineSoft}` }}>
                      <td style={{ ...td, color: p.side === "LONG" ? C.green : C.red }}>{p.side}</td>
                      <td style={td}>{p.size.toFixed(3)}</td>
                      <td style={td}>${fmt(p.entry)}</td>
                      <td style={{ ...td, color: C.amber }}>${fmt(p.stop)}</td>
                      <td style={{ ...td, textAlign: "right", color: p.pnl >= 0 ? C.green : C.red }}>{p.pnl >= 0 ? "+" : ""}{p.pnl}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Panel>

          <Panel title="Strategy pool" icon={<Cpu size={14} color={C.mut} />} sim
            right={<span style={{ fontFamily: F.mono, fontSize: 10, color: C.dim }}>{strats.filter(s => s.alloc > 0).length} live · {strats.length} tracked</span>}
            style={{ minHeight: 150 }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
              {strats.map((s) => {
                const L = LIFECYCLE[s.state];
                const dying = s.decay > 0.6;
                return (
                  <div key={s.id} style={{ display: "grid", gridTemplateColumns: "128px 62px 1fr 96px", gap: 10, alignItems: "center" }}>
                    <span style={{ fontFamily: F.mono, fontSize: 11.5, color: C.txt }}>{s.id}</span>
                    <span style={{ fontFamily: F.mono, fontSize: 9.5, color: L.c, border: `1px solid ${L.c}44`, borderRadius: 3, padding: "1px 5px", textAlign: "center", letterSpacing: 0.5 }}>{L.label}</span>
                    <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                      <div style={{ flex: 1 }}><Bar v={s.alloc} max={40} c={C.cyan} h={5} /></div>
                      <span style={{ fontFamily: F.mono, fontSize: 10, color: C.dim, width: 28 }}>{s.alloc}%</span>
                    </div>
                    <span style={{ fontFamily: F.mono, fontSize: 10, color: dying ? C.red : s.decay > 0.35 ? C.amber : C.dim, textAlign: "right" }}>
                      decay {(s.decay * 100).toFixed(0)}%{dying ? " ↯repair" : ""}
                    </span>
                  </div>
                );
              })}
            </div>
          </Panel>
        </div>

        {/* right column */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <Panel title="World state" icon={<AlertTriangle size={14} color={C.mut} />} sim style={{ minHeight: 168 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
              <span style={{ fontFamily: F.mono, fontSize: 10, color: C.dim }}>POSTURE</span>
              <span style={{
                fontFamily: F.disp, fontWeight: 700, fontSize: 12, color: POSTURE[posture],
                border: `1px solid ${POSTURE[posture]}55`, borderRadius: 5, padding: "3px 10px", letterSpacing: 1,
              }}>{posture}</span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {events.map((e, i) => {
                const near = e.eta < 420;
                return (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, fontFamily: F.mono, fontSize: 11 }}>
                    <Dot c={near ? C.red : C.amber} pulse={near} />
                    <span style={{ color: C.txt, flex: 1 }}>{e.label}</span>
                    <span style={{ color: near ? C.red : C.mut }}>T-{eta(e.eta)}</span>
                    <span style={{ color: POSTURE[e.action] || C.mut, fontSize: 9.5, border: `1px solid ${(POSTURE[e.action] || C.mut)}44`, borderRadius: 3, padding: "1px 4px" }}>{e.action}</span>
                  </div>
                );
              })}
            </div>
          </Panel>

          <Panel title="Scrutiny feed" icon={<Cpu size={14} color={C.mut} />} sim
            right={<span style={{ fontFamily: F.mono, fontSize: 9.5, color: C.dim, animation: "bmx-scan 2s infinite" }}>reasoning · live</span>}
            style={{ flex: 1, minHeight: 300 }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 9 }}>
              {scrutiny.map((s) => {
                const col = s.v === "TRADE" ? C.green : s.v === "KILL" ? C.red : C.mut;
                return (
                  <div key={s.id} style={{
                    borderLeft: `2px solid ${col}`, paddingLeft: 9, animation: "bmx-in .3s ease",
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 3 }}>
                      <span style={{ fontFamily: F.mono, fontSize: 10.5, color: col, fontWeight: 600 }}>{s.v}{s.tf !== "—" ? ` · ${s.tf}` : ""}</span>
                      <span style={{ fontFamily: F.mono, fontSize: 9.5, color: C.dim, marginLeft: "auto" }}>{s.t}</span>
                    </div>
                    <div style={{ fontFamily: F.disp, fontSize: 11.5, color: C.txt, lineHeight: 1.4, marginBottom: 5 }}>{s.thesis}</div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <div style={{ flex: 1, maxWidth: 90 }}>
                        <Bar v={s.conv} max={1} c={s.conv >= 0.6 ? C.green : C.amber} h={4} />
                      </div>
                      <span style={{ fontFamily: F.mono, fontSize: 10, color: C.mut }}>conv {s.conv.toFixed(2)}</span>
                      {s.contra.map((x, i) => (
                        <span key={i} style={{ fontFamily: F.mono, fontSize: 9, color: C.amber, border: `1px solid ${C.amber}44`, borderRadius: 3, padding: "0 4px" }}>{x}</span>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          </Panel>
        </div>
      </div>

      <div style={{ marginTop: 10, fontFamily: F.mono, fontSize: 10, color: C.dim, textAlign: "center" }}>
        {connected
          ? "data layer connected · telemetry panel is live · SIM panels await their subsystems"
          : "data layer offline · run the server (python -m botmaximus.main) for live telemetry"}
      </div>
    </div>
  );
}

/* ============================ style bits ============================ */
const th = { padding: "0 0 6px", fontWeight: 500, letterSpacing: 0.5 };
const td = { padding: "6px 0" };
function Stat({ label, val, c }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", padding: "5px 0", borderBottom: `1px solid ${C.lineSoft}` }}>
      <span style={{ fontFamily: F.mono, fontSize: 11, color: C.mut }}>{label}</span>
      <span style={{ fontFamily: F.mono, fontSize: 14, color: c, fontWeight: 500 }}>{val}</span>
    </div>
  );
}
function killBtn(confirm) {
  return {
    display: "flex", alignItems: "center", gap: 6, cursor: "pointer",
    fontFamily: F.disp, fontWeight: 700, fontSize: 12, letterSpacing: 0.8,
    color: "#fff", background: confirm ? C.red : "#B3312C",
    border: `1px solid ${C.red}`, borderRadius: 7, padding: "8px 14px",
  };
}
const ghostBtn = {
  cursor: "pointer", fontFamily: F.mono, fontSize: 11, color: C.mut,
  background: "transparent", border: `1px solid ${C.line}`, borderRadius: 6, padding: "7px 10px",
};
