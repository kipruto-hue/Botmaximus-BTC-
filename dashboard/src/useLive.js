import { useEffect, useRef, useState } from "react";

/**
 * Live link to the BOTMAXIMUS data layer.
 * Primary: websocket /ws/live (snapshot every ~1.5s).
 * Fallback: poll /api/telemetry every 2.5s when the socket is down.
 * Returns { connected, snap } — snap is the server telemetry snapshot or null.
 */
export default function useLive() {
  const [connected, setConnected] = useState(false);
  const [snap, setSnap] = useState(null);
  const wsRef = useRef(null);

  useEffect(() => {
    let alive = true;
    let pollTimer = null;
    let retryTimer = null;

    const poll = async () => {
      try {
        const r = await fetch("/api/telemetry");
        if (r.ok && alive) setSnap(await r.json());
      } catch { /* server down — snap stays stale */ }
    };

    const connect = () => {
      if (!alive) return;
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(`${proto}//${location.host}/ws/live`);
      wsRef.current = ws;
      ws.onopen = () => {
        setConnected(true);
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
      };
      ws.onmessage = (e) => {
        try { setSnap(JSON.parse(e.data)); } catch { /* ignore bad frame */ }
      };
      ws.onclose = () => {
        setConnected(false);
        if (!alive) return;
        if (!pollTimer) pollTimer = setInterval(poll, 2500);
        retryTimer = setTimeout(connect, 3000);
      };
      ws.onerror = () => ws.close();
    };

    connect();
    return () => {
      alive = false;
      clearTimeout(retryTimer);
      if (pollTimer) clearInterval(pollTimer);
      wsRef.current?.close();
    };
  }, []);

  return { connected, snap };
}
