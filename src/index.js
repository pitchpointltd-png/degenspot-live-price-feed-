/**
 * DegenSpot Live Price Feed — Wave A / LW-A-A2 (Railway)
 * Always-on Node service: holds a persistent Bybit v5 public linear tickers
 * WebSocket and upserts the SINGLE live_price row (BTCUSDT) in Supabase every ~3s.
 * Bybit tolerates cloud/datacenter IPs (Binance blocks Railway egress, so aggTrade
 * sockets opened but never delivered messages).
 * Feed only — no fills, no trade_journal.
 */
import { createClient } from '@supabase/supabase-js';
import http from 'node:http';
const SYMBOL = 'BTCUSDT';
const BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/linear';
const HEARTBEAT_MS = 20_000;
const WRITE_INTERVAL_MS = 3_000;
const STALE_MS = 15_000;
const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;
const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('[fatal] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}
// Price-monitor tick driver config (additive — see pingPriceMonitor below).
// Declared AFTER SUPABASE_URL so the URL default can reference it (const TDZ).
const PRICE_MONITOR_URL = process.env.PRICE_MONITOR_URL
  || `${SUPABASE_URL}/functions/v1/price-monitor`;
const PRICE_MONITOR_TICK_MS = Number(process.env.PRICE_MONITOR_TICK_MS) || 5000;
const PRICE_MONITOR_TIMEOUT_MS = Number(process.env.PRICE_MONITOR_TIMEOUT_MS) || 8000;
const PRICE_MONITOR_ENABLED =
  (process.env.PRICE_MONITOR_ENABLED ?? 'true').toLowerCase() !== 'false';
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});
let ws = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let reconnectDelay = RECONNECT_BASE_MS;
let lastMsgAt = 0;
let lastTicker = {};
let latestPrice = null;
let latestLast = null;
let latestMark = null;
let latestIndex = null;
let minuteBucket = null;
let high = null;
let low = null;
let shuttingDown = false;
let pmInFlight = false;
function currentMinuteISO(ts = Date.now()) {
  const d = new Date(ts);
  d.setUTCSeconds(0, 0);
  return d.toISOString();
}
function ingestTrade(price, ts) {
  if (!Number.isFinite(price) || price <= 0) return;
  const bucket = currentMinuteISO(ts);
  if (bucket !== minuteBucket) {
    minuteBucket = bucket;
    high = price;
    low = price;
  } else {
    if (high == null || price > high) high = price;
    if (low == null || price < low) low = price;
  }
  latestPrice = price;
}
function stopHeartbeat() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
}
function handleTicker(msg) {
  // Bybit sends a full "snapshot" first then partial "delta" messages that only
  // carry changed fields — merge onto the last known ticker, never replace it.
  Object.assign(lastTicker, msg.data);
  const last = parseFloat(lastTicker.lastPrice);
  const mark = parseFloat(lastTicker.markPrice);
  const index = parseFloat(lastTicker.indexPrice);
  if (Number.isFinite(last) && last > 0) {
    latestLast = last;
    ingestTrade(last, Number(msg.ts) || Date.now());
  }
  if (Number.isFinite(mark)) latestMark = mark;
  if (Number.isFinite(index)) latestIndex = index;
}
function connect() {
  if (shuttingDown) return;
  const socket = new WebSocket(BYBIT_WS_URL);
  ws = socket;
  socket.addEventListener('open', () => {
    if (ws !== socket) return;
    reconnectDelay = RECONNECT_BASE_MS;
    lastMsgAt = Date.now();
    console.log('[ws] open');
    try {
      socket.send(JSON.stringify({ op: 'subscribe', args: ['tickers.BTCUSDT'] }));
    } catch (err) {
      console.error('[ws] subscribe failed', err?.message ?? err);
    }
    stopHeartbeat();
    heartbeatTimer = setInterval(() => {
      if (ws === socket && socket.readyState === WebSocket.OPEN) {
        try { socket.send(JSON.stringify({ op: 'ping' })); } catch { /* ignore */ }
      }
    }, HEARTBEAT_MS);
  });
  socket.addEventListener('message', (event) => {
    if (ws !== socket) return;
    try {
      const msg = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data));
      if (msg?.op != null) return; // subscribe/pong ack — not price data
      if (msg?.topic !== 'tickers.BTCUSDT' || !msg.data) return;
      lastMsgAt = Date.now();
      handleTicker(msg);
    } catch (err) {
      console.error('[ws] parse error', err?.message ?? err);
    }
  });
  socket.addEventListener('close', () => {
    if (ws !== socket) return;
    stopHeartbeat();
    console.warn('[ws] closed');
    scheduleReconnect();
  });
  socket.addEventListener('error', (err) => {
    if (ws !== socket) return;
    console.error('[ws] error', err?.message ?? err);
    try { socket.close(); } catch { /* ignore */ }
  });
}
function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  const delay = reconnectDelay;
  reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  console.log(`[ws] reconnecting in ${delay}ms`);
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
}
function forceReconnect(reason) {
  if (shuttingDown) return;
  console.warn(`[ws] force reconnect: ${reason}`);
  const dead = ws;
  ws = null;
  stopHeartbeat();
  try { dead?.close(); } catch { /* ignore */ }
  scheduleReconnect();
}
function checkStale() {
  if (shuttingDown || !lastMsgAt) return;
  if (Date.now() - lastMsgAt > STALE_MS) {
    lastMsgAt = Date.now();
    forceReconnect(`no message for >${STALE_MS}ms`);
  }
}
async function writeLoop() {
  if (latestLast == null || latestLast <= 0) return;
  try {
    const { error } = await supabase.from('live_price').upsert({
      symbol: SYMBOL,
      price: latestLast,
      last_price: latestLast,
      mark_price: latestMark,
      index_price: latestIndex,
      high: high ?? latestLast,
      low: low ?? latestLast,
      minute_bucket: minuteBucket ?? currentMinuteISO(),
      source: 'railway-bybit',
      updated_at: new Date().toISOString(),
    }, { onConflict: 'symbol' });
    if (error) {
      console.error('[db] upsert error', error.message);
    } else if (Number.isFinite(latestMark)) {
      // Best-effort append to the ordered time-series. mark_price is NOT NULL,
      // so skip the history row on any cycle where mark isn't a finite number.
      // Failures here must never block or affect the authoritative live_price
      // upsert or the WebSocket loop — swallow and log a concise warning.
      try {
        const { error: histError } = await supabase.from('live_price_history').insert({
          symbol: SYMBOL,
          mark_price: latestMark,
          last_price: latestLast,
          index_price: latestIndex,
        });
        if (histError) console.warn('[db] history insert error', histError.message);
      } catch (histErr) {
        console.warn('[db] history insert threw', histErr?.message ?? histErr);
      }
    }
  } catch (err) {
    console.error('[db] upsert threw', err?.message ?? err);
  }
}
async function pingPriceMonitor() {
  // Fire-and-forget tick driver. Isolated from the price-write loop:
  // its own try/catch + AbortController timeout + in-flight guard so it can
  // never stall the 3s feed. Overlapping/duplicate ticks are safe on the
  // server side (unique ledger_key index + status='open' close guard).
  if (!PRICE_MONITOR_ENABLED) return;
  if (pmInFlight) return; // skip if the previous tick is still running
  pmInFlight = true;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), PRICE_MONITOR_TIMEOUT_MS);
  try {
    const res = await fetch(PRICE_MONITOR_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
      },
      body: '{}',
      signal: controller.signal,
    });
    if (!res.ok) {
      console.warn(`[pm] price-monitor ${res.status}`);
    }
  } catch (err) {
    // aborted timeout or network hiccup — log concise, never rethrow
    console.warn('[pm] ping failed', err?.name === 'AbortError' ? 'timeout' : (err?.message ?? err));
  } finally {
    clearTimeout(timer);
    pmInFlight = false;
  }
}
http.createServer((req, res) => {
  const ageMs = lastMsgAt ? Date.now() - lastMsgAt : null;
  const healthy = latestLast != null && ageMs != null && ageMs < STALE_MS;
  res.writeHead(healthy ? 200 : 503, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ healthy, symbol: SYMBOL, price: latestPrice, last: latestLast, mark: latestMark, index: latestIndex, high, low, minute_bucket: minuteBucket, last_msg_age_ms: ageMs }));
}).listen(PORT, () => console.log(`[http] health server on :${PORT}`));
function shutdown(sig) {
  console.log(`[sys] ${sig} — shutting down`);
  shuttingDown = true;
  try { ws?.close(); } catch { /* ignore */ }
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', (r) => console.error('[sys] unhandledRejection', r));
process.on('uncaughtException', (e) => console.error('[sys] uncaughtException', e?.message ?? e));
connect();
setInterval(writeLoop, WRITE_INTERVAL_MS);
setInterval(checkStale, 5_000);
if (PRICE_MONITOR_ENABLED) {
  setInterval(pingPriceMonitor, PRICE_MONITOR_TICK_MS);
  console.log(`[boot] price-monitor tick every ${PRICE_MONITOR_TICK_MS}ms`);
}
console.log('[boot] live-price-feed started');
