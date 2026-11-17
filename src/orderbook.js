/**
 * XC2 — Bybit orderbook.1000 → public.live_orderbook (1 Hz upsert)
 * Standalone from src/index.js (live_price feed).
 */
import { createClient } from '@supabase/supabase-js';
import http from 'node:http';

const SYMBOL = 'BTCUSDT';
const SOURCE = 'railway-bybit-ob1000';
const BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/linear';
const ORDERBOOK_TOPIC = 'orderbook.1000.BTCUSDT';
const STEPS = ['0.1', '1', '10', '100'];

const HEARTBEAT_MS = 20_000;
const WRITE_INTERVAL_MS = 1_000;
const BOOK_FRESH_MS = 5_000;
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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

/** @type {Map<number, number>} */
const bids = new Map();
/** @type {Map<number, number>} */
const asks = new Map();

let ws = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let reconnectDelay = RECONNECT_BASE_MS;
let lastMsgAt = 0;
let lastBookUpdateAt = 0;
let hasSnapshot = false;
let shuttingDown = false;

let lastWriteOk = false;
let lastWriteAt = 0;
let minuteWritesOk = 0;
let minuteWritesFailed = 0;

function clearBook() {
  bids.clear();
  asks.clear();
  hasSnapshot = false;
  lastBookUpdateAt = 0;
}

function applyLevels(map, levels) {
  if (!Array.isArray(levels)) return;
  for (const row of levels) {
    if (!Array.isArray(row) || row.length < 2) continue;
    const price = parseFloat(row[0]);
    const size = parseFloat(row[1]);
    if (!Number.isFinite(price) || !Number.isFinite(size)) continue;
    if (size === 0) map.delete(price);
    else map.set(price, size);
  }
}

function handleOrderbookMessage(msg) {
  if (msg?.topic !== ORDERBOOK_TOPIC || !msg?.data) return;
  const { type, data } = msg;
  if (type === 'snapshot') {
    bids.clear();
    asks.clear();
    applyLevels(bids, data.b);
    applyLevels(asks, data.a);
    hasSnapshot = true;
    lastBookUpdateAt = Date.now();
    return;
  }
  if (type === 'delta') {
    if (!hasSnapshot) return;
    applyLevels(bids, data.b);
    applyLevels(asks, data.a);
    lastBookUpdateAt = Date.now();
  }
}

function decimalsForStep(step) {
  return step === '0.1' ? 1 : 0;
}

function roundToStep(price, step) {
  const d = decimalsForStep(step);
  return parseFloat(Number(price).toFixed(d));
}

function roundQty(qty) {
  return parseFloat(Number(qty).toFixed(3));
}

function bucketPrice(price, stepNum, side) {
  if (side === 'ask') return Math.ceil(price / stepNum) * stepNum;
  return Math.floor(price / stepNum) * stepNum;
}

function buildStepLadder(map, step, side) {
  const stepNum = Number(step);
  const buckets = new Map();
  for (const [price, size] of map) {
    if (!(size > 0)) continue;
    const b = bucketPrice(price, stepNum, side);
    buckets.set(b, (buckets.get(b) ?? 0) + size);
  }
  const asc = side === 'ask';
  const entries = [...buckets.entries()]
    .map(([p, q]) => [roundToStep(p, step), roundQty(q)])
    .sort((a, b) => (asc ? a[0] - b[0] : b[0] - a[0]));
  return entries.slice(0, 8);
}

function buildStepsPayload() {
  const out = {};
  for (const step of STEPS) {
    out[step] = {
      asks: buildStepLadder(asks, step, 'ask'),
      bids: buildStepLadder(bids, step, 'bid'),
    };
  }
  return out;
}

function bestBid() {
  if (bids.size === 0) return null;
  return Math.max(...bids.keys());
}

function bestAsk() {
  if (asks.size === 0) return null;
  return Math.min(...asks.keys());
}

function sumSizes(map) {
  let s = 0;
  for (const v of map.values()) s += v;
  return s;
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function connect() {
  if (shuttingDown) return;
  clearBook();
  const socket = new WebSocket(BYBIT_WS_URL);
  ws = socket;

  socket.addEventListener('open', () => {
    if (ws !== socket) return;
    reconnectDelay = RECONNECT_BASE_MS;
    lastMsgAt = Date.now();
    clearBook();
    console.log('[ob-ws] open');
    try {
      socket.send(JSON.stringify({ op: 'subscribe', args: [ORDERBOOK_TOPIC] }));
    } catch (err) {
      console.error('[ob-ws] subscribe failed', err?.message ?? err);
    }
    stopHeartbeat();
    heartbeatTimer = setInterval(() => {
      if (ws === socket && socket.readyState === WebSocket.OPEN) {
        try {
          socket.send(JSON.stringify({ op: 'ping' }));
        } catch {
          /* ignore */
        }
      }
    }, HEARTBEAT_MS);
  });

  socket.addEventListener('message', (event) => {
    if (ws !== socket) return;
    try {
      const msg = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data));
      if (msg?.op != null) return;
      lastMsgAt = Date.now();
      handleOrderbookMessage(msg);
    } catch (err) {
      console.error('[ob-ws] parse error', err?.message ?? err);
    }
  });

  socket.addEventListener('close', () => {
    if (ws !== socket) return;
    stopHeartbeat();
    console.warn('[ob-ws] closed');
    scheduleReconnect();
  });

  socket.addEventListener('error', (err) => {
    if (ws !== socket) return;
    console.error('[ob-ws] error', err?.message ?? err);
    try {
      socket.close();
    } catch {
      /* ignore */
    }
  });
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) return;
  const delay = reconnectDelay;
  reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  console.log(`[ob-ws] reconnecting in ${delay}ms`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, delay);
}

function forceReconnect(reason) {
  if (shuttingDown) return;
  console.warn(`[ob-ws] force reconnect: ${reason}`);
  const dead = ws;
  ws = null;
  stopHeartbeat();
  clearBook();
  try {
    dead?.close();
  } catch {
    /* ignore */
  }
  scheduleReconnect();
}

function checkStale() {
  if (shuttingDown || !lastMsgAt) return;
  if (Date.now() - lastMsgAt > STALE_MS) {
    lastMsgAt = Date.now();
    forceReconnect(`no message for >${STALE_MS}ms`);
  }
}

function canWrite() {
  if (!hasSnapshot || bids.size === 0 || asks.size === 0) return false;
  if (Date.now() - lastBookUpdateAt > BOOK_FRESH_MS) return false;
  return true;
}

async function writeLoop() {
  if (!canWrite()) return;
  const bb = bestBid();
  const ba = bestAsk();
  if (bb == null || ba == null) return;

  const payload = {
    symbol: SYMBOL,
    source: SOURCE,
    updated_at: new Date().toISOString(),
    best_bid: bb,
    best_ask: ba,
    bid_qty: roundQty(sumSizes(bids)),
    ask_qty: roundQty(sumSizes(asks)),
    levels: bids.size + asks.size,
    steps: buildStepsPayload(),
  };

  try {
    const { error } = await supabase.from('live_orderbook').upsert(payload, { onConflict: 'symbol' });
    if (error) {
      lastWriteOk = false;
      minuteWritesFailed += 1;
      console.error('[ob-db] upsert error', error.message);
    } else {
      lastWriteOk = true;
      lastWriteAt = Date.now();
      minuteWritesOk += 1;
    }
  } catch (err) {
    lastWriteOk = false;
    minuteWritesFailed += 1;
    console.error('[ob-db] upsert threw', err?.message ?? err);
  }
}

function minuteLog() {
  const bb = bestBid();
  const ba = bestAsk();
  const levels = bids.size + asks.size;
  console.log(
    `[ob] levels=${levels} bid=${bb ?? '—'} ask=${ba ?? '—'} writes ok=${minuteWritesOk} failed=${minuteWritesFailed}`,
  );
  minuteWritesOk = 0;
  minuteWritesFailed = 0;
}

http
  .createServer((req, res) => {
    const ageMs = lastMsgAt ? Date.now() - lastMsgAt : null;
    const bookFresh = lastBookUpdateAt > 0 && Date.now() - lastBookUpdateAt < BOOK_FRESH_MS;
    const healthy = hasSnapshot && bookFresh && ageMs != null && ageMs < STALE_MS;
    res.writeHead(healthy ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        healthy,
        service: 'orderbook',
        symbol: SYMBOL,
        levels: bids.size + asks.size,
        best_bid: bestBid(),
        best_ask: bestAsk(),
        last_msg_age_ms: ageMs,
        last_write_ok: lastWriteOk,
        last_write_age_ms: lastWriteAt ? Date.now() - lastWriteAt : null,
      }),
    );
  })
  .listen(PORT, () => console.log(`[ob-http] health on :${PORT}`));

function shutdown(sig) {
  console.log(`[ob-sys] ${sig} — shutting down`);
  shuttingDown = true;
  try {
    ws?.close();
  } catch {
    /* ignore */
  }
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', (r) => console.error('[ob-sys] unhandledRejection', r));
process.on('uncaughtException', (e) => console.error('[ob-sys] uncaughtException', e?.message ?? e));

connect();
setInterval(writeLoop, WRITE_INTERVAL_MS);
setInterval(checkStale, 5_000);
setInterval(minuteLog, 60_000);

console.log('[ob-boot] orderbook service started');