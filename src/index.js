/**
 * DegenSpot Live Price Feed — Wave A / LW-A-A1 (Railway)
 * Always-on Node service: holds a persistent Binance USDⓈ-M futures aggTrade
 * WebSocket and upserts the SINGLE live_price row (BTCUSDT) in Supabase every ~3s.
 * Feed only — no fills, no trade_journal.
 */
import { createClient } from '@supabase/supabase-js';
import http from 'node:http';
const SYMBOL = 'BTCUSDT';
const BINANCE_WS_URL = 'wss://fstream.binance.com/stream?streams=btcusdt@aggTrade';
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
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});
let ws = null;
let reconnectTimer = null;
let reconnectDelay = RECONNECT_BASE_MS;
let lastMsgAt = 0;
let latestPrice = null;
let minuteBucket = null;
let high = null;
let low = null;
let shuttingDown = false;
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
function connect() {
  if (shuttingDown) return;
  const socket = new WebSocket(BINANCE_WS_URL);
  ws = socket;
  socket.addEventListener('open', () => {
    if (ws !== socket) return;
    reconnectDelay = RECONNECT_BASE_MS;
    lastMsgAt = Date.now();
    console.log('[ws] open');
  });
  socket.addEventListener('message', (event) => {
    if (ws !== socket) return;
    lastMsgAt = Date.now();
    try {
      const msg = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data));
      const data = msg?.data;
      if (!data || data.p == null) return;
      const price = parseFloat(data.p);
      const tradeTs = Number(data.T ?? data.E) || Date.now();
      ingestTrade(price, tradeTs);
    } catch (err) {
      console.error('[ws] parse error', err?.message ?? err);
    }
  });
  socket.addEventListener('close', () => {
    if (ws !== socket) return;
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
  if (latestPrice == null || latestPrice <= 0) return;
  try {
    const { error } = await supabase.from('live_price').upsert({
      symbol: SYMBOL,
      price: latestPrice,
      high: high ?? latestPrice,
      low: low ?? latestPrice,
      minute_bucket: minuteBucket ?? currentMinuteISO(),
      source: 'railway',
      updated_at: new Date().toISOString(),
    }, { onConflict: 'symbol' });
    if (error) console.error('[db] upsert error', error.message);
  } catch (err) {
    console.error('[db] upsert threw', err?.message ?? err);
  }
}
http.createServer((req, res) => {
  const ageMs = lastMsgAt ? Date.now() - lastMsgAt : null;
  const healthy = latestPrice != null && ageMs != null && ageMs < STALE_MS;
  res.writeHead(healthy ? 200 : 503, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ healthy, symbol: SYMBOL, price: latestPrice, high, low, minute_bucket: minuteBucket, last_msg_age_ms: ageMs }));
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
console.log('[boot] live-price-feed started');
