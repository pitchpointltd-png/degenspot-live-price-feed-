# degenspot-live-price-feed

Railway services for DegenSpot market data.

## Live price feed (`npm start`)

Runs `src/index.js`: Bybit tickers WebSocket → `live_price` / `live_price_history`, optional `price-monitor` ping. Do not change this when adding other workers.

**Env:** `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, optional `PORT`, `PRICE_MONITOR_*`.

## Order book service (`npm run start:orderbook`)

Runs `src/orderbook.js` (XC2): separate process that subscribes to Bybit **orderbook.1000.BTCUSDT** on the linear public WebSocket, maintains a local book (snapshot + delta), and upserts **`public.live_orderbook`** once per second when the book is fresh (updated within the last 5s).

**Writes (service role):** `symbol` `BTCUSDT`, `source` `railway-bybit-ob1000`, best bid/ask, depth totals, and `steps` ladders for `0.1`, `1`, `10`, `100` BTC buckets (top 8 levels per side per step).

**Env (required):**

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

**Optional:** `PORT` — health check HTTP (default 8080).

**Deploy:** Use as a second Railway service from this repo with start command `npm run start:orderbook` (europe-west4). Requires XC1 migration applied (`supabase db push` on degenspot).

**Ops:** Logs one summary line per minute (levels, best bid/ask, write ok/fail counts). DB failures are logged only; the WebSocket loop keeps running.