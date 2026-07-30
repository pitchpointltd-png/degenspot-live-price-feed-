# DegenSpot Live Price Feed (Railway)

Wave A Phase A1 — always-on Node service that maintains a **single** `live_price` row in Supabase from the Binance USDⓈ-M futures `/market` WebSocket.

No trade/fill logic. No `trade_journal` reads.

## Requirements

- **Node.js 22.x** (global `WebSocket`)
- Supabase project with a `live_price` table (one row per `symbol`, upserted in place)

### Expected `live_price` schema

| Column | Type | Notes |
|--------|------|--------|
| `symbol` | text | PK / unique — `BTCUSDT` |
| `price` | numeric | Latest aggTrade last price |
| `high` | numeric | Max price in current UTC minute bucket |
| `low` | numeric | Min price in current UTC minute bucket |
| `minute_bucket` | timestamptz | Start of current minute (UTC) |
| `source` | text | `railway` |
| `updated_at` | timestamptz | Last successful upsert |

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | yes | `https://<project-ref>.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Service role key (bypasses RLS) |

Do **not** commit keys. Set them in Railway → Service → Variables.

## Local run

```bash
npm install
export SUPABASE_URL="https://YOUR_PROJECT.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
npm start