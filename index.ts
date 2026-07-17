/**
 * Wave A Phase B — realtime engine scaffold.
 * Persistent outbound Binance WebSocket inside a SQLite-backed Durable Object.
 * Ticks → console only. No Supabase / trade logic yet.
 */
import { DurableObject } from "cloudflare:workers";

const BINANCE_COMBINED_STREAM_URL =
  "https://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@markPrice";

const ALARM_INTERVAL_MS = 30_000;

export interface Env {
  PRICE_ENGINE: DurableObjectNamespace<PriceEngine>;
}

type BinanceCombinedMessage = {
  stream?: string;
  data?: Record<string, unknown>;
};

export class PriceEngine extends DurableObject<Env> {
  private ws: WebSocket | null = null;
  private lastMarkPrice: number | null = null;

  async fetch(_request: Request): Promise<Response> {
    if (!this.isSocketOpen()) {
      await this.connect();
    }
    await this.ensureAlarm();
    return new Response("ok", { status: 200 });
  }

  async alarm(): Promise<void> {
    if (!this.isSocketOpen()) {
      await this.connect();
    }
    await this.ensureAlarm();
  }

  private isSocketOpen(): boolean {
    return this.ws != null && this.ws.readyState === WebSocket.OPEN;
  }

  private async ensureAlarm(): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
  }

  private async connect(): Promise<void> {
    if (this.isSocketOpen()) return;

    if (this.ws != null) {
      try {
        this.ws.close();
      } catch {
        /* ignore */
      }
      this.ws = null;
    }

    const res = await fetch(BINANCE_COMBINED_STREAM_URL, {
      headers: { Upgrade: "websocket" },
    });

    const ws = res.webSocket;
    if (!ws) {
      console.error("[PriceEngine] Binance did not accept WebSocket upgrade", res.status);
      return;
    }

    ws.accept();
    this.ws = ws;

    ws.addEventListener("message", (event: MessageEvent) => {
      this.onSocketMessage(event);
    });

    ws.addEventListener("close", () => {
      console.log("[PriceEngine] outbound WS closed — alarm will reconnect");
      this.ws = null;
    });

    ws.addEventListener("error", (event: Event) => {
      console.error("[PriceEngine] outbound WS error", event);
      try {
        ws.close();
      } catch {
        /* ignore */
      }
      this.ws = null;
    });

    console.log("[PriceEngine] connected to Binance combined stream");
  }

  private onSocketMessage(event: MessageEvent): void {
    try {
      const raw = typeof event.data === "string" ? event.data : String(event.data);
      const payload = JSON.parse(raw) as BinanceCombinedMessage;
      this.logCompactTick(payload);
    } catch (err) {
      console.error("[PriceEngine] failed to parse WS message", err);
    }
  }

  private logCompactTick(payload: BinanceCombinedMessage): void {
    const stream = payload.stream ?? "unknown";
    const data = payload.data;
    if (!data) return;

    let price: number | null = null;
    let markPrice = this.lastMarkPrice;
    let ts: number | null = null;

    if (stream.includes("markPrice")) {
      const parsed = parseFloat(String(data.p ?? ""));
      if (Number.isFinite(parsed)) {
        this.lastMarkPrice = parsed;
        markPrice = parsed;
      }
      const eventTs = Number(data.E ?? data.T);
      ts = Number.isFinite(eventTs) ? eventTs : Date.now();
    } else if (stream.includes("aggTrade")) {
      const parsed = parseFloat(String(data.p ?? ""));
      price = Number.isFinite(parsed) ? parsed : null;
      const tradeTs = Number(data.T ?? data.E);
      ts = Number.isFinite(tradeTs) ? tradeTs : Date.now();
    } else {
      return;
    }

    console.log(
      `[tick] stream=${stream} price=${price ?? "—"} mark=${markPrice ?? "—"} ts=${ts ?? "—"}`,
    );
  }
}

async function pingPriceEngine(env: Env): Promise<void> {
  const stub = env.PRICE_ENGINE.getByName("btc");
  await stub.fetch(new Request("https://price-engine/ping", { method: "POST" }));
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/start") {
      await pingPriceEngine(env);
      return new Response("ok", { status: 200 });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(_event: ScheduledEvent, env: Env): Promise<void> {
    await pingPriceEngine(env);
  },
};
