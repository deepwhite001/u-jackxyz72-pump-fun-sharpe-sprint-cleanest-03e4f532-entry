// Pump.fun Sharpe Sprint — cleanest paper bot.
// Strategy: early-entry trend rider on pump.fun bonding curves.
// The bot speaks agent-fun/replay/v2 on stdin/stdout: the harness sends a
// hello line, then one batch line per slot window, then a done line. The bot
// answers every batch with exactly one reply line echoing the batch seq and
// carrying its orders for that slot. Orders decided on a batch fill two slots
// later (latency_slots), so every decision is made on reserves already seen.
//
// Scoring is hourly Sharpe ratio, not raw PnL, so the design optimises for
// steady small wins: tiny equal stakes spread over many distinct mints,
// quick take-profit, tight stop-loss, a trailing stop once a trade is up,
// and a hard hold limit. At least 20 distinct mints must each complete a
// round trip of >= 0.01 SOL, max drawdown must stay <= 50%, net PnL > 0.
//
// The entry signal is a dip with buyers still stepping in: a curve that
// ran up, then fell 20-40% off its recent high while the last trades are
// mostly buys, tends to bounce part of the way back. One entry per coin,
// one full-size buy, exit on +12% take-profit, -10% stop, or a 400-slot
// hold cap — the cell a latency-aware offline grid over the dev dataset
// picked as least-bad (see the constants block).
//
// Determinism: no clock, no environment reads for decisions, no randomness.
// Every decision is a pure function of the hello + batch stream, iterated in
// insertion order, so the two graded executions produce identical logs.

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Resolve paths from this script's own location, never from the working
// directory: the grader may run the bot from any directory with the repo
// mounted read-only, so nothing is ever written into the repository.
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = process.env.MAKEFUN_OUTPUT_DIR ?? '/output';

const LAMPORTS_PER_SOL = 1_000_000_000n;
const SOL = 1e9; // lamports per SOL, as a float for ratio math
const CURVE_FEE_BPS = 125n; // protocol 95 + creator 30 bps on curve trades
const BPS = 10_000n;

// ---- Tunable strategy constants (v4: dip-buying, latency-aware tuning) ----
// v1 momentum (-0.33 SOL) and v2 dip (-0.12) both lost; v3 birth entries
// (-0.36) looked great in a naive offline sim but the edge was an artifact
// of pricing entries at the crossing trade — with the harness's 2-slot fill
// latency the same sim goes to -0.07/trade, and wide-stop / pure-hold
// variants are worse (-0.11..-0.44). A latency-aware grid over dip entries
// (/tmp/req27-sharpe-analysis6.py) found the least-bad cell: dip 20-40%
// below the trailing 2500-slot high, buyers still stepping in, TP +12%,
// stop -10%, hold cap 400 slots (mean -0.017/trade, win rate 41% — still
// slightly negative on this bearish dev window, but the best of every
// family tested, with tiny drawdown). One entry per coin, one full-size
// buy, no pyramid: the sim that picked these numbers had no adds.
const HIGH_WINDOW_SLOTS = 2500; // lookback for the recent high (~17 min)
const DIP_MIN = 0.2; // enter when price is >= 20% below the recent high
const DIP_MAX = 0.4; // ... but not more than 40% (a collapse, not a dip)
const FLOW_TRADES = 6; // of the last 6 trades at least MIN_FLOW_BUYS are buys
const MIN_FLOW_BUYS = 3;
const LAST_MUST_BE_BUY = true; // the dip must currently be bought, not sold
const MIN_REAL_SOL = 3; // curve real SOL reserve at entry, lower bound
const MAX_ENTRIES_PER_COIN = 1; // one entry per coin
const COOLDOWN_SLOTS = 0; // no re-entry
const STAKE_SOL = 0.05; // total SOL per position, bought in one order
const FIRST_BUY_SOL = 0.05; // first (only) buy of a position
const ADD_SOL = 0.03; // unused in v4 (first buy already equals the stake)
const MIN_BUY_SOL = 0.012; // smallest buy worth sending (>= 0.01 round trip)
const BUY_SLIPPAGE_BPS = 500;
const SELL_SLIPPAGE_BPS = 3000;
const TAKE_PROFIT = 0.12; // exit when liquidation value >= cost * 1.12
const STOP_LOSS = 0.1; // exit when liquidation value <= cost * 0.90
const TRAIL_ARM = 10; // trailing stop off in v4 (the grid had none)
const TRAIL_DROP = 0.1;
const MAX_HOLD_SLOTS = 400; // hard time exit
const FADE_WINDOW_SLOTS = 20;
const FADE_SOL = 1e9; // fade exit off in v4

// ---- Wire types (agent-fun/replay/v2, minimal subset this bot needs) ----
type Pubkey = string;

interface HelloMsg {
  type: 'hello';
  protocol: string;
  seed: string;
  capital: { lamports: string };
  caps: {
    max_open_positions: number;
    max_order_bps_of_real_reserve: number;
    max_orders_per_batch: number;
    shorts: boolean;
    leverage: boolean;
  };
  latency_slots: number;
  fees: {
    protocol_bps_fallback: number;
    creator_bps_fallback: number;
    base_fee_lamports: string;
    priority_fee_lamports: string;
    failed_tx_bps: number;
  };
}

interface TradeEvent {
  event: 'TradeEvent';
  mint: Pubkey;
  sol_amount: string;
  token_amount: string;
  is_buy: boolean;
  user: Pubkey;
  virtual_sol_reserves: string;
  virtual_token_reserves: string;
  real_sol_reserves: string;
  real_token_reserves: string;
  virtual_quote_reserves: string;
  real_quote_reserves: string;
  slot: number;
  block_time: number | null;
}

interface CreateEvent {
  event: 'CreateEvent';
  mint: Pubkey;
  creator: Pubkey;
  virtual_token_reserves: string;
  virtual_sol_reserves: string;
  real_token_reserves: string;
  virtual_quote_reserves: string;
  slot: number;
  block_time: number | null;
}

interface CompleteEvent {
  event: 'CompleteEvent';
  mint: Pubkey;
  slot: number;
  block_time: number | null;
}

type ChainEvent = TradeEvent | CreateEvent | CompleteEvent;

interface Mark {
  mint: Pubkey;
  slot: number;
  virtual_token_reserves: string;
  virtual_quote_reserves: string;
  real_token_reserves: string;
  real_quote_reserves: string;
}

interface Fill {
  client_id: string;
  mint: Pubkey;
  side: 'buy' | 'sell';
  status: 'filled' | 'rejected' | 'failed';
  reason: string | null;
  decided_slot: number;
  slot: number;
  quote_lamports: string;
  token_amount: string;
  fee_lamports: string;
  tx_fee_lamports: string;
}

interface BatchMsg {
  type: 'batch';
  seq: number;
  slot: number;
  events: ChainEvent[];
  marks: Mark[];
  fills: Fill[];
  account: {
    cash_lamports: string;
    positions: { mint: Pubkey; token_amount: string; cost_lamports: string }[];
  };
}

interface DoneMsg {
  type: 'done';
  seq: number;
  reason: string;
  account: BatchMsg['account'];
}

type Order =
  | { side: 'buy'; client_id: string; mint: Pubkey; quote_lamports: string; max_slippage_bps: number }
  | { side: 'sell'; client_id: string; mint: Pubkey; token_amount: string; max_slippage_bps: number };

// ---- Per-mint bookkeeping ----
interface Tick {
  slot: number;
  buy: boolean;
  sol: number; // SOL amount of the trade
  user: Pubkey;
  price: number; // virtual quote / virtual token at the trade
}

interface Tape {
  createdSlot: number | null;
  creator: Pubkey | null;
  trades: Tick[];
  entries: number;
  lastExitSlot: number;
  devSold: boolean;
  crossed: boolean; // real SOL already crossed RQ_ENTRY_SOL (entry chance used)
}

interface Curve {
  mint: Pubkey;
  createdSlot: number | null;
  vq: bigint;
  vt: bigint;
  rq: bigint;
  rt: bigint;
  complete: boolean;
}

interface HeldInfo {
  openedSlot: number;
  peakValueSol: number; // best liquidation value seen, SOL
  investedSol: number; // SOL sent in buys for this position
  lastBuyPrice: number;
  exiting: boolean;
}

function priceOf(vq: bigint, vt: bigint): number {
  if (vt === 0n) return 0;
  return Number(vq) / Number(vt);
}

// What selling `tokens` against (vq, vt) would pay, curve fees deducted.
function sellValueSol(tokens: bigint, vq: bigint, vt: bigint): number {
  if (tokens <= 0n || vt === 0n) return 0;
  const q = (tokens * vq) / (vt + tokens);
  const net = q - (q * CURVE_FEE_BPS + BPS - 1n) / BPS;
  return Number(net) / SOL;
}

// ---- Bot state: everything the strategy knows, folded in from the stream ----
class Bot {
  hello: HelloMsg | null = null;
  cashLamports = 0n;
  positions = new Map<Pubkey, { tokens: bigint; costLamports: bigint }>();
  curves = new Map<Pubkey, Curve>();
  tapes = new Map<Pubkey, Tape>();
  held = new Map<Pubkey, HeldInfo>();
  pending = new Map<string, { mint: Pubkey; side: 'buy' | 'sell'; amountLamports?: bigint }>();
  nextClient = 1;
  batches = 0;
  filledBuys = 0;
  filledSells = 0;
  coinsTraded = new Set<Pubkey>();
  roundTrips = new Set<Pubkey>();

  txFeeLamports(): bigint {
    const h = this.hello;
    if (h === null) return 105_000n;
    return BigInt(h.fees.base_fee_lamports) + BigInt(h.fees.priority_fee_lamports);
  }

  txFeeSol(): number {
    return Number(this.txFeeLamports()) / SOL;
  }

  clientId(): string {
    return `o${this.nextClient++}`;
  }

  curveOf(mint: Pubkey): Curve {
    let c = this.curves.get(mint);
    if (c === undefined) {
      c = { mint, createdSlot: null, vq: 0n, vt: 0n, rq: 0n, rt: 0n, complete: false };
      this.curves.set(mint, c);
    }
    return c;
  }

  tapeOf(mint: Pubkey): Tape {
    let t = this.tapes.get(mint);
    if (t === undefined) {
      t = { createdSlot: null, creator: null, trades: [], entries: 0, lastExitSlot: -1e12, devSold: false, crossed: false };
      this.tapes.set(mint, t);
    }
    return t;
  }

  // Size cap for buys on one mint in one reply: 1% of its real SOL reserve.
  maxBuyLamports(mint: Pubkey): bigint {
    const h = this.hello;
    const c = this.curves.get(mint);
    if (h === null || c === undefined || c.complete) return 0n;
    return (c.rq * BigInt(h.caps.max_order_bps_of_real_reserve)) / BPS;
  }

  committedCount(): number {
    const mints = new Set<Pubkey>(this.positions.keys());
    for (const p of this.pending.values()) if (p.side === 'buy') mints.add(p.mint);
    return mints.size;
  }

  // Cash minus what pending orders will take if they fill: every tx fee plus
  // each pending buy's amount, so parallel batches cannot over-commit cash.
  freeCashLamports(): bigint {
    let free = this.cashLamports;
    const tx = this.txFeeLamports();
    for (const p of this.pending.values()) {
      free -= tx;
      if (p.side === 'buy' && p.amountLamports !== undefined) free -= p.amountLamports;
    }
    return free;
  }

  applyHello(hello: HelloMsg): void {
    this.hello = hello;
    this.cashLamports = BigInt(hello.capital.lamports);
  }

  applyBatch(batch: BatchMsg): void {
    this.batches++;
    // Fills first: pending orders resolve, round trips are counted here.
    for (const f of batch.fills) {
      this.pending.delete(f.client_id);
      if (f.client_id.startsWith('_')) {
        // Harness close (_complete_ / _flatten_ / _writeoff_): position gone.
        this.held.delete(f.mint);
        continue;
      }
      if (f.status === 'filled') {
        if (f.side === 'buy') {
          this.filledBuys++;
          this.coinsTraded.add(f.mint);
        } else {
          this.filledSells++;
          if (this.coinsTraded.has(f.mint)) this.roundTrips.add(f.mint);
        }
      }
    }
    // Authoritative account snapshot.
    this.cashLamports = BigInt(batch.account.cash_lamports);
    this.positions.clear();
    for (const p of batch.account.positions) {
      const tokens = BigInt(p.token_amount);
      if (tokens > 0n) {
        this.positions.set(p.mint, { tokens, costLamports: BigInt(p.cost_lamports) });
      }
    }
    // Marks: latest reserves for mints we hold or have pending.
    for (const m of batch.marks) {
      const c = this.curveOf(m.mint);
      c.vq = BigInt(m.virtual_quote_reserves);
      c.vt = BigInt(m.virtual_token_reserves);
      c.rq = BigInt(m.real_quote_reserves);
      c.rt = BigInt(m.real_token_reserves);
    }
    // Events in chain order.
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const c = this.curveOf(e.mint);
        c.createdSlot = e.slot;
        c.vq = BigInt(e.virtual_quote_reserves);
        c.vt = BigInt(e.virtual_token_reserves);
        c.rt = BigInt(e.real_token_reserves);
        c.rq = 0n;
        const t = this.tapeOf(e.mint);
        t.createdSlot = e.slot;
        t.creator = e.creator;
      } else if (e.event === 'TradeEvent') {
        const c = this.curveOf(e.mint);
        c.vq = BigInt(e.virtual_quote_reserves);
        c.vt = BigInt(e.virtual_token_reserves);
        c.rq = BigInt(e.real_quote_reserves);
        c.rt = BigInt(e.real_token_reserves);
        const t = this.tapeOf(e.mint);
        const price = priceOf(BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves));
        t.trades.push({
          slot: e.slot,
          buy: e.is_buy,
          sol: Number(BigInt(e.sol_amount)) / SOL,
          user: e.user,
          price,
        });
        if (t.creator !== null && e.user === t.creator && !e.is_buy) t.devSold = true;
      } else {
        const c = this.curveOf(e.mint);
        c.complete = true;
      }
    }
    // Prune old tape ticks (keep a little more than the longest window used:
    // the recent-high lookback is the long one).
    const keepFrom = batch.slot - Math.max(HIGH_WINDOW_SLOTS, MAX_HOLD_SLOTS) - 10;
    for (const t of this.tapes.values()) {
      let cut = 0;
      while (cut < t.trades.length && t.trades[cut]!.slot <= keepFrom) cut++;
      if (cut > 0) t.trades.splice(0, cut);
    }
    // Drop held info for positions that are gone with nothing pending.
    for (const mint of [...this.held.keys()]) {
      if (this.positions.has(mint)) continue;
      let pendingHere = false;
      for (const p of this.pending.values()) if (p.mint === mint) pendingHere = true;
      if (!pendingHere) {
        const info = this.held.get(mint);
        this.held.delete(mint);
        if (info !== undefined) this.tapeOf(mint).lastExitSlot = batch.slot;
      }
    }
  }

  // Net SOL flow and tape stats over the last `window` slots.
  windowStats(t: Tape, now: number, window: number): {
    netSol: number;
    buys: number;
    buyers: Set<Pubkey>;
    startPrice: number;
    lastPrice: number;
    high: number;
  } {
    const from = now - window;
    let netSol = 0;
    let buys = 0;
    const buyers = new Set<Pubkey>();
    let startPrice = -1;
    let lastPrice = 0;
    let high = 0;
    for (const tr of t.trades) {
      if (tr.slot <= from) {
        startPrice = tr.price;
        continue;
      }
      if (startPrice < 0) startPrice = tr.price;
      netSol += tr.buy ? tr.sol : -tr.sol;
      if (tr.buy) {
        buys++;
        buyers.add(tr.user);
      }
      if (tr.price > high) high = tr.price;
      lastPrice = tr.price;
    }
    return { netSol, buys, buyers, startPrice, lastPrice, high };
  }

  // Dip entry: price well below its recent high, with buyers still active.
  entryOk(mint: Pubkey, now: number): boolean {
    const h = this.hello;
    if (h === null) return false;
    const c = this.curves.get(mint);
    if (c === undefined || c.complete) return false;
    const t = this.tapes.get(mint);
    if (t === undefined) return false;
    if (t.entries >= MAX_ENTRIES_PER_COIN) return false;
    if (now - t.lastExitSlot < COOLDOWN_SLOTS) return false;
    if (t.devSold) return false; // creator already sold: rug smell, skip
    const rqSol = Number(c.rq) / SOL;
    if (rqSol < MIN_REAL_SOL) return false;
    if (t.trades.length < FLOW_TRADES) return false;
    // Recent high within the high window.
    let high = 0;
    for (let i = t.trades.length - 1; i >= 0; i--) {
      const tr = t.trades[i]!;
      if (now - tr.slot > HIGH_WINDOW_SLOTS) break;
      if (tr.price > high) high = tr.price;
    }
    if (high <= 0) return false;
    const last = t.trades[t.trades.length - 1]!;
    const dip = 1 - last.price / high;
    if (dip < DIP_MIN || dip > DIP_MAX) return false;
    // Buyers still stepping in: of the last few trades enough are buys and
    // the most recent trade is a buy (the dip is being bought, not sold).
    let buys = 0;
    let netSol = 0;
    const start = Math.max(0, t.trades.length - FLOW_TRADES);
    for (let i = start; i < t.trades.length; i++) {
      const tr = t.trades[i]!;
      if (tr.buy) {
        buys++;
        netSol += tr.sol;
      } else {
        netSol -= tr.sol;
      }
    }
    if (buys < MIN_FLOW_BUYS || netSol < 0) return false;
    if (LAST_MUST_BE_BUY && !last.buy) return false;
    return true;
  }

  decide(batch: BatchMsg): Order[] {
    const h = this.hello;
    if (h === null) return [];
    const now = batch.slot;
    const cap = h.caps.max_orders_per_batch;
    const txSol = this.txFeeSol();
    const orders: Order[] = [];
    const marks = new Map<Pubkey, Mark>();
    for (const m of batch.marks) marks.set(m.mint, m);
    const pendingMints = new Set<Pubkey>();
    for (const p of this.pending.values()) pendingMints.add(p.mint);

    const valueOf = (mint: Pubkey): number => {
      const pos = this.positions.get(mint);
      if (pos === undefined) return 0;
      const mk = marks.get(mint);
      if (mk !== undefined) {
        return sellValueSol(pos.tokens, BigInt(mk.virtual_quote_reserves), BigInt(mk.virtual_token_reserves)) - txSol;
      }
      const c = this.curves.get(mint);
      if (c === undefined) return 0;
      return sellValueSol(pos.tokens, c.vq, c.vt) - txSol;
    };

    // 1. Exits first, in deterministic mint order.
    const heldMints = [...this.positions.keys()].sort();
    for (const mint of heldMints) {
      if (orders.length >= cap) break;
      const pos = this.positions.get(mint)!;
      const c = this.curves.get(mint);
      if (c === undefined || c.complete) continue;
      let info = this.held.get(mint);
      if (info === undefined) {
        info = { openedSlot: now, peakValueSol: 0, investedSol: Number(pos.costLamports) / SOL, lastBuyPrice: 0, exiting: false };
        this.held.set(mint, info);
      }
      const value = valueOf(mint);
      const cost = Number(pos.costLamports) / SOL;
      if (value > info.peakValueSol) info.peakValueSol = value;
      if (info.exiting) continue;
      let exit = false;
      if (value <= cost * (1 - STOP_LOSS)) exit = true; // stop-loss
      else if (value >= cost * (1 + TAKE_PROFIT)) exit = true; // take-profit
      else if (info.peakValueSol >= cost * (1 + TRAIL_ARM) && value < info.peakValueSol * (1 - TRAIL_DROP)) exit = true; // trailing stop
      else if (now - info.openedSlot >= MAX_HOLD_SLOTS) exit = true; // stale
      else {
        const t = this.tapes.get(mint);
        if (t !== undefined) {
          const fade = this.windowStats(t, now, FADE_WINDOW_SLOTS);
          if (fade.netSol < -FADE_SOL) exit = true; // flow turned against us
        }
      }
      if (!exit) continue;
      if (pendingMints.has(mint)) continue; // a sell is already in flight
      info.exiting = true;
      orders.push({ side: 'sell', client_id: this.clientId(), mint, token_amount: pos.tokens.toString(), max_slippage_bps: SELL_SLIPPAGE_BPS });
      pendingMints.add(mint);
    }

    // 2. Pyramid adds into strength, then fresh entries.
    let freeSol = Number(this.freeCashLamports()) / SOL;
    let slotsLeft = h.caps.max_open_positions - this.committedCount();
    const tradedMints: Pubkey[] = [];
    for (const e of batch.events) {
      if (e.event !== 'TradeEvent') continue;
      if (tradedMints[tradedMints.length - 1] !== e.mint && !tradedMints.includes(e.mint)) tradedMints.push(e.mint);
    }
    for (const mint of tradedMints) {
      if (orders.length >= cap) break;
      if (pendingMints.has(mint)) continue;
      const c = this.curves.get(mint);
      if (c === undefined || c.complete) continue;
      const info = this.held.get(mint);
      const pos = this.positions.get(mint);
      if (info !== undefined && pos !== undefined && !info.exiting) {
        // v4 takes no pyramid adds: the first buy already equals the stake.
        continue;
      }
      // Fresh entry: dip signal only (see entryOk).
      if (slotsLeft <= 0) continue;
      if (this.positions.has(mint)) continue;
      if (!this.entryOk(mint, now)) continue;
      const capLamports = (this.maxBuyLamports(mint) * 97n) / 100n;
      let amountLamports = BigInt(Math.floor(FIRST_BUY_SOL * SOL));
      if (amountLamports > capLamports) amountLamports = capLamports;
      if (amountLamports < BigInt(Math.floor(MIN_BUY_SOL * SOL))) continue;
      if (freeSol < Number(amountLamports) / SOL + txSol) continue;
      const price = priceOf(c.vq, c.vt);
      orders.push({ side: 'buy', client_id: this.clientId(), mint, quote_lamports: amountLamports.toString(), max_slippage_bps: BUY_SLIPPAGE_BPS });
      const t = this.tapeOf(mint);
      t.entries++;
      this.held.set(mint, { openedSlot: now, peakValueSol: 0, investedSol: Number(amountLamports) / SOL, lastBuyPrice: price, exiting: false });
      freeSol -= Number(amountLamports) / SOL + txSol;
      slotsLeft--;
      pendingMints.add(mint);
    }
    return orders;
  }
}

// ---- Wire loop: one JSON line in, one reply line out per batch ----
async function main(): Promise<void> {
  const bot = new Bot();
  let buffer = '';
  let doneMsg: DoneMsg | null = null;

  for await (const chunk of process.stdin) {
    buffer += chunk.toString('utf8');
    let nl = buffer.indexOf('\n');
    while (nl !== -1) {
      const line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      nl = buffer.indexOf('\n');
      if (line.trim() === '') continue;
      let msg: { type?: unknown };
      try {
        msg = JSON.parse(line) as { type?: unknown };
      } catch {
        console.error('bad JSON line from harness, ignoring');
        continue;
      }
      if (msg.type === 'hello') {
        bot.applyHello(msg as unknown as HelloMsg);
      } else if (msg.type === 'batch') {
        const batch = msg as unknown as BatchMsg;
        bot.applyBatch(batch);
        const orders = bot.decide(batch);
        for (const o of orders) {
          bot.pending.set(o.client_id, {
            mint: o.mint,
            side: o.side,
            amountLamports: o.side === 'buy' ? BigInt(o.quote_lamports) : undefined,
          });
        }
        const reply = JSON.stringify({ type: 'reply', seq: batch.seq, orders });
        process.stdout.write(reply + '\n');
      } else if (msg.type === 'done') {
        doneMsg = msg as unknown as DoneMsg;
      }
    }
    if (doneMsg !== null) break;
  }

  // Best-effort run summary for the operator. It is written under
  // MAKEFUN_OUTPUT_DIR (default /output), never into the repository, and a
  // failure to write it must never fail the run: the action log is the score.
  try {
    mkdirSync(OUTPUT_DIR, { recursive: true });
    const finalCash = doneMsg !== null ? BigInt(doneMsg.account.cash_lamports) : bot.cashLamports;
    const summary = {
      bot_dir: SCRIPT_DIR,
      batches: bot.batches,
      filled_buys: bot.filledBuys,
      filled_sells: bot.filledSells,
      distinct_mints_traded: bot.coinsTraded.size,
      round_trips: bot.roundTrips.size,
      final_cash_lamports: finalCash.toString(),
      open_positions_at_done: doneMsg !== null ? doneMsg.account.positions.length : bot.positions.size,
    };
    writeFileSync(join(OUTPUT_DIR, 'summary.json'), JSON.stringify(summary, null, 2) + '\n', 'utf8');
  } catch (err) {
    console.error(`summary write failed (ignored): ${err instanceof Error ? err.message : String(err)}`);
  }
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exit(1);
});
