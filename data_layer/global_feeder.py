"""
data_layer/global_feeder.py — Admin-managed global data feed.

The GlobalFeeder is the single canonical data source for all
downstream components. It wraps one (or a failover pair of) broker
websocket connections, normalizes every raw frame, and fans it out
across the EventBus.

Key design rules:
  • Only ONE GlobalFeeder instance runs at a time (managed by AdminConsole).
  • No strategy or execution logic lives here — pure data normalization.
  • Connection health is monitored via a heartbeat task; reconnect is
    automatic without blocking any other coroutine.
  • No time.sleep — all waits use asyncio.sleep or asyncio.Event.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import (
    BaseFeeder, CandleEvent, EventBus, IndexTick, OptionTick, SystemEvent,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Mock Feeder  (no external dependencies — default provider)
# ─────────────────────────────────────────────────────────────────────────────

class MockFeeder(BaseFeeder):
    """
    Generates synthetic IST-timestamped ticks for all monitored indices.
    Used in paper-trading and testing modes.

    Implements the two-stage BaseFeeder contract:
      _ws_loop()    — generates synthetic "raw" frames and enqueues them
      _parse_frame() — converts each frame into IndexTick/OptionTick and publishes
    """

    _BASE: Dict[str, float] = {
        "NIFTY": 24_500.0, "BANKNIFTY": 52_000.0,
        "FINNIFTY": 23_000.0, "SENSEX": 80_000.0, "MIDCPNIFTY": 12_000.0,
        "CRUDEOIL": 6_200.0,
    }

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        super().__init__(bus)
        self._cfg = cfg
        self._prices: Dict[str, float] = dict(self._BASE)
        self._tick_interval = 0.1          # 100 ms per tick batch
        self._rng = random.Random()

    async def connect(self) -> bool:
        self._connected = True
        logger.info("MockFeeder: connected (synthetic mode).")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        logger.debug("MockFeeder: subscribed to %d tokens.", len(tokens))

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        pass

    async def _ws_loop(self) -> None:
        """
        Simulate a WebSocket receive loop.  Each 'frame' is a pre-built dict
        that _parse_frame() will convert into typed ticks.  No heavy computation
        happens here — the frame dict is created cheaply and enqueued immediately.
        """
        from data_layer.instrument_registry import next_expiry as _nexp
        while self._running:
            now = datetime.now(IST)

            for underlying in self._cfg.monitored_indices:
                expiry = _nexp(underlying) or (now.date() + __import__("datetime").timedelta(days=7))
                # Commodities/unknowns have no synthetic base — skip in mock mode
                # (the live dual feed provides their futures/ATM ticks).
                if underlying not in self._prices:
                    continue
                p = self._prices[underlying]
                p = max(p * (1 + self._rng.gauss(0, 0.0003)), 1.0)
                self._prices[underlying] = p
                step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
                atm = round(p / step) * step

                # Enqueue one raw frame per underlying (cheap dict, no computation)
                self._enqueue_raw({
                    "type": "batch",
                    "underlying": underlying,
                    "ltp": round(p, 2),
                    "atm": atm,
                    "step": step,
                    "expiry": expiry,
                    "timestamp": now,
                })

            await asyncio.sleep(self._tick_interval)

    async def _parse_frame(self, raw: Any) -> None:
        """
        Expand one raw batch frame into IndexTick + OptionTicks and publish.
        All CPU work (option pricing math, random draws) happens here,
        isolated from the WS receive path.
        """
        underlying = raw["underlying"]
        p          = raw["ltp"]
        atm        = raw["atm"]
        step       = raw["step"]
        expiry     = raw["expiry"]
        now        = raw["timestamp"]

        tick = IndexTick(
            symbol=underlying, ltp=p,
            open=round(p * 0.9998, 2), high=round(p * 1.001, 2),
            low=round(p * 0.999, 2), close=p,
            volume=self._rng.randint(1_000, 50_000), timestamp=now,
        )
        await self._publish_index(tick)

        for i in range(-3, 4):
            strike = atm + i * step
            for opt_type in ("CE", "PE"):
                intrinsic = max((p - strike) if opt_type == "CE" else (strike - p), 0)
                ltp = max(intrinsic + abs(self._rng.gauss(50, 15)), 0.5)
                opt = OptionTick(
                    symbol=f"{underlying}OPT{strike}{opt_type}",
                    underlying=underlying, strike=strike,
                    option_type=opt_type, expiry=expiry,
                    ltp=round(ltp, 2), bid=round(ltp - 0.5, 2),
                    ask=round(ltp + 0.5, 2),
                    oi=self._rng.randint(100_000, 10_000_000),
                    change_oi=self._rng.randint(-100_000, 200_000),
                    volume=self._rng.randint(1_000, 300_000),
                    iv=round(abs(self._rng.gauss(15, 3)), 2),
                    delta=round(0.5 - i * 0.08, 4),
                    timestamp=now,
                )
                await self._publish_option(opt)



# ─────────────────────────────────────────────────────────────────────────────
# DedupBuffer — per-symbol tick deduplication for dual-feed setups
# ─────────────────────────────────────────────────────────────────────────────

class DedupBuffer:
    """
    Tracks the last accepted (monotonic_ts, ltp) per symbol.

    accept() returns True (and updates state) when:
      • first tick for the symbol, OR
      • >= 100 ms elapsed since last accepted tick, OR
      • price moved more than 1e-4 (absolute).

    All other ticks are silently dropped → returns False.
    """

    def __init__(self) -> None:
        self._last: Dict[str, Tuple[float, float]] = {}   # symbol → (ts, ltp)
        # Active-PASSIVE failover: when a primary provider is set, ONLY the primary
        # feeder's ticks drive prices; the secondary is used only when the primary
        # goes stale (down). This prevents two feeds disagreeing on the same
        # contract (the price flip-flop, e.g. 711 vs 365). None → legacy active-active.
        self._primary: Optional[str] = None
        self._stale_sec: float = 3.0
        # 2026-08-23 fix: PER-SYMBOL, not a single global timestamp. The old
        # single self._last_primary_ts got refreshed by ANY symbol ticking
        # from the primary provider -- so if the primary silently dropped
        # just ONE strike/index (a real, plausible failure: a subscription
        # quietly lost, a specific contract's feed stalling) while every
        # OTHER symbol on that same connection kept ticking fine, the
        # primary looked "healthy" globally and the secondary's real ticks
        # for that one dead symbol were still rejected -- defeating the
        # entire purpose of having a backup feed for exactly this scenario.
        # Now each symbol's own primary-side freshness is tracked
        # independently, so failover engages per-symbol, not all-or-nothing.
        self._last_primary_ts: Dict[str, float] = {}
        self._primary_set_ts: float = 0.0   # boot-time reference for never-yet-seen symbols

    def set_primary(self, provider: Optional[str], stale_sec: float = 3.0) -> None:
        self._primary = (provider or "").lower() or None
        self._stale_sec = stale_sec
        # Treat the primary as "just ticked" at startup so the secondary is NOT used
        # in the boot window before the primary's first tick (which would capture a
        # stale secondary price at entry). The secondary only takes over after the
        # primary actually goes stale_sec without a tick. Reset the per-symbol map --
        # a fresh boot/reconnect shouldn't inherit staleness verdicts from before.
        self._primary_set_ts = time.monotonic()
        self._last_primary_ts = {}

    def accept(self, symbol: str, ltp: float, provider: Optional[str] = None) -> bool:
        now = time.monotonic()
        # Active-passive gate — evaluated PER SYMBOL now (see __init__'s own
        # comment on why a global timestamp silently defeated single-strike
        # failover).
        if self._primary is not None and provider is not None:
            if provider.lower() == self._primary:
                self._last_primary_ts[symbol] = now
            else:
                # A symbol the primary has never ticked yet falls back to the
                # boot-time reference (preserves the original boot-window
                # guard); once the primary has ticked THIS symbol at least
                # once, only that symbol's own freshness matters.
                last_primary_for_symbol = self._last_primary_ts.get(symbol, self._primary_set_ts)
                if (now - last_primary_for_symbol) < self._stale_sec:
                    return False   # secondary dropped while THIS symbol's primary feed is healthy
        entry = self._last.get(symbol)
        if entry is None:
            self._last[symbol] = (now, ltp)
            return True
        prev_ts, prev_ltp = entry
        if (now - prev_ts) >= 0.100 or abs(ltp - prev_ltp) > 1e-4:
            self._last[symbol] = (now, ltp)
            return True
        return False

    def seconds_since_last_tick(self, symbol: str) -> Optional[float]:
        """None if this symbol has never had a genuinely-accepted tick yet
        (never seen, or still filtered out) -- a real staleness watchdog
        must never read that as "0 seconds ago", only as "no data at all
        yet". self._last is already the true per-symbol last-accepted-tick
        timestamp (updated by accept() from EITHER provider, active or
        standby, whichever one is actually delivering) -- exactly what a
        "is this symbol's feed alive at all" check needs, independent of
        which provider is currently primary."""
        entry = self._last.get(symbol)
        if entry is None:
            return None
        return time.monotonic() - entry[0]


# ─────────────────────────────────────────────────────────────────────────────
# UpstoxFeeder — stub for Upstox API v2 WebSocket feed
# ─────────────────────────────────────────────────────────────────────────────

# Broker WebSocket per-connection symbol cap. Above this the broker may silently drop
# the excess subscriptions (no ticks for those symbols). Conservative warn threshold.
_WS_SYMBOL_LIMIT = 50

_UPSTOX_INDEX_KEY_TO_INTERNAL: Dict[str, str] = {
    "NSE_INDEX|Nifty 50":        "NIFTY",
    "NSE_INDEX|Nifty Bank":      "BANKNIFTY",
    "NSE_INDEX|Nifty Fin Service": "FINNIFTY",
    "NSE_INDEX|NIFTY MID SELECT": "MIDCPNIFTY",
    "BSE_INDEX|SENSEX":          "SENSEX",
}


class UpstoxFeeder(BaseFeeder):
    """
    Live Upstox API v3 data feeder using MarketDataStreamerV3.

    Streams real-time INDEX_TICK and OPTION_TICK events for all configured
    monitored indices. The WebSocket runs in a thread via asyncio.to_thread;
    the on_message callback uses run_coroutine_threadsafe to safely enqueue
    frames into the asyncio raw queue.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        self._streamer = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed_keys: List[str] = []   # all currently subscribed instrument keys
        self._extra_spot_keys: Dict[str, str] = {}  # NSE_EQ|<ISIN> → "EICHERMOT"
        try:
            import upstox_client  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False
        # Upstox SDK has its own auto-reconnect loop; DualFeeder should not
        # fight it by creating a new streamer on every close.
        self._uses_sdk_reconnect = True

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    def _index_instrument_keys(self) -> List[str]:
        """
        Upstox instrument keys for monitored instruments. MCX commodities use the
        near-month FUTURES instrument_key from the registry as the ATM source --
        2026-08-26: so does any underlying listed in cfg.futures_atm_underlyings
        (see GlobalConfig's own docstring for that field for the full rationale).
        """
        from data_layer.symbol_translator import SymbolTranslator
        from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_UPSTOX_INDEX_KEY_TO_INTERNAL.values())
        )
        _futures_atm = {u.upper() for u in (getattr(self._cfg, "futures_atm_underlyings", None) or [])}
        keys: List[str] = []
        for i in indices:
            if i.upper() in _MCX_UNDERLYINGS:
                fk = REGISTRY.get_futures_upstox(i.upper())
                if fk:
                    keys.append(fk)
                # MCX has no separate "spot index" key to fall back to -- unchanged
                # from before this 2026-08-26 change: no futures key yet means no
                # subscription this cycle, same as it always has.
            elif i.upper() in _futures_atm:
                # 2026-08-26, direct user spec revision: SellStraddle now wants BOTH
                # the real spot AND the futures price simultaneously (to compute their
                # mean for ATM), not futures-instead-of-spot -- so subscribe to both
                # keys, always, for a futures_atm underlying.
                keys.append(SymbolTranslator.to_upstox_index(i))
                fk = REGISTRY.get_futures_upstox(i.upper())
                if fk:
                    keys.append(fk)
                else:
                    # Futures key not resolved yet (startup-ordering race against
                    # REGISTRY load) -- spot alone still subscribes this cycle so
                    # ticks aren't lost entirely; self-corrects on the next rebuild
                    # once the futures key resolves (this list is rebuilt on
                    # reconnect).
                    logger.warning(
                        "UpstoxFeeder: futures_atm underlying %s has no resolved "
                        "futures key yet -- subscribing to spot only this cycle.", i,
                    )
            else:
                keys.append(SymbolTranslator.to_upstox_index(i))
        return keys

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "UpstoxFeeder: upstox_client SDK not installed — "
                "pip install upstox-client.  Feeder will not connect."
            )
            return False
        access_token = self._creds.get("access_token", "")
        if not access_token:
            logger.warning("UpstoxFeeder: no access_token in credentials — cannot connect.")
            return False

        self._loop = asyncio.get_running_loop()

        import upstox_client

        cfg_obj = upstox_client.Configuration()
        cfg_obj.access_token = access_token
        api_client_obj = upstox_client.ApiClient(cfg_obj)

        # Combine index keys + cached option keys into initial subscription list
        index_keys = self._index_instrument_keys()
        all_keys = list(index_keys)
        for k in self._subscribed_keys:
            if k not in all_keys:
                all_keys.append(k)
        self._subscribed_keys = all_keys

        def _on_open() -> None:
            self._connected = True
            # Re-assert ALL keys in "full" mode. Keys passed to the streamer CONSTRUCTOR
            # (instrumentKeys=, e.g. the trap's pre-connect day-locked strikes) were not
            # reliably streaming option data — only keys added AFTER connect via an explicit
            # subscribe(keys, "full") ticked. This explicit on-connect re-subscribe makes the
            # pre-connect keys (trap legs) stream too → fixes frozen trap LTP (ticks/min=0).
            try:
                if self._subscribed_keys:
                    try:
                        self._streamer.subscribe(self._subscribed_keys, "full")
                    except TypeError:
                        self._streamer.subscribe(self._subscribed_keys)
            except Exception as exc:
                logger.warning("UpstoxFeeder: on_open full re-subscribe failed: %s", exc)
            logger.info(
                "UpstoxFeeder: WebSocket connected — subscribed to %d keys (%d index, %d option).",
                len(self._subscribed_keys),
                len(index_keys),
                len(self._subscribed_keys) - len(index_keys),
            )

        def _on_message(message: bytes) -> None:
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self._parse_frame(message), self._loop
                )

        def _on_error(error) -> None:
            logger.warning("UpstoxFeeder: WS error: %s", error)

        def _on_close(*args, **kwargs) -> None:
            logger.info("UpstoxFeeder: WebSocket closed. args=%s", args)
            self._connected = False

        self._streamer = upstox_client.MarketDataStreamerV3(
            api_client=api_client_obj,
            instrumentKeys=self._subscribed_keys,
            mode="full",
        )
        self._streamer.on("open", _on_open)
        self._streamer.on("message", _on_message)
        self._streamer.on("error", _on_error)
        self._streamer.on("close", _on_close)
        # Let the Upstox SDK manage its own reconnects with a sensible interval.
        # DualFeeder will not spin-reconnect for this provider.
        self._streamer.auto_reconnect(True, interval=5, retry_count=99999)

        logger.info("UpstoxFeeder: streamer created — will connect in _ws_loop.")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._streamer:
            try:
                self._streamer.disconnect()
            except Exception:
                pass
            self._streamer = None

    @staticmethod
    def _is_upstox_key(token: str) -> bool:
        """Upstox instrument_keys contain a pipe (e.g. NSE_FO|...). Fyers symbols don't."""
        return "|" in token

    def _to_upstox_key(self, token: str) -> Optional[str]:
        """Convert Fyers symbols / internal canonical / MCX symbols into Upstox instrument keys."""
        if self._is_upstox_key(token):
            return token
        from data_layer.symbol_translator import SymbolTranslator
        from data_layer.instrument_registry import REGISTRY
        # Fyers index symbol (NSE:NIFTY50-INDEX, MCX:CRUDEOIL26JUNFUT)
        internal_idx = _FYERS_TO_INTERNAL.get(token) or _mcx_fyers_fut_to_internal(token)
        if internal_idx:
            return SymbolTranslator.to_upstox_index(internal_idx)
        # Fyers option symbol
        if token.startswith(("NSE:", "BSE:", "MCX:")):
            sym = SymbolTranslator.from_fyers(token)
            if sym is None:
                sym = _parse_mcx_fyers_option(token)
            if sym is not None:
                if isinstance(sym, tuple):
                    und, strike, ot, exp = sym
                else:
                    und, strike, ot, exp = sym.underlying, sym.strike, sym.option_type, sym.expiry
                return REGISTRY.get_broker_symbol(und, exp, int(strike), ot, "upstox")
        return None

    def set_rebalancer(self, rebalancer) -> None:
        """Optional StrikeRebalancer reference so market-close cleanup can avoid
        unsubscribing strikes that are pinned by an open position."""
        self._rebalancer = rebalancer

    def register_extra_spot_keys(self, mapping: Dict[str, str]) -> None:
        """Register NSE_EQ instrument keys → ticker names so stock ticks flow as INDEX_TICK.
        Also subscribes the keys on the active WebSocket streamer so Upstox actually sends them."""
        self._extra_spot_keys.update(mapping)
        new_keys = [k for k in mapping if k not in self._subscribed_keys]
        if not new_keys:
            return
        for k in new_keys:
            self._subscribed_keys.append(k)
        if self._streamer:
            try:
                try:
                    self._streamer.subscribe(new_keys, "full")
                except TypeError:
                    self._streamer.subscribe(new_keys)
                logger.info("UpstoxFeeder: subscribed %d equity spot keys: %s", len(new_keys), new_keys)
            except Exception as exc:
                logger.warning("UpstoxFeeder: equity spot subscribe error: %s", exc)

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        # In dual mode _strikes_to_tokens() (strike_rebalancer.py) deliberately sends BOTH
        # a native Upstox instrument_key AND a Fyers-format symbol for every leg in the same
        # call, so each feeder in the pair can filter to its own format. Converting the
        # Fyers-format one back to Upstox here produces the SAME key as the native one for
        # that leg -- so `mine` can contain true duplicates. The old dedup only checked
        # against self._subscribed_keys (state from PRIOR calls), never against duplicates
        # arising within this same call, so every leg got appended (and re-sent to the WS)
        # twice -- inflating the ~50/connection limit warning 2x on every fresh subscribe
        # (2026-07-24: confirmed live via /api/admin/subscribed_keys — every option key
        # appeared exactly twice, 74 "subscribed" vs 38 truly distinct symbols).
        mine: List[str] = []
        for t in tokens:
            if self._is_upstox_key(t):
                mine.append(t)
            else:
                ukey = self._to_upstox_key(t)
                if ukey:
                    mine.append(ukey)
                else:
                    logger.debug("UpstoxFeeder: could not convert token %s to Upstox key", t)
        mine = list(dict.fromkeys(mine))  # de-dupe within this call, preserve order
        new_keys = [t for t in mine if t not in self._subscribed_keys]
        if not new_keys:
            return
        for k in new_keys:
            self._subscribed_keys.append(k)
        _total = len(self._subscribed_keys)
        if _total > _WS_SYMBOL_LIMIT:
            logger.warning(
                "UpstoxFeeder: %d symbols subscribed — EXCEEDS the ~%d/connection WS limit. "
                "The broker may SILENTLY DROP the excess (e.g. SENSEX legs subscribed last get "
                "no ticks). Reduce the subscription set (subscribe only DEPLOYED instruments).",
                _total, _WS_SYMBOL_LIMIT,
            )
        if self._streamer:   # don't gate on possibly-stale _connected flag
            try:
                # SDK signature: subscribe(instrumentKeys, mode='ltpc') — positional.
                # Some SDK builds accept only keys; fall back if mode is rejected.
                try:
                    self._streamer.subscribe(new_keys, "full")
                except TypeError:
                    self._streamer.subscribe(new_keys)
                logger.info("UpstoxFeeder: subscribed %d option keys (total now %d).",
                            len(new_keys), _total)
            except Exception as exc:
                logger.warning("UpstoxFeeder: subscribe error: %s", exc)

    async def resubscribe_tokens(self, tokens: List[str]) -> None:
        """Force-resubscribe tokens even if already in _subscribed_keys.
        Use in engine heartbeats to recover from silent WS subscription drops
        (Upstox SDK can silently stop delivering ticks for subscribed keys without
        triggering a reconnect — explicit re-subscribe recovers them)."""
        mine: List[str] = []
        for t in tokens:
            if self._is_upstox_key(t):
                mine.append(t)
            else:
                ukey = self._to_upstox_key(t)
                if ukey:
                    mine.append(ukey)
        mine = list(dict.fromkeys(mine))  # de-dupe within this call (see subscribe_tokens)
        if not mine:
            return
        for k in mine:
            if k not in self._subscribed_keys:
                self._subscribed_keys.append(k)
        if self._streamer:
            try:
                try:
                    self._streamer.subscribe(mine, "full")
                except TypeError:
                    self._streamer.subscribe(mine)
                logger.info("UpstoxFeeder: force-resubscribed %d keys.", len(mine))
            except Exception as exc:
                logger.warning("UpstoxFeeder: resubscribe error: %s", exc)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        mine: List[str] = []
        for t in tokens:
            if self._is_upstox_key(t):
                mine.append(t)
            else:
                ukey = self._to_upstox_key(t)
                if ukey:
                    mine.append(ukey)
        for t in mine:
            if t in self._subscribed_keys:
                self._subscribed_keys.remove(t)
        # NOTE: intentionally NOT calling self._streamer.unsubscribe() here.
        # The Upstox SDK unsubscribe call triggers a WS reconnect which kills ALL
        # subscriptions (including MCX options). Removing from _subscribed_keys
        # is enough — the keys won't re-subscribe on reconnect and the extra ticks
        # from already-subscribed NSE strikes are ignored harmlessly.

    async def fetch_option_chain(self, underlying_key: str, expiry_date: date) -> Optional[Dict[str, Any]]:
        """Fetch Upstox /v2/option/chain for the underlying + expiry. Returns plain dict."""
        if not self._sdk_available:
            return None
        access_token = self._creds.get("access_token", "")
        if not access_token:
            logger.warning("UpstoxFeeder: fetch_option_chain skipped — no access_token.")
            return None
        try:
            import upstox_client
            from upstox_client.api.options_api import OptionsApi
            cfg_obj = upstox_client.Configuration()
            cfg_obj.access_token = access_token
            api_client = upstox_client.ApiClient(cfg_obj)
            api = OptionsApi(api_client)
            expiry_str = expiry_date.strftime("%Y-%m-%d")
            resp = await asyncio.to_thread(api.get_put_call_option_chain, underlying_key, expiry_str)
            if resp is None:
                return None
            data = resp.to_dict()
            logger.info(
                "UpstoxFeeder: fetched option chain %s expiry=%s rows=%d",
                underlying_key, expiry_str,
                len(data.get("data") or []),
            )
            return data
        except Exception as exc:
            logger.warning("UpstoxFeeder: fetch_option_chain failed: %s", exc)
            return None

    async def _ws_loop(self) -> None:
        if not self._streamer:
            return
        self._running = True
        try:
            await asyncio.to_thread(self._streamer.connect)
        except Exception as exc:
            logger.error("UpstoxFeeder: _ws_loop ended with error: %s", exc)
        finally:
            self._connected = False
            self._running = False

    def _get_option_meta(self, inst_key: str):
        """
        Return (underlying, strike, opt_type, expiry) for an Upstox instrument_key,
        using a lazily-built reverse lookup cache. Rebuilds when registry grows.
        """
        from data_layer.instrument_registry import REGISTRY
        from datetime import date as _date

        total = sum(len(v) for v in REGISTRY._upstox_keys.values())
        if not hasattr(self, "_rev_map") or total != getattr(self, "_rev_map_size", -1):
            rev: Dict[str, tuple] = {}
            for underlying, kmap in REGISTRY._upstox_keys.items():
                for (exp_str, strike, opt_type), stored_key in kmap.items():
                    try:
                        expiry = _date.fromisoformat(exp_str)
                    except ValueError:
                        continue
                    rev[stored_key] = (underlying, strike, opt_type, expiry)
            self._rev_map: Dict[str, tuple] = rev
            self._rev_map_size: int = total
        return self._rev_map.get(inst_key)

    @staticmethod
    def _extract_ltp(feed_data) -> Optional[float]:
        """
        Extract LTP from a decoded Upstox feed entry (dict form from MarketDataStreamerV3).
        Handles full mode (fullFeed.marketFF / fullFeed.indexFF) and ltpc mode.
        """
        if not isinstance(feed_data, dict):
            return None
        # Full mode
        ff = feed_data.get("fullFeed") or feed_data.get("ff")
        if isinstance(ff, dict):
            for sub in ("marketFF", "indexFF"):
                blk = ff.get(sub)
                if isinstance(blk, dict):
                    ltp = (blk.get("ltpc") or {}).get("ltp")
                    if ltp:
                        return float(ltp)
        # ltpc mode
        ltpc = feed_data.get("ltpc")
        if isinstance(ltpc, dict) and ltpc.get("ltp"):
            return float(ltpc["ltp"])
        return None

    @staticmethod
    def _extract_extras(feed_data) -> Dict[str, float]:
        """Extract OI, volume, and ATP (broker VWAP) from a full-mode dict feed entry."""
        result: Dict[str, float] = {"oi": 0, "volume": 0, "atp": 0.0}
        if not isinstance(feed_data, dict):
            return result
        ff = feed_data.get("fullFeed") or feed_data.get("ff") or {}
        mff = ff.get("marketFF") if isinstance(ff, dict) else None
        if isinstance(mff, dict):
            try:
                result["oi"] = int(float(mff.get("oi") or 0))
            except (TypeError, ValueError):
                pass
            try:
                result["volume"] = int(float(mff.get("vtt") or 0))
            except (TypeError, ValueError):
                pass
            try:
                # ATP = exchange average traded price = broker VWAP for this contract
                result["atp"] = float((mff.get("eFeedDetails") or {}).get("atp") or mff.get("atp") or 0.0)
            except (TypeError, ValueError):
                pass
        return result

    async def _parse_frame(self, raw: Any) -> None:
        # MarketDataStreamerV3 on("message") delivers an already-decoded dict in
        # recent SDKs; older builds emit raw protobuf bytes. Handle both.
        decoded = raw
        if not isinstance(raw, dict):
            try:
                import upstox_client
                obj = upstox_client.MarketDataStreamerV3.decode_protobuf(raw)
                # Convert protobuf to dict if helper available
                decoded = obj if isinstance(obj, dict) else getattr(obj, "__dict__", {}) or {}
            except Exception as exc:
                if not hasattr(self, "_logged_raw_type"):
                    self._logged_raw_type = True
                    logger.warning("UpstoxFeeder: cannot decode message type=%s err=%s sample=%r",
                                   type(raw).__name__, exc, str(raw)[:200])
                return

        if not hasattr(self, "_logged_raw_type"):
            self._logged_raw_type = True
            logger.info("UpstoxFeeder: first raw message type=%s keys=%s",
                        type(raw).__name__,
                        list(decoded.keys())[:6] if isinstance(decoded, dict) else "n/a")

        feeds = decoded.get("feeds") if isinstance(decoded, dict) else None
        if not isinstance(feeds, dict):
            return

        if not hasattr(self, "_logged_first_tick"):
            self._logged_first_tick = True
            logger.info("UpstoxFeeder: first decoded frame keys sample: %s", list(feeds.keys())[:3])

        now = datetime.now(IST)

        for inst_key, feed_data in feeds.items():
            # Diagnostic: log first MCX option tick received (one-shot)
            if inst_key.startswith("MCX_FO|") and not inst_key == "MCX_FO|499095":
                _dk = f"_mcxoptlog_{inst_key}"
                if not getattr(self, _dk, False):
                    setattr(self, _dk, True)
                    ltp_raw = self._extract_ltp(feed_data)
                    logger.info("UpstoxFeeder: MCX option tick received key=%s ltp=%s", inst_key, ltp_raw)

            ltp = self._extract_ltp(feed_data)
            if ltp is None or ltp == 0.0:
                continue

            # ── Index tick ──────────────────────────────────────────────────
            _spot_name = _UPSTOX_INDEX_KEY_TO_INTERNAL.get(inst_key) or self._extra_spot_keys.get(inst_key)
            _fut_name = None if _spot_name else _mcx_upstox_fut_to_internal(inst_key)
            internal_name = _spot_name or _fut_name
            if internal_name:
                # 2026-08-26, direct user spec: futures_atm underlyings now carry BOTH
                # a real spot key AND a futures key at once (see _index_instrument_keys)
                # -- tag which one this tick came from so SellStraddle can track both
                # and compute their mean, instead of one silently overwriting the other.
                _source = "futures" if _fut_name else "spot"
                # Diagnostic: log the raw index value per key (throttled) so a mis-valued
                # SENSEX/BSE index (wrong strikes) is immediately visible.
                _dk = f"_idxlog_{internal_name}_{_source}"
                if time.monotonic() - getattr(self, _dk, 0.0) > 30.0:
                    setattr(self, _dk, time.monotonic())
                    logger.info("UpstoxFeeder: INDEX %s (%s) key=%s ltp=%.2f",
                                internal_name, _source, inst_key, ltp)
                tick = IndexTick(
                    symbol=internal_name,
                    ltp=ltp,
                    open=ltp, high=ltp, low=ltp, close=ltp,
                    volume=0,
                    timestamp=now,
                    source=_source,
                )
                await self._publish_index(tick)
                continue

            # ── Option tick — look up via cached reverse map ──────────────
            meta = self._get_option_meta(inst_key)
            if not meta:
                # Log unrecognised option keys (throttled) so silent drops are visible
                _dk = f"_unrecog_{inst_key}"
                if time.monotonic() - getattr(self, _dk, 0.0) > 60.0:
                    setattr(self, _dk, time.monotonic())
                    logger.warning("UpstoxFeeder: option tick key=%s not in REGISTRY — tick dropped", inst_key)
            if meta:
                underlying, strike, opt_type, expiry = meta
                extras = self._extract_extras(feed_data)
                opt_tick = OptionTick(
                    symbol=inst_key,
                    underlying=underlying,
                    strike=float(strike),
                    option_type=opt_type,
                    expiry=expiry,
                    ltp=ltp,
                    bid=ltp,
                    ask=ltp,
                    oi=extras["oi"],
                    change_oi=0,
                    volume=extras["volume"],
                    iv=0.0,
                    delta=0.0,
                    timestamp=now,
                    atp=float(extras.get("atp") or 0.0),  # broker VWAP
                )
                await self._publish_option(opt_tick)


# ─────────────────────────────────────────────────────────────────────────────
# FyersFeeder — stub for Fyers API v3 WebSocket feed
# ─────────────────────────────────────────────────────────────────────────────

# 2026-08-04: briefly split (Upstox=index/options exclusively, Fyers=FnO
# equity exclusively) then REVERTED same-session -- the FnO watchlist is
# only ~30 stocks (the nightly scan's own top-N, not the full ~200-stock
# universe), small enough that mirroring it to BOTH providers alongside
# indices/options costs little and keeps the existing active-passive
# failover intact for everything, not just indices. Kept as a toggle
# (rather than deleting the split code) in case a future universe size
# makes the exclusive split worth revisiting.
_FYERS_CARRIES_INDEX_OPTIONS = True

_FYERS_INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY":      "NSE:NIFTY50-INDEX",
    "BANKNIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":   "NSE:FINNIFTY-INDEX",
    "SENSEX":     "BSE:SENSEX-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
}
_FYERS_TO_INTERNAL: Dict[str, str] = {v: k for k, v in _FYERS_INDEX_SYMBOLS.items()}


def _mcx_fyers_fut_to_internal(symbol: str) -> Optional[str]:
    """Map a futures Fyers symbol (e.g. MCX:CRUDEOIL26JUNFUT, or NSE:NIFTY...FUT
    for any underlying in cfg.futures_atm_underlyings) back to its internal name
    (e.g. 'CRUDEOIL', 'NIFTY'). 2026-08-26: scans every underlying REGISTRY has
    ever resolved a futures key for, not just _MCX_UNDERLYINGS -- a futures tick
    only ever arrives here at all if something upstream (_index_symbols) chose
    to subscribe to it, so a broader match here is safe and needs no separate
    cfg threading through this free function."""
    from data_layer.instrument_registry import REGISTRY
    if not symbol:
        return None
    for u, sym in REGISTRY._futures_fyers.items():
        if sym == symbol:
            return u
    return None


def _mcx_upstox_fut_to_internal(ikey: str) -> Optional[str]:
    """Map a futures Upstox instrument_key (e.g. MCX_FO|499095, or NIFTY's own
    futures key for any underlying in cfg.futures_atm_underlyings) back to its
    internal name. See _mcx_fyers_fut_to_internal's own docstring for why this
    scans every resolved futures key, not just MCX."""
    from data_layer.instrument_registry import REGISTRY
    if not ikey:
        return None
    for u, key in REGISTRY._futures_upstox.items():
        if key == ikey:
            return u
    return None


import re as _re
_MCX_FY_OPT_RE = _re.compile(r"^MCX:([A-Z]+?)(\d{2})([A-Z]{3})(\d+)(CE|PE)$")


def _parse_mcx_fyers_option(symbol: str):
    """Parse 'MCX:CRUDEOIL26JUN8850CE' -> (underlying, strike, opt_type, expiry).

    Resolves the expiry actually ENCODED in the symbol (yy+mon), not just
    "whatever the registry currently considers active" -- mirrors
    SymbolTranslator.from_fyers()'s own monthly-format resolution
    (data_layer/symbol_translator.py), which already learned this lesson
    (see get_active_expiry_strict's 2026-08-09 incident docstring: silently
    substituting the active/nearest expiry for a specific requested one fed
    real historical prices for the WRONG contract, valid-looking data with a
    wrong-month bug). A stale/rolled tick for a symbol whose exact month
    isn't in the currently-loaded registry falls back to get_active_expiry
    (logged, since that fallback path is the one that can silently mismatch).
    """
    m = _MCX_FY_OPT_RE.match(symbol or "")
    if not m:
        return None
    underlying, yy, mon3, strike, ot = m.groups()
    from data_layer.instrument_registry import REGISTRY
    from data_layer.symbol_translator import _MONTH_3
    exp = None
    try:
        year = 2000 + int(yy)
        month = _MONTH_3.index(mon3) + 1
        month_exps = [e for e in REGISTRY.all_expiries(underlying) if e.year == year and e.month == month]
        if month_exps:
            exp = max(month_exps)
    except Exception:
        exp = None
    if exp is None:
        logger.warning("_parse_mcx_fyers_option: no loaded expiry matches %s%s in symbol %r -- "
                        "falling back to the current active expiry (may be a different contract).",
                        yy, mon3, symbol)
        exp = REGISTRY.get_active_expiry(underlying)
    return (underlying, float(strike), ot, exp)


class FyersFeeder(BaseFeeder):
    """
    Live Fyers API v3 data feeder using FyersDataSocket.

    Streams real-time INDEX_TICK events for all configured monitored indices.
    The WebSocket runs in a thread via asyncio.to_thread; the on_message callback
    uses call_soon_threadsafe to safely enqueue frames into the asyncio raw queue.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        self._socket = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed_tokens: List[str] = []  # option tokens to re-subscribe on reconnect
        # FnO equity spot routing: "NSE:RELIANCE-EQ" → "RELIANCE"
        self._fno_equity_map: Dict[str, str] = {}
        try:
            import fyers_apiv3  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False
        # Fyers SDK reconnect is disabled; DualFeeder manages reconnect with backoff.
        self._uses_sdk_reconnect = False

    def subscribe_fno_equity(self, fyers_sym: str, underlying: str) -> None:
        """
        Register a Fyers equity symbol (e.g. 'NSE:RELIANCE-EQ') to emit EQUITY_TICK
        events carrying the normalized underlying name ('RELIANCE') for Trap Scanner FnO books.
        Also subscribes the symbol on the live WebSocket if connected.
        """
        self._fno_equity_map[fyers_sym] = underlying.upper()
        if fyers_sym not in self._subscribed_tokens:
            self._subscribed_tokens.append(fyers_sym)
        if self._socket:
            try:
                self._socket.subscribe(symbols=[fyers_sym], data_type="SymbolUpdate")
            except Exception as exc:
                logger.debug("FyersFeeder.subscribe_fno_equity: socket subscribe failed: %s", exc)
        logger.info("FyersFeeder: registered FnO equity %s → %s", fyers_sym, underlying)

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    @staticmethod
    def _normalize_access_token(raw_token: str, app_id: str = "") -> str:
        """
        FyersDataSocket expects the raw JWT access_token (e.g. eyJ...).  Some callers
        pass it as 'app_id:token'.  Strip the app_id prefix when present so the SDK
        can decode the JWT and extract the hsm_key.
        """
        raw_token = (raw_token or "").strip()
        if not raw_token:
            return ""
        # If someone passed "APP-100:jwt", keep the JWT part.
        if ":" in raw_token:
            parts = raw_token.split(":", 1)
            if "." in parts[1]:
                return parts[1]
        return raw_token

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "FyersFeeder: fyers_apiv3 SDK not installed — "
                "pip install fyers-apiv3.  Feeder will not connect."
            )
            return False
        access_token = self._normalize_access_token(
            self._creds.get("access_token", ""),
            self._creds.get("api_key") or self._creds.get("app_key", ""),
        )
        if not access_token:
            logger.warning("FyersFeeder: no access_token in credentials — cannot connect.")
            return False

        self._loop = asyncio.get_running_loop()

        from fyers_apiv3.FyersWebsocket import data_ws

        def _on_message(msg: dict) -> None:
            # Use run_coroutine_threadsafe so _parse_frame runs even if _parse_task is cancelled
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._parse_frame(msg), self._loop)

        def _on_error(msg: dict) -> None:
            logger.warning("FyersFeeder: WS error: %s", msg)

        def _on_connect() -> None:
            # Subscribe all symbols from WS thread on every connect/reconnect
            self._connected = True
            symbols = self._index_symbols()
            all_symbols = list(symbols) + list(self._subscribed_tokens)
            if all_symbols and self._socket:
                self._socket.subscribe(symbols=all_symbols, data_type="SymbolUpdate")
                logger.info("FyersFeeder: connected and subscribed — %d index + %d option tokens",
                            len(symbols), len(self._subscribed_tokens))

        def _on_close(msg: dict) -> None:
            logger.info("FyersFeeder: WebSocket closed: %s", msg)
            self._connected = False

        self._socket = data_ws.FyersDataSocket(
            access_token=access_token,
            write_to_file=False,
            litemode=False,  # full mode needed for continuous option tick streaming
            reconnect=False,  # managed by DualFeeder._run_stream with exponential backoff
            on_message=_on_message,
            on_error=_on_error,
            on_connect=_on_connect,
            on_close=_on_close,
            reconnect_retry=0,
        )
        logger.info("FyersFeeder: socket created — will connect in _ws_loop.")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._socket:
            try:
                self._socket.close_connection()
            except Exception:
                pass
            self._socket = None

    def _index_symbols(self) -> List[str]:
        """
        Fyers-format 'index' symbols for all monitored instruments. For MCX
        commodities (CRUDEOIL) the ATM source is the near-month FUTURES symbol
        from the registry (e.g. MCX:CRUDEOIL26JUNFUT), not a spot index --
        2026-08-26: so is any underlying listed in cfg.futures_atm_underlyings.
        """
        if not _FYERS_CARRIES_INDEX_OPTIONS:
            return []
        from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_FYERS_INDEX_SYMBOLS.keys())
        )
        _futures_atm = {u.upper() for u in (getattr(self._cfg, "futures_atm_underlyings", None) or [])}
        syms: List[str] = []
        for i in indices:
            if i.upper() in _MCX_UNDERLYINGS:
                fut = REGISTRY.get_futures_fyers(i.upper())
                if fut:
                    syms.append(fut)
                # MCX has no separate spot symbol to fall back to -- unchanged.
            elif i.upper() in _futures_atm:
                # 2026-08-26, direct user spec revision: subscribe to BOTH spot and
                # futures for a futures_atm underlying (mean-based ATM), not futures-
                # instead-of-spot -- see UpstoxFeeder._index_instrument_keys' matching
                # comment.
                if i in _FYERS_INDEX_SYMBOLS:
                    syms.append(_FYERS_INDEX_SYMBOLS[i])
                fut = REGISTRY.get_futures_fyers(i.upper())
                if fut:
                    syms.append(fut)
                else:
                    logger.warning(
                        "FyersFeeder: futures_atm underlying %s has no resolved futures "
                        "symbol yet -- subscribing to spot only this cycle.", i,
                    )
            elif i in _FYERS_INDEX_SYMBOLS:
                syms.append(_FYERS_INDEX_SYMBOLS[i])
        return syms

    @staticmethod
    def _is_fyers_symbol(token: str) -> bool:
        """
        Fyers symbols start with an exchange prefix: NSE:NIFTY... / BSE:SENSEX...
        / MCX:CRUDEOIL... (commodities). Excludes the internal canonical format
        (NIFTY:02JUN26:...) which has no exchange prefix, and Upstox keys (...|...).
        """
        return token.startswith(("NSE:", "BSE:", "MCX:")) and "|" not in token

    def _upstox_mcx_to_fyers(self, upstox_key: str) -> Optional[str]:
        """Convert MCX_FO|<id> Upstox key → MCX:CRUDEOIL26JUL7000CE Fyers format."""
        try:
            from data_layer.instrument_registry import REGISTRY
            from data_layer.symbol_translator import SymbolTranslator
            from data_layer.instrument_registry import is_monthly_expiry
            meta = None
            for und, kmap in REGISTRY._upstox_keys.items():
                for (exp_str, strike, ot), key in kmap.items():
                    if key == upstox_key:
                        from datetime import date as _date
                        exp = _date.fromisoformat(exp_str)
                        meta = (und, strike, ot, exp)
                        break
                if meta:
                    break
            if not meta:
                return None
            und, strike, ot, exp = meta
            from data_layer.symbol_translator import InternalSymbol
            internal = InternalSymbol(underlying=und, expiry=exp, strike=strike, option_type=ot)
            return SymbolTranslator.to_fyers(internal, is_monthly=is_monthly_expiry(exp, und))
        except Exception:
            return None

    def _meta_from_upstox_key(self, key: str) -> Optional[Tuple[str, float, str, date]]:
        """Reverse-map an Upstox instrument key → (underlying, strike, opt_type, expiry)."""
        from data_layer.instrument_registry import REGISTRY
        for und, kmap in REGISTRY._upstox_keys.items():
            for (exp_str, strike, ot), stored in kmap.items():
                if stored == key:
                    return (und, float(strike), ot, date.fromisoformat(exp_str))
        return None

    def _to_fyers_symbol(self, token: str) -> Optional[str]:
        """Convert Upstox keys / internal canonical / MCX symbols into Fyers format."""
        if self._is_fyers_symbol(token):
            return token
        # Upstox instrument key → lookup via registry
        if "|" in token:
            meta = self._meta_from_upstox_key(token)
            if meta:
                und, strike, ot, exp = meta
                from data_layer.instrument_registry import REGISTRY
                return REGISTRY.get_broker_symbol(und, exp, int(strike), ot, "fyers")
            return None
        # Fyers MCX option fallback parser
        mcx = _parse_mcx_fyers_option(token)
        if mcx is not None:
            und, strike, ot, exp = mcx
            from data_layer.instrument_registry import REGISTRY
            return REGISTRY.get_broker_symbol(und, exp, int(strike), ot, "fyers")
        return None

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        # In dual mode the rebalancer usually sends Upstox-format keys.  Convert any
        # non-Fyers token (Upstox key / MCX / BSE / NSE) into the matching Fyers symbol.
        mine: List[str] = []
        for t in tokens:
            if self._is_fyers_symbol(t):
                mine.append(t)
            else:
                fy = self._to_fyers_symbol(t)
                if fy:
                    mine.append(fy)
                else:
                    logger.debug("FyersFeeder: could not convert token %s to Fyers format", t)
        # Diagnostic: reveal received vs matched so we can see why options may be 0.
        logger.info(
            "FyersFeeder.subscribe_tokens: received=%d matched_fyers=%d connected=%s sample_in=%r sample_mine=%r",
            len(tokens), len(mine), self._connected,
            tokens[:2], mine[:2],
        )
        # Remember tokens so they are re-subscribed on every reconnect
        for t in mine:
            if t not in self._subscribed_tokens:
                self._subscribed_tokens.append(t)
        if len(self._subscribed_tokens) > _WS_SYMBOL_LIMIT:
            logger.warning(
                "FyersFeeder: %d symbols subscribed — EXCEEDS the ~%d/connection WS limit. "
                "The broker may SILENTLY DROP the excess (symbols subscribed last get no ticks). "
                "Reduce the subscription set (subscribe only DEPLOYED instruments).",
                len(self._subscribed_tokens), _WS_SYMBOL_LIMIT,
            )
        # Subscribe whenever the socket exists — do NOT gate on the _connected
        # flag, which can be stale (e.g. options arrive after _on_connect already
        # fired, or during DualFeeder churn) and would silently drop the tokens.
        # The FyersDataSocket dedups duplicate subscriptions, so this is safe.
        if self._socket and mine:
            try:
                self._socket.subscribe(symbols=mine, data_type="SymbolUpdate")
                logger.info("FyersFeeder: subscribed to %d option tokens (connected=%s).",
                            len(mine), self._connected)
            except Exception as exc:
                logger.warning("FyersFeeder: subscribe_tokens error: %s", exc)

    async def resubscribe_tokens(self, tokens: List[str]) -> None:
        """Force re-subscribe tokens (used by engine heartbeats to recover silent drops)."""
        await self.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        mine: List[str] = []
        for t in tokens:
            if self._is_fyers_symbol(t):
                mine.append(t)
            else:
                fy = self._to_fyers_symbol(t)
                if fy:
                    mine.append(fy)
        for t in mine:
            if t in self._subscribed_tokens:
                self._subscribed_tokens.remove(t)
        if self._socket and self._connected and mine:
            try:
                self._socket.unsubscribe(symbols=mine, data_type="SymbolUpdate")
            except Exception as exc:
                logger.debug("FyersFeeder: unsubscribe_tokens error: %s", exc)

    async def _ws_loop(self) -> None:
        if not self._socket:
            return
        self._running = True
        try:
            await asyncio.to_thread(self._socket.connect)
            # FyersDataSocket.connect() may return after starting the background
            # thread.  Stay alive until on_close sets _connected=False or stop()
            # is called, so DualFeeder._run_stream doesn't spin-reconnect.
            while self._running and self._connected:
                await asyncio.sleep(1.0)
        except Exception as exc:
            logger.error("FyersFeeder: _ws_loop ended with error: %s", exc)
        finally:
            self._connected = False
            self._running = False

    def set_latency_tracker(self, provider: str, latency_dict: Dict[str, float]) -> None:
        """Called by DualFeeder so this feeder can record its own tick latency."""
        self._latency_provider = provider
        self._latency_dict = latency_dict

    async def _parse_frame(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            logger.info("FyersFeeder: raw frame is not dict — type=%s val=%r", type(raw).__name__, str(raw)[:200])
            return
        symbol_fyers = raw.get("symbol", "")
        ltp = raw.get("ltp")
        if not symbol_fyers or ltp is None:
            return
        if not hasattr(self, "_logged_first_tick"):
            self._logged_first_tick = True
            logger.info("FyersFeeder: first TICK frame keys=%s sample=%r", list(raw.keys()), str(raw)[:400])

        _spot_internal = _FYERS_TO_INTERNAL.get(symbol_fyers)
        _fut_internal = None if _spot_internal else _mcx_fyers_fut_to_internal(symbol_fyers)
        internal = _spot_internal or _fut_internal
        if internal:
            # 2026-08-26: tag spot vs futures -- see UpstoxFeeder's matching comment.
            _source = "futures" if _fut_internal else "spot"
            _dk = f"_idxlog_{internal}_{_source}"
            if time.monotonic() - getattr(self, _dk, 0.0) > 30.0:
                setattr(self, _dk, time.monotonic())
                logger.info("FyersFeeder: INDEX %s (%s) sym=%s ltp=%.2f",
                            internal, _source, symbol_fyers, float(ltp))
            t0 = time.monotonic()
            tick = IndexTick(
                symbol=internal,
                ltp=float(ltp),
                open=float(raw.get("open_price") or ltp),
                high=float(raw.get("high_price") or ltp),
                low=float(raw.get("low_price")  or ltp),
                close=float(raw.get("prev_close_price") or ltp),
                volume=int(raw.get("vol_traded_today") or 0),
                timestamp=datetime.now(IST),
                source=_source,
            )
            await self._publish_index(tick)
            if hasattr(self, "_latency_dict"):
                self._latency_dict[self._latency_provider] = (time.monotonic() - t0) * 1000.0
        elif symbol_fyers in self._fno_equity_map:
            # FnO equity spot tick (e.g. NSE:RELIANCE-EQ) → EQUITY_TICK for Trap Scanner FnO
            underlying_name = self._fno_equity_map[symbol_fyers]
            equity_tick = IndexTick(
                symbol=underlying_name,
                ltp=float(ltp),
                open=float(raw.get("open_price") or ltp),
                high=float(raw.get("high_price") or ltp),
                low=float(raw.get("low_price") or ltp),
                close=float(raw.get("prev_close_price") or ltp),
                volume=int(raw.get("vol_traded_today") or 0),
                timestamp=datetime.now(IST),
            )
            await self._bus.publish(Topic.EQUITY_TICK, equity_tick)
            # Also publish as INDEX_TICK so CandleCache builds 5M/75M candles for this
            # stock (drives d1_trap_fno C2 state machine via CANDLE_CLOSE events).
            await self._bus.publish(Topic.INDEX_TICK, equity_tick)
        else:
            # Option tick — parse Fyers symbol (NSE or MCX) and publish OptionTick
            try:
                from data_layer.symbol_translator import SymbolTranslator
                _u = _s = _ot = _exp = None
                sym = SymbolTranslator.from_fyers(symbol_fyers)
                if sym is not None:
                    _u, _s, _ot, _exp = sym.underlying, sym.strike, sym.option_type, sym.expiry
                else:
                    mcx = _parse_mcx_fyers_option(symbol_fyers)
                    if mcx is not None:
                        _u, _s, _ot, _exp = mcx
                if _u is not None and _exp is not None:
                    from data_layer.base_feeder import OptionTick
                    opt_tick = OptionTick(
                        symbol      = symbol_fyers,
                        underlying  = _u,
                        strike      = _s,
                        option_type = _ot,
                        expiry      = _exp,
                        ltp         = float(ltp),
                        bid         = float(raw.get("bid_price") or ltp),
                        ask         = float(raw.get("ask_price") or ltp),
                        oi          = int(raw.get("oi") or 0),
                        change_oi   = int(raw.get("chng_oi") or 0),
                        volume      = int(raw.get("vol_traded_today") or 0),
                        iv          = float(raw.get("iv") or 0.0),
                        delta       = 0.0,
                        timestamp   = datetime.now(IST),
                        atp         = float(raw.get("avg_trade_price") or 0.0),  # broker VWAP
                    )
                    await self._publish_option(opt_tick)
            except Exception as _exc:
                logger.debug("FyersFeeder: option tick parse error for %s: %s", symbol_fyers, _exc)


_ANGELONE_INDEX_TOKENS: Dict[str, Tuple[int, str]] = {
    # underlying -> (exchangeType, token). exchangeType per SmartAPI WebSocket2:
    # 1=nse_cm, 2=nse_fo, 3=bse_cm, 4=bse_fo, 5=mcx_fo. These are AngelOne's own
    # well-known, documented index tokens (NOT derived from a scrip search --
    # indices aren't in the equity/derivative scrip master the same way options
    # are). 2026-09-06: NOT yet live-verified against a real AngelOne WebSocket
    # session -- confirm these are still correct on first real connect before
    # trusting this feeder's index ticks broadly (same "verify on first real
    # day" discipline every other new integration in this codebase follows).
    "NIFTY":     (1, "99926000"),
    "BANKNIFTY": (1, "99926009"),
    "SENSEX":    (3, "99919000"),
}
_ANGELONE_TOKEN_TO_INTERNAL: Dict[Tuple[int, str], str] = {
    v: k for k, v in _ANGELONE_INDEX_TOKENS.items()
}


class AngelOneFeeder(BaseFeeder):
    """
    Live AngelOne SmartAPI data feeder using SmartWebSocketV2.

    2026-09-06: built as a free, genuinely headless (no browser/OAuth/Cloudflare
    dependency) replacement candidate for Fyers in the admin Data Feeder panel
    -- Fyers headless auto-login was confirmed non-viable (Cloudflare Turnstile
    gates its login page; see broker_auth/headless_totp_auth_fyers.py's own
    docstring for the full evidence trail). AngelOne's MPIN+TOTP headless auth
    (SmartConnect.generateSession) was ALREADY built and proven working in this
    codebase for the CLIENT EXECUTION broker (execution_bridge/broker_angel.py)
    -- this feeder reuses the exact same auth mechanic for market DATA instead.

    ⚠️ Deliberately NOT wired into the live DualFeeder Upstox+Fyers failover
    pair by this change -- that's a live-trading-critical swap the user should
    make explicitly, once they've verified via the admin panel's own toggle
    that this feeder actually streams real ticks. Registered in
    _FEEDER_REGISTRY and exposed in the admin Data Feeder panel as its own
    independently-toggleable feed for exactly that verification step.

    ⚠️ The WebSocket message field names/scaling below (paise vs rupees,
    exact dict keys) are written from SmartAPI's documented WebSocket2
    contract, NOT verified against a real live session in this codebase --
    unlike Upstox/Fyers, which have been running in production. Treat the
    first real connection as a verification pass, same discipline as every
    other "not yet live-verified" integration already documented in this
    codebase (OI-Flow, Liquidity Sweep, etc.) -- watch the logs, not just
    "did it connect."
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        self._smartapi = None
        self._socket = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # token -> (underlying, strike, opt_type, expiry) for options; used to
        # reconstruct an OptionTick from a WS message that only carries the
        # bare numeric token, not the human-readable symbol.
        self._token_meta: Dict[str, Tuple[str, float, str, date]] = {}
        self._subscribed: Dict[int, set] = {}   # exchangeType -> set of tokens
        self._scrip_cache: Dict[str, str] = {}  # (exchange, tradingsymbol) -> token, flattened key
        try:
            import SmartApi  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "AngelOneFeeder: smartapi-python SDK not installed — "
                "pip install smartapi-python pyotp.  Feeder will not connect."
            )
            return False
        client_code = self._creds.get("client_id", "")
        api_key     = self._creds.get("api_key", "")
        password    = self._creds.get("password", "")
        totp_secret = self._creds.get("totp_secret", "")
        if not (client_code and api_key and password and totp_secret):
            logger.warning(
                "AngelOneFeeder: missing client_id/api_key/password/totp_secret "
                "in admin feeder credentials — cannot connect."
            )
            return False

        self._loop = asyncio.get_running_loop()

        from SmartApi import SmartConnect
        import pyotp

        self._smartapi = SmartConnect(api_key=api_key)
        totp_code = pyotp.TOTP(totp_secret).now()
        # Same generateSession() call already proven working in
        # execution_bridge/broker_angel.py's headless client-execution path.
        session = await asyncio.to_thread(
            self._smartapi.generateSession, client_code, password, totp_code,
        )
        if not (session and session.get("status")):
            logger.error("AngelOneFeeder: headless auth failed: %s", session)
            return False
        data = session.get("data") or {}
        jwt_token  = data.get("jwtToken", "")
        feed_token = data.get("feedToken", "") or await asyncio.to_thread(self._smartapi.getfeedToken)
        if not (jwt_token and feed_token):
            logger.error("AngelOneFeeder: auth succeeded but jwtToken/feedToken missing: %s", data)
            return False
        if jwt_token.startswith("Bearer "):
            jwt_token = jwt_token[7:]

        from SmartApi.smartWebSocketV2 import SmartWebSocketV2

        def _on_data(wsapp, message) -> None:
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._parse_frame(message), self._loop)

        def _on_open(wsapp) -> None:
            self._connected = True
            logger.info("AngelOneFeeder: WebSocket connected.")
            # Re-assert all known subscriptions on every (re)connect.
            if self._subscribed:
                token_list = [
                    {"exchangeType": et, "tokens": list(toks)}
                    for et, toks in self._subscribed.items() if toks
                ]
                if token_list and self._socket:
                    try:
                        self._socket.subscribe("angelone_feed", 3, token_list)
                    except Exception as exc:
                        logger.warning("AngelOneFeeder: on_open re-subscribe failed: %s", exc)

        def _on_error(wsapp, error) -> None:
            logger.warning("AngelOneFeeder: WS error: %s", error)

        def _on_close(wsapp) -> None:
            logger.info("AngelOneFeeder: WebSocket closed.")
            self._connected = False

        self._socket = SmartWebSocketV2(jwt_token, api_key, client_code, feed_token)
        self._socket.on_open = _on_open
        self._socket.on_data = _on_data
        self._socket.on_error = _on_error
        self._socket.on_close = _on_close
        logger.info("AngelOneFeeder: authenticated, socket created — will connect in _ws_loop.")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._socket:
            try:
                self._socket.close_connection()
            except Exception:
                pass
            self._socket = None

    async def _ws_loop(self) -> None:
        if not self._socket:
            return
        self._running = True
        try:
            await asyncio.to_thread(self._socket.connect)
            while self._running and self._connected:
                await asyncio.sleep(1.0)
        except Exception as exc:
            logger.error("AngelOneFeeder: _ws_loop ended with error: %s", exc)
        finally:
            self._connected = False
            self._running = False

    def _resolve_option_token(self, underlying: str, strike: float, opt_type: str, expiry: date) -> Optional[str]:
        """Pure lookup -- resolve the numeric AngelOne symboltoken for an
        option contract via the scrip master search, caching the result.
        Mirrors execution_bridge/broker_angel.py's own _lookup_symbol,
        independently reimplemented here (fresh code, no import) since a
        feeder has no reason to depend on the execution broker's own
        internal state, same standalone-mandate precedent as every other
        strategy/component pair in this codebase. Deliberately does NOT
        touch self._subscribed -- subscription bookkeeping belongs solely
        to subscribe_tokens/unsubscribe_tokens, so there is exactly one
        place that ever decides "is this token already subscribed.\""""
        from data_layer.symbol_translator import InternalSymbol, SymbolTranslator
        internal = InternalSymbol(underlying=underlying, strike=strike, option_type=opt_type, expiry=expiry)
        tradingsymbol = SymbolTranslator.to_angelone(internal)
        exchange = "BFO" if underlying.upper() == "SENSEX" else "NFO"
        cache_key = f"{exchange}:{tradingsymbol}"
        if cache_key in self._scrip_cache:
            token = self._scrip_cache[cache_key]
            if token:
                self._token_meta[token] = (underlying, strike, opt_type, expiry)
            return token or None
        if not self._smartapi:
            return None
        try:
            res = self._smartapi.searchScrip(exchange, tradingsymbol)
            token = ""
            if res and res.get("status") and res.get("data"):
                for it in res["data"]:
                    if it.get("tradingsymbol") == tradingsymbol:
                        token = str(it.get("symboltoken", ""))
                        break
            self._scrip_cache[cache_key] = token
            if token:
                self._token_meta[token] = (underlying, strike, opt_type, expiry)
            return token or None
        except Exception as exc:
            logger.warning("AngelOneFeeder: scrip search failed for %s: %s", tradingsymbol, exc)
            return None

    def _resolve_any_token(self, token: str) -> Optional[Tuple[str, int]]:
        """Accept an Upstox key / Fyers symbol / internal canonical token
        (whatever format strike_rebalancer.py's cross-feeder broadcast sends)
        and resolve it to (real_angelone_token, exchange_type) via the scrip
        master. Pure lookup -- never mutates subscription state. Mirrors
        FyersFeeder._to_fyers_symbol's own cross-format acceptance pattern."""
        from data_layer.instrument_registry import REGISTRY
        from data_layer.symbol_translator import SymbolTranslator
        meta = None
        if "|" in token:  # Upstox instrument key
            for und, kmap in REGISTRY._upstox_keys.items():
                for (exp_str, strike, ot), stored in kmap.items():
                    if stored == token:
                        meta = (und, float(strike), ot, date.fromisoformat(exp_str))
                        break
                if meta:
                    break
        elif token.startswith(("NSE:", "BSE:")):  # Fyers symbol
            parsed = SymbolTranslator.from_fyers(token.split(":", 1)[1])
            if parsed:
                meta = (parsed.underlying, parsed.strike, parsed.option_type, parsed.expiry)
        else:
            parsed = SymbolTranslator.from_angelone(token)
            if parsed:
                meta = (parsed.underlying, parsed.strike, parsed.option_type, parsed.expiry)
        if not meta:
            return None
        und, strike, ot, exp = meta
        real_token = self._resolve_option_token(und, strike, ot, exp)
        if not real_token:
            return None
        exchange_type = 4 if und.upper() == "SENSEX" else 2
        return (real_token, exchange_type)

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        new_by_exchange: Dict[int, List[str]] = {}
        for t in tokens:
            resolved = self._resolve_any_token(t)
            if not resolved:
                logger.debug("AngelOneFeeder: could not resolve token %s", t)
                continue
            real_token, exchange_type = resolved
            if real_token not in self._subscribed.get(exchange_type, set()):
                new_by_exchange.setdefault(exchange_type, []).append(real_token)
        if not new_by_exchange:
            return
        for et, toks in new_by_exchange.items():
            self._subscribed.setdefault(et, set()).update(toks)
        if self._socket and self._connected:
            token_list = [{"exchangeType": et, "tokens": toks} for et, toks in new_by_exchange.items()]
            try:
                self._socket.subscribe("angelone_feed", 3, token_list)
                logger.info("AngelOneFeeder: subscribed to %d new option token(s).",
                            sum(len(v) for v in new_by_exchange.values()))
            except Exception as exc:
                logger.warning("AngelOneFeeder: subscribe_tokens error: %s", exc)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        by_exchange: Dict[int, List[str]] = {}
        for t in tokens:
            resolved = self._resolve_any_token(t)
            if not resolved:
                continue
            real_token, exchange_type = resolved
            if real_token in self._subscribed.get(exchange_type, set()):
                self._subscribed[exchange_type].discard(real_token)
                by_exchange.setdefault(exchange_type, []).append(real_token)
        if by_exchange and self._socket and self._connected:
            token_list = [{"exchangeType": et, "tokens": toks} for et, toks in by_exchange.items()]
            try:
                self._socket.unsubscribe("angelone_feed", 3, token_list)
            except Exception as exc:
                logger.debug("AngelOneFeeder: unsubscribe_tokens error: %s", exc)

    async def _index_subscribe_all(self) -> None:
        """Subscribe to every monitored index's fixed AngelOne token at connect time."""
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_ANGELONE_INDEX_TOKENS.keys())
        )
        by_exchange: Dict[int, List[str]] = {}
        for i in indices:
            pair = _ANGELONE_INDEX_TOKENS.get(i.upper())
            if pair:
                et, tok = pair
                self._subscribed.setdefault(et, set()).add(tok)
                by_exchange.setdefault(et, []).append(tok)
        if by_exchange and self._socket:
            token_list = [{"exchangeType": et, "tokens": toks} for et, toks in by_exchange.items()]
            try:
                self._socket.subscribe("angelone_feed", 3, token_list)
            except Exception as exc:
                logger.warning("AngelOneFeeder: index subscribe failed: %s", exc)

    async def _parse_frame(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            return
        if not hasattr(self, "_logged_first_tick"):
            self._logged_first_tick = True
            logger.info("AngelOneFeeder: first TICK frame keys=%s sample=%r",
                        list(raw.keys()), str(raw)[:400])
        exchange_type = raw.get("exchange_type")
        token = str(raw.get("token", ""))
        ltp_paise = raw.get("last_traded_price")
        if exchange_type is None or not token or ltp_paise is None:
            return
        ltp = float(ltp_paise) / 100.0

        idx_internal = _ANGELONE_TOKEN_TO_INTERNAL.get((exchange_type, token))
        if idx_internal:
            tick = IndexTick(
                symbol=idx_internal,
                ltp=ltp,
                open=float(raw.get("open_price_of_the_day", ltp_paise)) / 100.0,
                high=float(raw.get("high_price_of_the_day", ltp_paise)) / 100.0,
                low=float(raw.get("low_price_of_the_day", ltp_paise)) / 100.0,
                close=float(raw.get("closed_price", ltp_paise)) / 100.0,
                volume=int(raw.get("volume_trade_for_the_day", 0) or 0),
                timestamp=datetime.now(IST),
            )
            await self._publish_index(tick)
            return

        meta = self._token_meta.get(token)
        if meta:
            underlying, strike, opt_type, expiry = meta
            opt_tick = OptionTick(
                symbol=token,  # symbol field is informational only downstream; strategies key off underlying/strike/option_type/expiry
                underlying=underlying,
                strike=strike,
                option_type=opt_type,
                expiry=expiry,
                ltp=ltp,
                bid=ltp,
                ask=ltp,
                oi=int(raw.get("open_interest", 0) or 0),
                change_oi=0,
                volume=int(raw.get("volume_trade_for_the_day", 0) or 0),
                iv=0.0,
                delta=0.0,
                timestamp=datetime.now(IST),
                atp=float(raw.get("average_traded_price", ltp_paise)) / 100.0,
            )
            await self._publish_option(opt_tick)


# ─────────────────────────────────────────────────────────────────────────────
# DualFeeder — concurrent active-active dual-provider feed manager
# ─────────────────────────────────────────────────────────────────────────────

class DualFeeder:
    """
    Manages Upstox + Fyers feeders concurrently (active-active).

    Each provider runs in its own asyncio Task. A crash in one does NOT
    affect the other. Exponential back-off reconnect is per-provider.
    All ticks pass through DedupBuffer before being published so
    duplicate ticks from the trailing provider are silently discarded.
    Per-provider latency (ms) is tracked in _latency.
    """

    MAX_RECONNECT_DELAY = 60

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._running = False
        self._dedup = DedupBuffer()
        self._latency: Dict[str, float] = {}
        self._tasks: List[asyncio.Task] = []
        self._feeders: Dict[str, BaseFeeder] = {}
        self._staleness_boot_ts: float = 0.0
        self._staleness_alerted: set = set()

    _FEEDER_CLS: Dict[str, type] = {
        "upstox":   UpstoxFeeder,
        "upstox2":  UpstoxFeeder,
        "fyers":    FyersFeeder,
        "angelone": AngelOneFeeder,
    }

    async def start(self, upstox_creds: Dict[str, str], fyers_creds: Dict[str, str]) -> None:
        """Legacy convenience wrapper — maps positional args to the generic creds map."""
        creds_map: Dict[str, Dict[str, str]] = {}
        if upstox_creds and upstox_creds.get("access_token"):
            creds_map["upstox"] = upstox_creds
        if fyers_creds and fyers_creds.get("access_token"):
            creds_map["fyers"] = fyers_creds
        await self.start_providers(creds_map)

    async def start_providers(self, creds_map: Dict[str, Dict[str, str]]) -> None:
        """
        Start any number of provider streams from a {provider: creds} map.

        The configured primary drives prices; every other connected provider is a
        hot standby. If the primary goes stale, all standby ticks are accepted
        until the primary returns.
        """
        self._running = True

        # Active-PASSIVE: the primary provider drives all prices; standbys are
        # used only when the primary goes stale (down). Avoids two feeds
        # disagreeing on a contract (price flip-flop).
        _primary = (getattr(self._cfg, "primary_feeder_provider", "upstox") or "upstox").lower()
        if _primary not in self._FEEDER_CLS:
            _primary = "upstox"
        self._dedup.set_primary(_primary, float(getattr(self._cfg, "feeder_failover_stale_sec", 3.0)))
        logger.info("DualFeeder: active-passive — primary=%s (standbys used only when primary stale).", _primary)

        for provider, creds in creds_map.items():
            provider = provider.lower()
            cls = self._FEEDER_CLS.get(provider)
            if cls is None:
                logger.warning("DualFeeder: unknown provider '%s' — skipping.", provider)
                continue
            feeder = cls(self._bus, self._cfg)
            feeder.set_credentials(creds)
            feeder.set_provider_name(provider)
            if hasattr(feeder, "set_latency_tracker"):
                feeder.set_latency_tracker(provider, self._latency)
            feeder.set_dedup_buffer(self._dedup)
            try:
                ok = await feeder.connect()
            except Exception as exc:
                logger.warning("DualFeeder: %s connect raised: %s — continuing.", provider, exc)
                ok = False
            if ok:
                self._feeders[provider] = feeder
                task = asyncio.create_task(
                    self._run_stream(provider, feeder),
                    name=f"dual_feeder_{provider}",
                )
                self._tasks.append(task)
                logger.info("DualFeeder: %s stream task started.", provider)
            else:
                logger.warning("DualFeeder: %s failed to connect — stream not started.", provider)

        # 2026-08-23 fix: this is the actual production dual-broker path, and
        # it never had ANY staleness detection at all -- GlobalFeeder's own
        # heartbeat/_reconnect only exist on the single-provider code path
        # (_start_single_internal), never here. DualFeeder only reconnects a
        # provider when its stream cleanly disconnects or throws; a "connected
        # but silent" WebSocket (broker-side throttling, a subscription
        # quietly dropped, a stale session that doesn't error) was completely
        # invisible. Watches each monitored underlying's own last-accepted-tick
        # age (DedupBuffer.seconds_since_last_tick, which reflects whichever
        # provider is ACTUALLY delivering, not just the configured primary) --
        # not connection state, actual data flow.
        watchdog_task = asyncio.create_task(self._staleness_watchdog(), name="dual_feeder_staleness_watchdog")
        self._tasks.append(watchdog_task)

    _STALENESS_CHECK_INTERVAL_SEC = 15.0
    _STALENESS_THRESHOLD_SEC = 30.0
    _STALENESS_BOOT_GRACE_SEC = 45.0   # no symbol has ticked yet right after connect -- not stale, just starting up

    async def _staleness_watchdog(self) -> None:
        """Per-underlying: alerts (SysEvent.FEEDER_DOWN, symbol-scoped message)
        when a monitored index's own feed has produced no accepted tick from
        EITHER provider in _STALENESS_THRESHOLD_SEC, during market hours only
        (no ticks are legitimately expected outside 09:15-15:30, so silence
        there is not a fault). Publishes a matching FEEDER_RESTORED once a
        fresh tick arrives again, and never re-alerts for a symbol already
        flagged stale (avoids spamming the dashboard every 15s during a real
        outage)."""
        self._staleness_boot_ts = time.monotonic()
        self._staleness_alerted: set = set()
        while self._running:
            try:
                await asyncio.sleep(self._STALENESS_CHECK_INTERVAL_SEC)
            except asyncio.CancelledError:
                break
            await self._check_staleness_once()

    async def _check_staleness_once(self) -> None:
        """One staleness-check pass, split out of _staleness_watchdog's own
        sleep loop so it's directly unit-testable without needing to fake
        asyncio.sleep or wait through real 15s intervals."""
        if time.monotonic() - self._staleness_boot_ts < self._STALENESS_BOOT_GRACE_SEC:
            return
        now_t = datetime.now(IST).time()
        market_open = getattr(self._cfg.exchange, "market_open", None)
        market_close = getattr(self._cfg.exchange, "market_close", None)
        if market_open and market_close and not (market_open <= now_t <= market_close):
            return
        for underlying in list(getattr(self._cfg, "monitored_indices", []) or []):
            age = self._dedup.seconds_since_last_tick(underlying)
            is_stale = age is None or age > self._STALENESS_THRESHOLD_SEC
            if is_stale and underlying not in self._staleness_alerted:
                self._staleness_alerted.add(underlying)
                age_desc = f"{age:.0f}s" if age is not None else "no data yet"
                logger.critical(
                    "DualFeeder: %s feed STALE -- no accepted tick in %s from either provider.",
                    underlying, age_desc,
                )
                try:
                    await self._bus.publish(Topic.SYSTEM_EVENT, SystemEvent(
                        SysEvent.FEEDER_DOWN,
                        f"{underlying} feed stale — no tick in {age_desc} from either provider.",
                    ))
                except Exception:
                    pass
            elif not is_stale and underlying in self._staleness_alerted:
                self._staleness_alerted.discard(underlying)
                logger.info("DualFeeder: %s feed RESTORED (fresh tick received).", underlying)
                try:
                    await self._bus.publish(Topic.SYSTEM_EVENT, SystemEvent(
                        SysEvent.FEEDER_RESTORED, f"{underlying} feed restored.",
                    ))
                except Exception:
                    pass

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._tasks.clear()
        for provider, feeder in self._feeders.items():
            try:
                feeder.stop()
                await feeder.disconnect()
            except Exception as exc:
                logger.debug("DualFeeder: %s disconnect raised: %s", provider, exc)
        self._feeders.clear()
        logger.info("DualFeeder: stopped.")

    async def _run_stream(self, provider: str, feeder: BaseFeeder) -> None:
        # If the provider's own SDK handles reconnects, run it once and let the
        # SDK manage the lifecycle.  Fighting it with external reconnects creates
        # a spin of overlapping connections.
        if getattr(feeder, "_uses_sdk_reconnect", False):
            try:
                await feeder.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("DualFeeder: %s stream error: %s", provider, exc)
            return

        reconnect_attempts = 0
        while self._running:
            exit_reason: Optional[str] = None
            try:
                await feeder.run()
                exit_reason = "clean disconnect"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                exit_reason = f"stream error: {exc}"

            if not self._running:
                break

            delay = min(2 ** reconnect_attempts, self.MAX_RECONNECT_DELAY)
            delay = delay * (0.75 + 0.5 * random.random())  # jitter 0.75x–1.25x
            logger.warning(
                "DualFeeder: %s %s — reconnecting in %.0fs (attempt %d).",
                provider, exit_reason, delay, reconnect_attempts + 1,
            )
            await asyncio.sleep(delay)
            reconnect_attempts += 1
            try:
                await feeder.disconnect()
                ok = await feeder.connect()
            except Exception as reconnect_exc:
                logger.warning("DualFeeder: %s reconnect raised: %s", provider, reconnect_exc)
                ok = False
            if ok:
                reconnect_attempts = 0
                logger.info("DualFeeder: %s reconnected successfully.", provider)

    async def _wrap_publish_index(self, provider: str, feeder: BaseFeeder, tick: IndexTick) -> None:
        t0 = time.monotonic()
        if self._dedup.accept(tick.symbol, tick.ltp):
            await feeder._publish_index(tick)
            self._latency[provider] = (time.monotonic() - t0) * 1000.0

    @property
    def is_running(self) -> bool:
        return self._running and any(f.is_connected for f in self._feeders.values())

    @property
    def latency(self) -> Dict[str, float]:
        return dict(self._latency)

    @property
    def provider_connected(self) -> Dict[str, bool]:
        """Returns per-provider connected state based on active feeder tasks."""
        return {p: f.is_connected for p, f in self._feeders.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Feeder Registry — maps provider string → feeder class
# ─────────────────────────────────────────────────────────────────────────────

def _load_shared_client():
    """Lazy import so shared_feed_client.py doesn't create a circular dep at module load."""
    from data_layer.shared_feed_client import SharedFeedClient
    return SharedFeedClient


_FEEDER_REGISTRY: Dict[str, type] = {
    "mock":     MockFeeder,
    "upstox":   UpstoxFeeder,
    "upstox2":  UpstoxFeeder,
    "fyers":    FyersFeeder,
    "angelone": AngelOneFeeder,
    "shared":   None,   # populated on first access via register_feeder("shared", ...)
    # "shoonya": ShoonyaFeeder,
    # "dhan": DhanFeeder,
}


def register_feeder(provider: str, cls: type) -> None:
    """Called by broker feeder modules to self-register."""
    _FEEDER_REGISTRY[provider.lower()] = cls


# Auto-register SharedFeedClient for "shared" provider
try:
    from data_layer.shared_feed_client import SharedFeedClient as _SFC
    _FEEDER_REGISTRY["shared"] = _SFC
except ImportError:
    pass


# ─────────────────────────────────────────────────────────────────────────────
# GlobalFeeder — admin-managed wrapper with heartbeat + auto-reconnect
# ─────────────────────────────────────────────────────────────────────────────

class GlobalFeeder:
    """
    Lifecycle wrapper around one BaseFeeder instance.

    AdminConsole creates one GlobalFeeder, configures the provider,
    calls start() at 09:00 IST, and stop() at 15:30 IST.

    The heartbeat task checks connection health every 30 s and
    triggers a reconnect cycle if the feeder goes silent.
    """

    HEARTBEAT_INTERVAL = 30         # seconds
    MAX_RECONNECT_DELAY = 60        # seconds cap for exponential backoff

    def __init__(self, bus: EventBus, cfg: GlobalConfig, client_db=None) -> None:
        self._bus = bus
        self._cfg = cfg
        self._client_db = client_db   # Optional[ClientDB]; None in demo/paper mode
        self._feeder: Optional[BaseFeeder] = None
        self._feeder_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._tick_listener_task: Optional[asyncio.Task] = None
        self._candle_persist_task: Optional[asyncio.Task] = None
        self._last_tick_ts: float = 0.0
        self._reconnect_delay: float = 2.0
        self._running = False
        self._dual_feeder: Optional[DualFeeder] = None
        self._active_provider: str = "mock"
        self._cached_tokens: List[str] = []  # option tokens to re-apply on every reconnect
        self._extra_spot_keys: Dict[str, str] = {}   # inst_key → ticker for FnoStockMonitor
        self._rebalancer = None

    def _load_feeder_creds(self, provider: str) -> Dict[str, str]:
        """Load {api_key, api_secret, user_id, access_token} for a provider from DB.

        2026-09-06: AngelOne authenticates internally (client_code+password+
        TOTP -> jwtToken+feedToken inside AngelOneFeeder.connect()) and has
        no access_token/secret concept at all -- returns its own real
        credential shape instead of the OAuth-style one every other
        provider here uses."""
        creds: Dict[str, str] = {}
        if self._client_db is None or not provider:
            return creds
        try:
            row = self._client_db.get_feeder_creds_sync(provider) or {}
            if provider == "angelone":
                return {
                    "client_id": row.get("client_id", ""),
                    "api_key": row.get("api_key", ""),
                    "password": row.get("password", ""),
                    "totp_secret": row.get("totp_secret", ""),
                }
            creds = {
                "api_key": row.get("api_key", ""),
                "api_secret": row.get("secret", ""),
                "user_id": row.get("client_id", ""),
                "access_token": row.get("access_token", ""),
                "token_generated_at": row.get("token_generated_at", ""),
                "token_expiry_at": row.get("token_expiry_at", ""),
            }
        except Exception as exc:
            logger.warning("GlobalFeeder: could not load %s creds from DB: %s", provider, exc)
        return creds

    async def start(self) -> None:
        """
        Create feeder, connect, and launch run + heartbeat tasks.

        2026-09-06, direct user spec ("run all data feeder upstox1, fyers,
        angel parallel — if one fails other can immediately take its
        position, we will not ever lose any tick"): every one of
        upstox/fyers/angelone with usable, currently-saved credentials is
        started TOGETHER at boot, not just whichever two are configured as
        primary/secondary. The configured primary still drives prices
        (DedupBuffer.set_primary); every other connected provider is a hot
        standby. Falls back to a genuine single-provider feed only if just
        one of the three ends up with usable creds.

        Fixed alongside this (real bug, same class as the admin-toggle
        version already fixed in ui_layer.dashboard_server._start_feeder_
        stream): the old check (`secondary_creds.get("access_token")`)
        always evaluated False for AngelOne, which authenticates internally
        via client_code+password+TOTP and has no access_token field at all
        — AngelOne could never be picked up here even with valid creds
        saved. `_usable()` now accepts either credential shape.
        """
        self._running = True
        primary = self._cfg.primary_feeder_provider.lower()

        def _usable(p: str, creds: dict) -> bool:
            if not creds:
                return False
            if p == "angelone":
                return bool(creds.get("client_id") and creds.get("password"))
            return bool(creds.get("access_token"))

        candidates = {primary, "upstox", "fyers", "angelone"} - {"", "mock", "shared", "upstox2"}
        creds_map: Dict[str, Dict[str, str]] = {}
        for p in candidates:
            creds = self._load_feeder_creds(p)
            if _usable(p, creds):
                creds_map[p] = creds

        if len(creds_map) >= 2:
            await self.start_providers(creds_map)
            return
        if creds_map:
            p, c = next(iter(creds_map.items()))
            await self._start_single_internal(p, c)
            return

        # Nothing usable anywhere — single-provider fallback keeps the
        # original behavior (attempt the configured primary, even with
        # empty/stale creds, so existing reconnect/heartbeat logic still
        # engages and surfaces a clear FEEDER_DOWN rather than silently
        # doing nothing).
        primary_creds = self._load_feeder_creds(primary) if primary not in ("mock", "shared") else {}
        await self._start_single_internal(primary, primary_creds)

    async def _start_single_internal(self, provider: str, creds: Dict[str, str]) -> None:
        """Back-end for single-provider start (shared by start() and start_single())."""
        cls = _FEEDER_REGISTRY.get(provider)
        if cls is None:
            raise ValueError(f"GlobalFeeder: Unknown provider '{provider}'. Available: {list(_FEEDER_REGISTRY)}")

        try:
            self._feeder = cls(self._bus, self._cfg)
        except TypeError:
            self._feeder = cls(self._bus)

        if creds.get("access_token") and hasattr(self._feeder, "set_credentials"):
            self._feeder.set_credentials(creds)

        if not await self._feeder.connect():
            raise ConnectionError(f"GlobalFeeder: Failed to connect via '{provider}'.")

        self._last_tick_ts = time.monotonic()
        self._market_close_handled = False
        self._feeder_task = asyncio.create_task(self._run_feeder(), name="global_feeder_run")
        self._heartbeat_task = asyncio.create_task(self._heartbeat(), name="global_feeder_hb")
        self._tick_listener_task = asyncio.create_task(self._tick_listener(), name="global_feeder_tick_listener")
        self._market_close_task = asyncio.create_task(self._market_close_loop(), name="global_feeder_market_close")
        if self._client_db is not None:
            self._candle_persist_task = asyncio.create_task(
                self._candle_persist_loop(), name="candle_persist_1m"
            )

        self._active_provider = provider
        await self._bus.publish(Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_RESTORED, provider))
        logger.info("GlobalFeeder: Started with provider='%s'.", provider)

    async def stop(self) -> None:
        self._running = False
        if self._feeder:
            self._feeder.stop()
            await self._feeder.disconnect()
        for task in (self._feeder_task, self._heartbeat_task, self._tick_listener_task,
                     getattr(self, "_market_close_task", None)):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        if self._candle_persist_task and not self._candle_persist_task.done():
            self._candle_persist_task.cancel()
            try:
                await self._candle_persist_task
            except asyncio.CancelledError:
                pass
        logger.info("GlobalFeeder: Stopped.")

    def set_rebalancer(self, rebalancer) -> None:
        """Proxy StrikeRebalancer reference to the active underlying feeder."""
        if self._dual_feeder is not None:
            for f in self._dual_feeder._feeders.values():
                if hasattr(f, "set_rebalancer"):
                    f.set_rebalancer(rebalancer)
        elif self._feeder is not None:
            if hasattr(self._feeder, "set_rebalancer"):
                self._feeder.set_rebalancer(rebalancer)

    def subscribe_fno_equity(self, fyers_sym: str, underlying: str) -> None:
        """
        Register a Fyers equity symbol ('NSE:RELIANCE-EQ') to route EQUITY_TICK
        events for Trap Scanner FnO books. Delegates to FyersFeeder only (not Upstox).
        """
        if self._dual_feeder is not None:
            fyers_f = self._dual_feeder._feeders.get("fyers")
            if fyers_f and hasattr(fyers_f, "subscribe_fno_equity"):
                fyers_f.subscribe_fno_equity(fyers_sym, underlying)
        elif self._feeder is not None and hasattr(self._feeder, "subscribe_fno_equity"):
            self._feeder.subscribe_fno_equity(fyers_sym, underlying)

    def register_extra_spot_keys(self, mapping: Dict[str, str]) -> None:
        """Register NSE_EQ instrument keys → ticker names so FnoStockMonitor spot ticks flow as INDEX_TICK."""
        self._extra_spot_keys.update(mapping)
        self._reapply_extra_spot_keys()
        logger.info("GlobalFeeder: registered %d extra spot keys", len(mapping))

    def _reapply_extra_spot_keys(self) -> None:
        """Push _extra_spot_keys to the currently-active feeder(s). Called after OAuth reconnect."""
        if not self._extra_spot_keys:
            return
        if self._dual_feeder is not None:
            for f in self._dual_feeder._feeders.values():
                if hasattr(f, "register_extra_spot_keys"):
                    f.register_extra_spot_keys(self._extra_spot_keys)
        elif self._feeder is not None:
            if hasattr(self._feeder, "register_extra_spot_keys"):
                self._feeder.register_extra_spot_keys(self._extra_spot_keys)

    def _index_option_feeders(self):
        """Feeders that should carry index/option token subscriptions. When
        _FYERS_CARRIES_INDEX_OPTIONS is False (2026-08-04 provider split),
        Fyers is excluded -- it's dedicated to FnO equity spot only."""
        feeders = list(self._dual_feeder._feeders.values()) if self._dual_feeder is not None else []
        if not _FYERS_CARRIES_INDEX_OPTIONS:
            feeders = [f for f in feeders if getattr(f, "_provider_name", "") != "fyers"]
        return feeders

    async def subscribe_tokens(self, tokens: list) -> None:
        """Proxy to active feeder(s) — DualFeeder takes priority over initial feeder."""
        for t in tokens:
            if t not in self._cached_tokens:
                self._cached_tokens.append(t)
        if self._dual_feeder is not None:
            for feeder in self._index_option_feeders():
                await feeder.subscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.subscribe_tokens(tokens)

    async def resubscribe_tokens(self, tokens: list) -> None:
        """Force-resubscribe tokens even if already subscribed (heartbeat recovery)."""
        for t in tokens:
            if t not in self._cached_tokens:
                self._cached_tokens.append(t)
        if self._dual_feeder is not None:
            for feeder in self._index_option_feeders():
                if hasattr(feeder, "resubscribe_tokens"):
                    await feeder.resubscribe_tokens(tokens)
                else:
                    await feeder.subscribe_tokens(tokens)
        elif self._feeder is not None:
            if hasattr(self._feeder, "resubscribe_tokens"):
                await self._feeder.resubscribe_tokens(tokens)
            else:
                await self._feeder.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: list) -> None:
        """Proxy to active feeder(s) — DualFeeder takes priority over initial feeder."""
        for t in tokens:
            if t in self._cached_tokens:
                self._cached_tokens.remove(t)
        if self._dual_feeder is not None:
            for feeder in self._index_option_feeders():
                await feeder.unsubscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.unsubscribe_tokens(tokens)

    async def fetch_option_chain(self, underlying_key: str, expiry_date: date) -> Optional[Dict[str, Any]]:
        """Proxy to active feeder(s). Returns the first successful chain snapshot."""
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                if hasattr(feeder, "fetch_option_chain"):
                    data = await feeder.fetch_option_chain(underlying_key, expiry_date)
                    if data:
                        return data
        elif self._feeder is not None and hasattr(self._feeder, "fetch_option_chain"):
            return await self._feeder.fetch_option_chain(underlying_key, expiry_date)
        return None

    async def _reapply_cached_tokens(self) -> None:
        """Re-subscribe cached option tokens after a DualFeeder reconnect."""
        if not self._cached_tokens:
            return
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.subscribe_tokens(self._cached_tokens)
            logger.info("GlobalFeeder: re-applied %d cached option tokens after reconnect.",
                        len(self._cached_tokens))

    async def _stop_initial_feeder(self) -> None:
        """Stop and discard the initial (mock) feeder when switching to a real provider."""
        if self._feeder is not None:
            try:
                self._feeder.stop()
                await self._feeder.disconnect()
            except Exception as exc:
                logger.debug("GlobalFeeder: initial feeder stop raised: %s", exc)
            self._feeder = None
        for task in (self._feeder_task, self._heartbeat_task, self._tick_listener_task):
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        self._feeder_task = None
        self._heartbeat_task = None
        self._tick_listener_task = None
        logger.info("GlobalFeeder: initial feeder stopped — switching to live provider.")

    async def start_providers(self, creds_map: Dict[str, Dict[str, str]]) -> None:
        """
        Bootstrap a DualFeeder with an arbitrary set of providers.
        Stops MockFeeder and any prior DualFeeder.
        """
        if not creds_map:
            raise ValueError("GlobalFeeder: start_providers called with empty creds_map.")
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        await self._stop_initial_feeder()
        dual = DualFeeder(self._bus, self._cfg)
        await dual.start_providers(creds_map)
        self._dual_feeder = dual
        self._active_provider = "dual"
        self._market_close_handled = False
        if not getattr(self, "_market_close_task", None) or self._market_close_task.done():
            self._market_close_task = asyncio.create_task(
                self._market_close_loop(), name="global_feeder_market_close"
            )
        if self._client_db is not None and (
            not getattr(self, "_candle_persist_task", None) or self._candle_persist_task.done()
        ):
            self._candle_persist_task = asyncio.create_task(
                self._candle_persist_loop(), name="candle_persist_1m"
            )
        await self._bus.publish(
            Topic.SYSTEM_EVENT,
            SystemEvent(SysEvent.FEEDER_RESTORED, "dual_active_active"),
        )
        logger.info("GlobalFeeder: DualFeeder active-passive started with %s.", list(creds_map.keys()))
        self._reapply_extra_spot_keys()
        await self._reapply_cached_tokens()

    async def start_dual(self, upstox_creds: Dict[str, str], fyers_creds: Dict[str, str]) -> None:
        """Legacy convenience wrapper around start_providers for Upstox + Fyers."""
        creds_map: Dict[str, Dict[str, str]] = {}
        if upstox_creds and upstox_creds.get("access_token"):
            creds_map["upstox"] = upstox_creds
        if fyers_creds and fyers_creds.get("access_token"):
            creds_map["fyers"] = fyers_creds
        await self.start_providers(creds_map)

    async def start_single(self, provider: str, creds: Dict[str, str]) -> None:
        """Bootstrap single-provider DualFeeder. Stops MockFeeder and any prior DualFeeder."""
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        await self._stop_initial_feeder()
        dual = DualFeeder(self._bus, self._cfg)
        # 2026-09-06: AngelOne (and any future headless-only provider) has no
        # pre-existing access_token -- its own connect() does the full
        # client_code+password+TOTP auth internally. Gate on EITHER shape so
        # start_single("angelone", ...) doesn't silently start with an empty
        # creds_map (the old access_token-only check always failed for it).
        _has_usable_creds = bool(creds) and (
            creds.get("access_token") or (creds.get("client_id") and creds.get("password"))
        )
        await dual.start_providers({provider: creds} if _has_usable_creds else {})
        self._dual_feeder = dual
        self._active_provider = provider
        await self._bus.publish(
            Topic.SYSTEM_EVENT,
            SystemEvent(SysEvent.FEEDER_RESTORED, f"single_{provider}"),
        )
        logger.info("GlobalFeeder: single-provider '%s' feeder started.", provider)
        self._reapply_extra_spot_keys()
        await self._reapply_cached_tokens()

    @property
    def dual_latency(self) -> Dict[str, float]:
        """Per-provider latency dict from the DualFeeder, or {} if inactive."""
        return self._dual_feeder.latency if self._dual_feeder is not None else {}

    async def _run_feeder(self) -> None:
        while self._running:
            try:
                if self._feeder:
                    await self._feeder.run()
            except Exception as exc:
                logger.error("GlobalFeeder: Feeder crashed: %s. Reconnecting in %.0fs.", exc, self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self.MAX_RECONNECT_DELAY)
                await self._reconnect()
            else:
                break    # Clean exit

    async def _reconnect(self) -> None:
        if self._feeder:
            try:
                await self._feeder.disconnect()
            except Exception:
                pass
            if not await self._feeder.connect():
                logger.error("GlobalFeeder: Reconnect failed.")
                await self._bus.publish(
                    Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_DOWN, "reconnect_failed")
                )
            else:
                self._reconnect_delay = 2.0
                self._last_tick_ts = time.monotonic()
                # disconnect() set _running=False which exits _ws_loop → run() returns → task done.
                # Must restart the feeder run loop so ticks resume.
                if not self._feeder_task or self._feeder_task.done():
                    self._feeder_task = asyncio.create_task(
                        self._run_feeder(), name="global_feeder_run"
                    )
                logger.info("GlobalFeeder: Reconnected.")
                await self._bus.publish(
                    Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_RESTORED, "reconnect_ok")
                )

    async def _heartbeat(self) -> None:
        """
        Detect silent feed (no ticks for > HEARTBEAT_INTERVAL seconds)
        and trigger a reconnect.
        """
        while self._running:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            silence = time.monotonic() - self._last_tick_ts
            if silence > self.HEARTBEAT_INTERVAL * 2:
                logger.warning("GlobalFeeder: No ticks for %.0f seconds — triggering reconnect.", silence)
                await self._reconnect()

    async def _tick_listener(self) -> None:
        """
        Subscribes to INDEX_TICK and refreshes _last_tick_ts on every arrival.
        This is the only correct way to drive the heartbeat — avoids the need
        for any external caller to invoke record_tick().
        """
        q = self._bus.subscribe(Topic.INDEX_TICK)
        while self._running:
            try:
                await asyncio.wait_for(q.get(), timeout=1.0)
                self._last_tick_ts = time.monotonic()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @staticmethod
    def _is_nse_bse_token(token: str) -> bool:
        """True for NSE/BSE equity/F&O/index tokens (excludes MCX and Delta crypto)."""
        t = str(token).upper()
        return t.startswith("NSE_") or t.startswith("BSE_") or t.startswith("NSE|") or t.startswith("BSE|")

    async def _market_close_loop(self) -> None:
        """Daily at 15:40 IST unsubscribe all NSE/BSE option/index feeds to free WS slots.
        MCX/crypto evening sessions are left untouched."""
        from datetime import time as _dtime
        _close_time = _dtime(15, 40)
        while self._running:
            await asyncio.sleep(60.0)
            if self._market_close_handled:
                continue
            now = datetime.now(IST)
            if now.time() < _close_time:
                continue
            pinned: Dict[str, set] = {}
            if self._rebalancer is not None and hasattr(self._rebalancer, "pinned_strikes"):
                for und in getattr(self._cfg, "monitored_indices", []):
                    pinned[und] = set(self._rebalancer.pinned_strikes(und))

            tokens = []
            for t in self._cached_tokens:
                if not self._is_nse_bse_token(t):
                    continue
                meta = self._get_option_meta(t)
                if meta:
                    und, strike, _, _ = meta
                    if und in pinned and float(strike) in pinned[und]:
                        continue  # keep pinned open-position feeds alive
                tokens.append(t)
            if tokens:
                try:
                    await self.unsubscribe_tokens(tokens)
                    logger.info(
                        "GlobalFeeder: market-close unsubscribe at %s — dropped %d NSE/BSE tokens "
                        "(pinned strikes preserved).",
                        now.strftime("%H:%M:%S"), len(tokens),
                    )
                except Exception as exc:
                    logger.warning("GlobalFeeder: market-close unsubscribe failed: %s", exc)
            try:
                await self._bus.publish(Topic.SYSTEM_EVENT,
                                        SystemEvent(SysEvent.MARKET_CLOSE, "NSE/BSE feeds unsubscribed"))
            except Exception:
                pass
            self._market_close_handled = True

    @property
    def active_provider(self) -> str:
        """Currently active provider name: 'mock', 'upstox', 'fyers', or 'dual'."""
        return self._active_provider

    @property
    def is_running(self) -> bool:
        primary_ok = self._running and (self._feeder is not None and self._feeder.is_connected)
        dual_ok = self._dual_feeder is not None and self._dual_feeder.is_running
        return primary_ok or dual_ok

    async def _candle_persist_loop(self) -> None:
        """Persist every 1-minute CandleEvent to option_1m_bar_repository."""
        q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        try:
            while self._running:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(ev, CandleEvent):
                    continue
                if ev.timeframe != 1:
                    continue
                try:
                    await self._client_db.upsert_1m_bar(
                        symbol    = ev.symbol,
                        timestamp = ev.timestamp,
                        open_     = ev.open,
                        high      = ev.high,
                        low       = ev.low,
                        close     = ev.close,
                        volume    = float(ev.volume) if ev.volume else 0.0,
                    )
                except Exception as exc:
                    logger.warning("1m bar persist failed [%s]: %s", ev.symbol, exc)
        finally:
            try:
                self._bus._subs[Topic.CANDLE_CLOSE].remove(q)
            except (ValueError, KeyError, AttributeError):
                pass
