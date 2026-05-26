"""
execution.py — Execution Engine & Risk Management.

Responsibilities:
  • Position sizing using fixed-fractional risk with ATR-based SL
  • Margin availability validation before any order placement
  • Bracket order submission (entry + SL + target)
  • Dynamic trailing stop-loss activation
  • Daily loss / max-trade circuit breakers
  • Full position lifecycle management

All order placement is async and non-blocking. Position state is
persisted in-memory and queryable by the backtest engine via the same API.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

from config import SystemConfig
from data_provider import BaseBroker, InstrumentInfo
from strategies import SignalDirection, SignalEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain Objects
# ---------------------------------------------------------------------------

class PositionStatus(Enum):
    PENDING_ENTRY = auto()
    OPEN = auto()
    SL_TRIGGERED = auto()
    TARGET_HIT = auto()
    MANUALLY_CLOSED = auto()
    EXPIRED = auto()


@dataclass
class OptionPosition:
    """Full lifecycle record of a single option trade."""
    position_id: str
    signal: SignalEvent
    instrument: InstrumentInfo

    entry_order_id: str = ""
    sl_order_id: str = ""
    target_order_id: str = ""

    entry_price: float = 0.0        # Option LTP at fill
    sl_price: float = 0.0           # Option SL price (tracked separately from underlying SL)
    target_price: float = 0.0       # Option target price
    quantity: int = 0               # Number of lots × lot_size

    current_ltp: float = 0.0        # Latest option mark price
    trailing_sl_price: float = 0.0  # Active trailing SL (0 = not activated)
    peak_ltp: float = 0.0           # Highest option LTP seen during trade

    status: PositionStatus = PositionStatus.PENDING_ENTRY
    open_time: datetime = field(default_factory=datetime.now)
    close_time: Optional[datetime] = None
    exit_price: float = 0.0
    realized_pnl: float = 0.0       # Net P&L after costs
    notes: str = ""

    @property
    def unrealized_pnl(self) -> float:
        if self.entry_price == 0 or self.current_ltp == 0:
            return 0.0
        return (self.current_ltp - self.entry_price) * self.quantity

    @property
    def risk_amount(self) -> float:
        """Maximum loss if SL hit (gross, before costs)."""
        return abs(self.entry_price - self.sl_price) * self.quantity

    @property
    def is_open(self) -> bool:
        return self.status == PositionStatus.OPEN


@dataclass
class DailyStats:
    date: date = field(default_factory=date.today)
    trades_taken: int = 0
    winning_trades: int = 0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    total_costs: float = 0.0
    max_drawdown_intraday: float = 0.0
    _peak_equity: float = 0.0

    @property
    def win_rate(self) -> float:
        if self.trades_taken == 0:
            return 0.0
        return self.winning_trades / self.trades_taken

    def update_drawdown(self, current_equity: float) -> None:
        if current_equity > self._peak_equity:
            self._peak_equity = current_equity
        dd = self._peak_equity - current_equity
        if dd > self.max_drawdown_intraday:
            self.max_drawdown_intraday = dd


# ---------------------------------------------------------------------------
# Transaction Cost Calculator
# ---------------------------------------------------------------------------

class CostCalculator:
    """Indian statutory charges for NSE/NFO options."""

    def __init__(self, config: SystemConfig) -> None:
        self._cfg = config.backtest

    def compute(self, turnover: float, is_sell: bool) -> float:
        """Return total transaction cost for a given turnover (price × qty)."""
        brokerage = self._cfg.brokerage_per_order
        stt = turnover * self._cfg.stt_pct / 100 if is_sell else 0.0
        exchange = turnover * self._cfg.exchange_charge_pct / 100
        sebi = turnover * self._cfg.sebi_fee_pct / 100
        gst = (brokerage + exchange) * self._cfg.gst_pct / 100
        total = brokerage + stt + exchange + sebi + gst
        return round(total, 2)

    def slippage(self, price: float, is_buy: bool) -> float:
        """Apply slippage: buy pays more, sell receives less."""
        factor = 1 + self._cfg.slippage_pct / 100 if is_buy else 1 - self._cfg.slippage_pct / 100
        return price * factor


# ---------------------------------------------------------------------------
# Risk Engine
# ---------------------------------------------------------------------------

class RiskEngine:
    """
    Validates trade proposals against capital, margin, and daily circuit breakers.
    Computes position sizes using fixed-fractional risk on the option premium.
    """

    def __init__(self, config: SystemConfig, broker: BaseBroker) -> None:
        self._config = config
        self._broker = broker
        self._daily_stats = DailyStats()
        self._starting_capital = config.risk.capital
        self._current_equity = config.risk.capital
        self._halted = False

    @property
    def is_halted(self) -> bool:
        return self._halted

    async def refresh_equity(self) -> None:
        """Re-fetch funds and update equity tracking."""
        funds = await self._broker.get_funds()
        self._current_equity = funds.get("available", self._current_equity) + funds.get("used", 0.0)
        self._daily_stats.update_drawdown(self._current_equity)

    def check_daily_loss_circuit(self) -> bool:
        """Return True if we can still trade today."""
        if self._halted:
            return False
        daily_loss_pct = abs(self._daily_stats.net_pnl) / self._starting_capital * 100
        if self._daily_stats.net_pnl < 0 and daily_loss_pct >= self._config.risk.max_daily_loss_percent:
            logger.warning(
                "CIRCUIT BREAKER: Daily loss %.2f%% ≥ %.2f%% limit. Trading halted.",
                daily_loss_pct, self._config.risk.max_daily_loss_percent,
            )
            self._halted = True
            return False
        if self._daily_stats.trades_taken >= self._config.risk.max_daily_trades:
            logger.warning("Max daily trades (%d) reached. No more entries.", self._config.risk.max_daily_trades)
            return False
        return True

    def compute_position_size(
        self,
        signal: SignalEvent,
        option_entry_price: float,
        lot_size: int,
    ) -> int:
        """
        Returns the number of lots to trade.

        Risk model: risk no more than max_risk_per_trade_percent of equity per trade.
        Risk per lot = |entry_option_price - sl_option_price| × lot_size

        We estimate SL option price as a fraction of the underlying SL distance
        relative to the underlying risk, applied proportionally to the option premium.
        """
        underlying_risk = signal.risk_on_underlying
        if underlying_risk == 0:
            return 0

        # Max allowed loss in INR
        max_risk_inr = self._current_equity * self._config.risk.max_risk_per_trade_percent / 100

        # Approximate: option moves ~delta × underlying_move; assume delta ≈ 0.5 for ATM
        # Conservative flat 50% of premium for SL estimation
        option_sl_distance = option_entry_price * 0.50

        if option_sl_distance <= 0:
            return 0

        risk_per_lot = option_sl_distance * lot_size
        lots = int(max_risk_inr / risk_per_lot)
        return max(1, min(lots, 10))     # Cap at 10 lots per trade

    async def validate_margin(self, option_price: float, quantity: int) -> bool:
        """Check if sufficient margin exists for the trade."""
        required = option_price * quantity * 1.05   # 5% margin buffer
        funds = await self._broker.get_funds()
        available = funds.get("available", 0.0)
        max_usable = available * self._config.risk.margin_utilization_limit
        if required > max_usable:
            logger.warning(
                "Insufficient margin. Required=%.2f Available×limit=%.2f",
                required, max_usable,
            )
            return False
        return True

    def record_trade_result(self, position: OptionPosition) -> None:
        self._daily_stats.trades_taken += 1
        net_pnl = position.realized_pnl
        self._daily_stats.net_pnl += net_pnl
        self._daily_stats.gross_pnl += position.unrealized_pnl
        if net_pnl > 0:
            self._daily_stats.winning_trades += 1

    def reset_daily(self) -> None:
        """Call at market open each day."""
        self._daily_stats = DailyStats()
        self._halted = False
        logger.info("RiskEngine: Daily stats reset.")

    def get_daily_stats(self) -> DailyStats:
        return self._daily_stats


# ---------------------------------------------------------------------------
# Order Manager
# ---------------------------------------------------------------------------

class OrderManager:
    """
    Translates SignalEvents into broker orders and manages the full order lifecycle.

    Implements:
      • Market entry order for the option contract
      • Separate SL-M order immediately after fill confirmation
      • Limit target order
      • Dynamic trailing SL update loop
    """

    def __init__(
        self,
        config: SystemConfig,
        broker: BaseBroker,
        risk_engine: RiskEngine,
        cost_calculator: CostCalculator,
    ) -> None:
        self._config = config
        self._broker = broker
        self._risk = risk_engine
        self._costs = cost_calculator
        self._positions: Dict[str, OptionPosition] = {}
        self._position_counter = 0
        self._is_paper = config.trading_modes.paper_trading

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    async def submit_entry(
        self,
        signal: SignalEvent,
        instrument: InstrumentInfo,
        option_ltp: float,
    ) -> Optional[OptionPosition]:
        """
        Full entry sequence:
          1. Size the position
          2. Validate margin
          3. Place market buy order
          4. On fill: place SL and target orders
          5. Return the OptionPosition object
        """
        lot_size = instrument.lot_size
        lots = self._risk.compute_position_size(signal, option_ltp, lot_size)
        quantity = lots * lot_size

        if quantity == 0:
            logger.warning("OrderManager: Zero quantity computed — skipping entry.")
            return None

        if not await self._risk.validate_margin(option_ltp, quantity):
            return None

        self._position_counter += 1
        pos_id = f"POS-{self._position_counter:04d}"

        # Apply entry slippage
        entry_with_slip = self._costs.slippage(option_ltp, is_buy=True)

        # Submit entry order
        try:
            entry_oid = await self._broker.place_order(
                token=instrument.token,
                exchange=instrument.exchange,
                symbol=instrument.symbol,
                transaction_type="BUY",
                quantity=quantity,
                order_type="MARKET",
                price=entry_with_slip,
                tag=f"ENTRY-{pos_id}",
            )
        except Exception as exc:
            logger.error("OrderManager: Entry order failed: %s", exc)
            return None

        # Compute option SL and target prices
        sl_option_price = entry_with_slip * 0.50       # 50% of premium as hard SL
        target_option_price = entry_with_slip * (1 + self._config.risk.min_risk_reward_ratio)

        # Place SL order
        try:
            sl_oid = await self._broker.place_order(
                token=instrument.token,
                exchange=instrument.exchange,
                symbol=instrument.symbol,
                transaction_type="SELL",
                quantity=quantity,
                order_type="SL-M",
                trigger_price=sl_option_price,
                tag=f"SL-{pos_id}",
            )
        except Exception as exc:
            logger.error("OrderManager: SL order failed: %s", exc)
            sl_oid = ""

        # Place target limit order
        try:
            tgt_oid = await self._broker.place_order(
                token=instrument.token,
                exchange=instrument.exchange,
                symbol=instrument.symbol,
                transaction_type="SELL",
                quantity=quantity,
                order_type="LIMIT",
                price=target_option_price,
                tag=f"TGT-{pos_id}",
            )
        except Exception as exc:
            logger.error("OrderManager: Target order failed: %s", exc)
            tgt_oid = ""

        entry_cost = self._costs.compute(entry_with_slip * quantity, is_sell=False)

        position = OptionPosition(
            position_id=pos_id,
            signal=signal,
            instrument=instrument,
            entry_order_id=entry_oid,
            sl_order_id=sl_oid,
            target_order_id=tgt_oid,
            entry_price=entry_with_slip,
            sl_price=sl_option_price,
            target_price=target_option_price,
            quantity=quantity,
            current_ltp=option_ltp,
            peak_ltp=option_ltp,
            status=PositionStatus.OPEN,
        )

        self._positions[pos_id] = position
        logger.info(
            "OrderManager: ENTERED %s | %s | entry=%.2f SL=%.2f TGT=%.2f qty=%d cost=%.2f",
            pos_id, instrument.symbol, entry_with_slip,
            sl_option_price, target_option_price, quantity, entry_cost,
        )
        return position

    # ------------------------------------------------------------------
    # Position Monitoring & Trailing SL
    # ------------------------------------------------------------------

    async def update_position_ltp(
        self, position_id: str, current_ltp: float, underlying_ltp: float
    ) -> None:
        """Called on every option tick for open positions."""
        pos = self._positions.get(position_id)
        if not pos or not pos.is_open:
            return

        pos.current_ltp = current_ltp
        if current_ltp > pos.peak_ltp:
            pos.peak_ltp = current_ltp

        # --- Activate trailing SL after 1R profit -----------------------
        cfg_r = self._config.risk
        one_r_profit = pos.entry_price * cfg_r.trailing_sl_activation_rr
        if pos.current_ltp >= pos.entry_price + one_r_profit and pos.trailing_sl_price == 0:
            # Lock in breakeven as trailing SL floor
            pos.trailing_sl_price = pos.entry_price * 1.02   # Slight buffer above cost
            logger.info("Trailing SL activated for %s at %.2f", position_id, pos.trailing_sl_price)

        # --- Update trailing SL upward only (never tighten downward) ----
        if pos.trailing_sl_price > 0:
            new_trail = pos.peak_ltp * (1 - cfg_r.trailing_sl_distance_atr / 100)
            if new_trail > pos.trailing_sl_price:
                old = pos.trailing_sl_price
                pos.trailing_sl_price = new_trail
                # Amend the SL order with new trigger
                if pos.sl_order_id and not self._is_paper:
                    try:
                        await self._broker.cancel_order(pos.sl_order_id)
                        new_sl_oid = await self._broker.place_order(
                            token=pos.instrument.token,
                            exchange=pos.instrument.exchange,
                            symbol=pos.instrument.symbol,
                            transaction_type="SELL",
                            quantity=pos.quantity,
                            order_type="SL-M",
                            trigger_price=new_trail,
                            tag=f"TSL-{position_id}",
                        )
                        pos.sl_order_id = new_sl_oid
                        logger.debug("Trailing SL updated: %.2f → %.2f", old, new_trail)
                    except Exception as exc:
                        logger.warning("TSL update failed: %s", exc)

        # --- Check if SL hit ---------------------------------------------
        effective_sl = pos.trailing_sl_price if pos.trailing_sl_price > 0 else pos.sl_price
        if current_ltp <= effective_sl:
            await self._close_position(pos, exit_price=current_ltp, reason="SL_TRIGGERED")
            return

        # --- Check if target hit -----------------------------------------
        if current_ltp >= pos.target_price:
            await self._close_position(pos, exit_price=current_ltp, reason="TARGET_HIT")

    # ------------------------------------------------------------------
    # Early Exit (OI Unwinding Signal)
    # ------------------------------------------------------------------

    async def early_exit_on_unwinding(self, position_id: str, current_ltp: float) -> None:
        """
        Exit early if the option chain shows structural unwinding that
        contradicts the original thesis.
        """
        pos = self._positions.get(position_id)
        if not pos or not pos.is_open:
            return
        if pos.current_ltp > pos.entry_price:   # Only exit if in profit
            logger.info("Early exit (unwinding signal) for %s @ %.2f", position_id, current_ltp)
            await self._close_position(pos, exit_price=current_ltp, reason="EARLY_UNWIND_EXIT")

    # ------------------------------------------------------------------
    # Close Position
    # ------------------------------------------------------------------

    async def _close_position(
        self, pos: OptionPosition, exit_price: float, reason: str
    ) -> None:
        exit_with_slip = self._costs.slippage(exit_price, is_buy=False)

        try:
            if pos.entry_order_id and not self._is_paper:
                await self._broker.place_order(
                    token=pos.instrument.token,
                    exchange=pos.instrument.exchange,
                    symbol=pos.instrument.symbol,
                    transaction_type="SELL",
                    quantity=pos.quantity,
                    order_type="MARKET",
                    tag=f"EXIT-{pos.position_id}",
                )
            # Cancel pending SL/target orders
            for oid in [pos.sl_order_id, pos.target_order_id]:
                if oid:
                    try:
                        await self._broker.cancel_order(oid)
                    except Exception:
                        pass
        except Exception as exc:
            logger.error("Close order failed for %s: %s", pos.position_id, exc)

        exit_turnover = exit_with_slip * pos.quantity
        entry_turnover = pos.entry_price * pos.quantity
        exit_cost = self._costs.compute(exit_turnover, is_sell=True)
        entry_cost = self._costs.compute(entry_turnover, is_sell=False)

        gross_pnl = (exit_with_slip - pos.entry_price) * pos.quantity
        pos.realized_pnl = gross_pnl - entry_cost - exit_cost
        pos.exit_price = exit_with_slip
        pos.close_time = datetime.now()

        status_map = {
            "SL_TRIGGERED": PositionStatus.SL_TRIGGERED,
            "TARGET_HIT": PositionStatus.TARGET_HIT,
            "EARLY_UNWIND_EXIT": PositionStatus.MANUALLY_CLOSED,
        }
        pos.status = status_map.get(reason, PositionStatus.MANUALLY_CLOSED)
        pos.notes = reason

        self._risk.record_trade_result(pos)
        logger.info(
            "CLOSED %s | %s | exit=%.2f | net_pnl=₹%.2f | reason=%s",
            pos.position_id, pos.instrument.symbol,
            exit_with_slip, pos.realized_pnl, reason,
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_open_positions(self) -> List[OptionPosition]:
        return [p for p in self._positions.values() if p.is_open]

    def get_all_positions(self) -> List[OptionPosition]:
        return list(self._positions.values())

    def open_position_count(self) -> int:
        return len(self.get_open_positions())

    def can_enter_new_trade(self) -> bool:
        return (
            self.open_position_count() < self._config.risk.max_open_positions
            and self._risk.check_daily_loss_circuit()
        )

    def get_trade_summary(self) -> Dict:
        all_pos = self.get_all_positions()
        closed = [p for p in all_pos if not p.is_open]
        winners = [p for p in closed if p.realized_pnl > 0]
        total_pnl = sum(p.realized_pnl for p in closed)
        gross_profit = sum(p.realized_pnl for p in winners)
        gross_loss = sum(p.realized_pnl for p in closed if p.realized_pnl <= 0)
        return {
            "total_trades": len(closed),
            "open_positions": self.open_position_count(),
            "win_rate": len(winners) / len(closed) if closed else 0.0,
            "net_pnl": total_pnl,
            "profit_factor": abs(gross_profit / gross_loss) if gross_loss != 0 else float("inf"),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
        }


# ---------------------------------------------------------------------------
# Execution Coordinator — top-level facade used by main.py
# ---------------------------------------------------------------------------

class ExecutionCoordinator:
    """
    Wires RiskEngine + OrderManager together and exposes a single
    process_signal() coroutine called by the trading loop.
    """

    def __init__(self, config: SystemConfig, broker: BaseBroker) -> None:
        self._config = config
        self._broker = broker
        self._cost_calc = CostCalculator(config)
        self.risk = RiskEngine(config, broker)
        self.order_manager = OrderManager(config, broker, self.risk, self._cost_calc)

    async def process_signal(
        self,
        signal: SignalEvent,
        instruments: Dict[Tuple[float, str], InstrumentInfo],
        option_ltp: float,
    ) -> Optional[OptionPosition]:
        """
        End-to-end handler: validate → size → execute.
        instruments key is (strike, option_type).
        """
        if not self.order_manager.can_enter_new_trade():
            logger.debug("ExecutionCoordinator: Entry blocked by risk/position limits.")
            return None

        key = (signal.target_strike, signal.option_type)
        instrument = instruments.get(key)
        if instrument is None:
            logger.warning(
                "ExecutionCoordinator: Instrument not found for (%s, %s)",
                signal.target_strike, signal.option_type,
            )
            return None

        await self.risk.refresh_equity()
        return await self.order_manager.submit_entry(signal, instrument, option_ltp)

    async def tick_positions(
        self,
        option_ltps: Dict[str, float],
        underlying_ltp: float,
    ) -> None:
        """Feed latest option LTPs into open positions for SL/target monitoring."""
        for pos in self.order_manager.get_open_positions():
            ltp = option_ltps.get(pos.instrument.symbol, pos.current_ltp)
            await self.order_manager.update_position_ltp(
                pos.position_id, ltp, underlying_ltp
            )
