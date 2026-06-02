"""Pure seller-trap detection state machine (Below -> Above -> Return).

Side-effect free: no logging, no I/O. Reference candles are tracked in a LIFO
stack; the most-recently added level is "active". Each level models sellers who
shorted on a break below the candle low (SL at the candle high). If price then
breaks above the high, those sellers are trapped; when price returns down to the
low, an entry is ready.
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class State(Enum):
    WATCH = auto()
    SELLERS_IN = auto()
    TRAPPED = auto()
    ENTRY_READY = auto()


@dataclass
class Level:
    entry_l: float
    sl_h: float
    trapped: bool = False


class SellerTrapDetector:
    def __init__(self) -> None:
        self._levels: List[Level] = []
        self.state: State = State.WATCH
        self.entry_ready: bool = False

    @property
    def active_level(self) -> Optional[Level]:
        return self._levels[-1] if self._levels else None

    def on_candle(self, c: dict) -> None:
        self._levels.append(Level(entry_l=c["low"], sl_h=c["high"]))

    def on_tick(self, price: float) -> None:
        lvl = self.active_level
        if lvl is None:
            return

        # Below: sellers entered.
        if self.state in (State.WATCH,) and price < lvl.entry_l:
            self.state = State.SELLERS_IN

        # Above: sellers trapped.
        if self.state == State.SELLERS_IN and price > lvl.sl_h:
            lvl.trapped = True
            self.state = State.TRAPPED

        # Return: entry ready.
        if self.state == State.TRAPPED and price <= lvl.entry_l:
            self.state = State.ENTRY_READY
            self.entry_ready = True

    def consume_entry(self) -> None:
        self.entry_ready = False

    def invalidate_active(self) -> None:
        if self._levels:
            self._levels.pop()
        self.entry_ready = False
        self.state = State.SELLERS_IN if self._levels else State.WATCH
