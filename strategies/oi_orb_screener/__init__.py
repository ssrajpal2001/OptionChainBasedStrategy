"""strategies/oi_orb_screener — OI-Spurt + Price Momentum + ORB screener (option buyer).

Ported from the standalone Colab script (colab/oi_orb_screener/screener_nse_direct.py,
confirmed working live 2026-08-24) into the live EC2 app as a connectivity/plumbing
proof: fired signals place a real order (paper_route mode) and subscribe to the
resulting option's live LTP. Explicitly NO SL/target/risk-cap logic this pass -- EOD
square-off only -- per direct user instruction 2026-08-24; that comes in a follow-up
pass before any real live capital is put behind this.

Fully standalone: its own events, its own execution bridge, its own book manager, its
own Topics -- shares no runtime infrastructure with SellStraddle / D1 Trap / FVG /
OI-Flow / Liquidity Sweep / Liquidity Trap, same mandate as every other strategy added
since OI-Flow. See C:\\Users\\SERVER\\.claude\\plans\\curried-snuggling-sunrise.md for
the full design plan and rationale.

First strategy in this codebase's live pipeline to trade individual F&O STOCKS
(dynamically chosen each day by the screener) rather than a fixed NIFTY/SENSEX/
BANKNIFTY underlying -- see strategies/oi_orb_screener/stock_resolve.py for how
strike/lot/expiry are resolved for a stock not known in advance.
"""
from strategies.oi_orb_screener.book_manager import (
    OiOrbScreenerBookManager,
    OiOrbScreenerTop20BookManager,
)
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy

__all__ = ["OiOrbScreenerStrategy", "OiOrbScreenerBookManager", "OiOrbScreenerTop20BookManager"]
