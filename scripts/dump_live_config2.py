"""Run ON THE SERVER: dumps the RAW admin sell_straddle section for NIFTY
(entry_rules_beginning/reentry, vwap_source, pool_itm_depth/pool_otm_depth,
etc. -- fields load_sell_straddle_config's own SellStraddleConfig dataclass
doesn't carry, since entries.py reads them directly from this raw section).

Usage: python scripts/dump_live_config2.py NIFTY
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.runtime_config import RuntimeConfig


def main() -> None:
    underlying = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    ss = RuntimeConfig.index_section(underlying, "sell_straddle")
    print(json.dumps(ss, indent=2, default=str))


if __name__ == "__main__":
    main()
