"""Run ON THE SERVER: dumps the EXACT effective SellStraddle config SA5770's
NIFTY book is actually running -- same function (load_sell_straddle_config)
the live engine itself calls, so this is the real merged admin-defaults +
client-overrides config, not an approximation.

Usage: python scripts/dump_live_config.py <client_id> <binding_id_or_underlying>
e.g.:  python scripts/dump_live_config.py ssrajpal2001 NIFTY
"""
import dataclasses
import json
import sys

from config.global_config import GlobalConfig
from strategies.sell_straddle.config import load_sell_straddle_config
from data_layer.client_db import ClientDB


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    client_id, underlying = sys.argv[1], sys.argv[2]

    cfg = load_sell_straddle_config(underlying, GlobalConfig(), client_id=client_id)
    d = dataclasses.asdict(cfg)
    # datetime.time objects aren't JSON-serializable by default.
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
    print(json.dumps(d, indent=2, default=str))

    # Also dump the deployment's own strategy_params (vp_oi_enabled,
    # lot_multiplier, strategy_name variant e.g. sell_straddle_calc_vwap).
    print("\n--- deployment strategy_params ---")
    db = ClientDB()
    bindings = db.get_bindings_safe_sync(client_id)
    import sqlite3
    con = sqlite3.connect(db._db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT deploy_id, binding_id, strategy_name, is_running, strategy_params "
        "FROM strategy_deployments WHERE client_id=?", (client_id,)
    ).fetchall()
    for r in rows:
        print(json.dumps(dict(r), indent=2))


if __name__ == "__main__":
    main()
