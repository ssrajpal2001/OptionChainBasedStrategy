"""
Test AngelOne full trade cycle: connect → place dummy order → get order ID → close it.

Usage:
  python3 scripts/test_angel_trade_cycle.py [client_id] [binding_id]
  python3 scripts/test_angel_trade_cycle.py ssrajpal2001 SA5770

Defaults to ssrajpal2001 and first AngelOne binding found.
Uses NIFTY spot (NSE) as the dummy instrument — small qty=1, LIMIT price far OTM
so the order stays open (won't fill) and can be cancelled safely.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CLIENT_ID  = sys.argv[1] if len(sys.argv) > 1 else "ssrajpal2001"
BINDING_ID = sys.argv[2] if len(sys.argv) > 2 else None

# Dummy order: buy 1 lot MCX CrudeOil CE option (far OTM) at Rs 0.50 limit — won't fill,
# tiny premium, no margin needed. Change strike if needed.
DUMMY_EXCHANGE    = "MCX"
DUMMY_SYMBOL      = "CRUDEOIL"       # prefix — script will search for CE option
DUMMY_OPTION_TYPE = "CE"             # buy a call option (not futures)
DUMMY_STRIKE      = 8000             # far OTM CE strike (crude ~7000-7100 range)
DUMMY_QTY         = 100              # CrudeOil lot size = 100
DUMMY_LIMIT_PRICE = 0.50             # far below market option price → will NOT fill

SEP = "=" * 55

async def main():
    print(SEP)
    print("  AngelOne Trade Cycle Test")
    print(SEP)

    # ── 1. Load binding ──────────────────────────────────────
    from data_layer.client_db import ClientDB
    import json
    db = ClientDB()
    bindings = await asyncio.to_thread(db.get_bindings_sync, CLIENT_ID)
    angel = None
    for b in bindings:
        provider = (b.get("provider") or "").lower()
        bid = b.get("binding_id", "")
        if "angel" in provider:
            if BINDING_ID is None or bid == BINDING_ID:
                angel = b
                break

    if not angel:
        print(f"ERROR: No AngelOne binding found for client={CLIENT_ID} binding={BINDING_ID}")
        return

    print(f"  Client  : {CLIENT_ID}")
    print(f"  Binding : {angel['binding_id']}")
    print(f"  Token   : {'SET' if angel.get('access_token') else 'MISSING'}")
    print()

    # ── 2. Build broker object and authenticate ──────────────
    from config.client_profiles import BrokerBinding
    from execution_bridge.broker_angel import AngelBroker

    binding_obj = BrokerBinding(**{k: angel.get(k) for k in BrokerBinding.__dataclass_fields__
                                   if k in angel})
    broker = AngelBroker(binding_obj, CLIENT_ID)

    print("[ 1 ] Authenticating with AngelOne...")
    ok = await broker.authenticate()
    if not ok:
        print("      FAILED — check token/credentials")
        return
    print("      OK — SmartAPI connected")
    print()

    # ── 3. Place dummy order ─────────────────────────────────
    from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType

    # Resolve MCX CrudeOil option symbol via AngelOne scrip search
    from data_layer.instrument_registry import REGISTRY
    from datetime import date
    await asyncio.to_thread(REGISTRY.load_sync, "CRUDEOIL", "")
    exp = REGISTRY.get_active_expiry("CRUDEOIL", date.today())
    angel_symbol = None
    if exp:
        raw = await asyncio.to_thread(
            broker._smartapi.searchScrip, DUMMY_EXCHANGE,
            f"CRUDEOIL{exp.strftime('%d%b%y').upper()}{DUMMY_STRIKE}{DUMMY_OPTION_TYPE}"
        )
        hits = (raw or {}).get("data") or []
        if hits:
            angel_symbol = hits[0]["tradingsymbol"]
            sym_token    = hits[0]["symboltoken"]
            # Pre-populate broker cache so place_order skips the duplicate searchScrip call
            broker._tok_cache[(DUMMY_EXCHANGE, angel_symbol)] = (sym_token, angel_symbol)
            print(f"      Resolved: {angel_symbol}  token={sym_token}")
    if not angel_symbol:
        # fallback: let broker resolve via its own lookup
        angel_symbol = f"CRUDEOIL{DUMMY_STRIKE}{DUMMY_OPTION_TYPE}"
        print(f"      Using fallback symbol: {angel_symbol}")

    req = OrderRequest(
        broker_symbol = angel_symbol,
        exchange      = DUMMY_EXCHANGE,
        side          = OrderSide.BUY,
        qty           = DUMMY_QTY,
        order_type    = OrderType.LIMIT,
        price         = DUMMY_LIMIT_PRICE,
        tag           = "TEST_CYCLE",
    )

    print(f"[ 2 ] Placing LIMIT BUY order: {angel_symbol} qty={DUMMY_QTY} @ {DUMMY_LIMIT_PRICE}")
    try:
        order_id = await broker.place_order(req)
        print(f"      Order ID returned : {order_id!r}")
        if not order_id:
            print("      ERROR: empty order ID — place_order failed silently")
            return
    except Exception as exc:
        print(f"      EXCEPTION: {exc}")
        return
    print()

    # ── 4. Fetch order status ────────────────────────────────
    print(f"[ 3 ] Fetching order status for {order_id}...")
    await asyncio.sleep(1)
    try:
        fill = await broker.get_order_status(order_id)
        print(f"      Status     : {fill.status}")
        print(f"      Avg price  : {fill.avg_price}")
        print(f"      Qty        : {fill.qty}")
        print(f"      Symbol     : {fill.broker_symbol}")
    except Exception as exc:
        print(f"      EXCEPTION fetching status: {exc}")
    print()

    # ── 5. Cancel the order ──────────────────────────────────
    print(f"[ 4 ] Cancelling order {order_id}...")
    try:
        cancelled = await broker.cancel_order(order_id)
        print(f"      Cancelled  : {cancelled}")
    except Exception as exc:
        print(f"      EXCEPTION cancelling: {exc}")
    print()

    # ── 6. Confirm cancelled ─────────────────────────────────
    print(f"[ 5 ] Confirming cancel status...")
    await asyncio.sleep(1)
    try:
        fill2 = await broker.get_order_status(order_id)
        print(f"      Final status : {fill2.status}")
    except Exception as exc:
        print(f"      EXCEPTION: {exc}")
    print()

    print(SEP)
    print("  Trade cycle test COMPLETE")
    print(SEP)

asyncio.run(main())
