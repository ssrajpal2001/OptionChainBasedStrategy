"""
Test AngelOne full trade cycle against a real NSE F&O STOCK option (not an
index, not MCX) -- connect -> resolve contract -> place dummy order ->
get order ID -> cancel it.

This exercises the exact path OI-ORB Screener will use tomorrow:
  strategies/oi_orb_screener/stock_resolve.resolve_contract()
    -> data_layer/symbol_translator.SymbolTranslator.to_angelone()
    -> execution_bridge/broker_angel.AngelBroker.place_order()
       -> AngelBroker._lookup_symbol() (exact searchScrip match first,
          falls back to a regex scrip-master scan only if that misses --
          the fallback was only ever validated against MCX/CRUDEOIL, never
          an NSE stock, see CLAUDE.md 2026-08-24 notes)

Usage:
  python3 scripts/test_angel_stock_trade_cycle.py [client_id] [binding_id] [STOCK] [strike]
  python3 scripts/test_angel_stock_trade_cycle.py ssrajpal2001 SA5770 RELIANCE 4000

Pick a strike deliberately far OTM (well above/below the stock's current
price) so the LIMIT order below market never fills -- safe to place and
cancel repeatedly. Defaults to RELIANCE far-OTM CE if no symbol/strike given
-- ADJUST THE STRIKE to something plausible for whichever stock you pass,
this script does not fetch a live price to sanity-check it for you.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CLIENT_ID  = sys.argv[1] if len(sys.argv) > 1 else "ssrajpal2001"
BINDING_ID = sys.argv[2] if len(sys.argv) > 2 else None
STOCK      = (sys.argv[3] if len(sys.argv) > 3 else "RELIANCE").upper()
RAW_STRIKE = float(sys.argv[4]) if len(sys.argv) > 4 else 4000.0

OPTION_TYPE     = "CE"
DUMMY_LIMIT_PRICE = 0.50   # far below any real premium -> will NOT fill

SEP = "=" * 55


async def main():
    print(SEP)
    print("  AngelOne STOCK Option Trade Cycle Test")
    print(SEP)

    # ── 1. Load binding ──────────────────────────────────────
    from data_layer.client_db import ClientDB
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
    print(f"  Stock   : {STOCK}   raw_strike={RAW_STRIKE}   type={OPTION_TYPE}")
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

    # ── 3. Resolve the real contract via the SAME path OI-ORB uses ────────
    print(f"[ 2 ] Resolving {STOCK} {OPTION_TYPE} near strike {RAW_STRIKE} via stock_resolve...")
    from strategies.oi_orb_screener.stock_resolve import resolve_contract_async
    contract = await resolve_contract_async(STOCK, RAW_STRIKE, OPTION_TYPE)
    if contract is None:
        print(f"      FAILED — no contract resolved for {STOCK} @ {RAW_STRIKE}{OPTION_TYPE}.")
        print(f"      Check that {STOCK} has active F&O contracts and the strike is on-grid.")
        return
    angel_symbol = contract.broker_symbols.get("angelone", "")
    print(f"      Resolved contract : {contract.underlying} {contract.strike}{contract.option_type} exp={contract.expiry}")
    print(f"      AngelOne symbol   : {angel_symbol!r}")
    if not angel_symbol:
        print("      FAILED — SymbolTranslator.to_angelone() produced an empty symbol.")
        return
    print()

    # ── 4. Place dummy order ─────────────────────────────────
    from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType

    req = OrderRequest(
        broker_symbol = angel_symbol,
        exchange      = "NFO",
        side          = OrderSide.BUY,
        qty           = 1,   # AngelBroker/SmartAPI expects share qty, not lots, for this test — adjust if rejected
        order_type    = OrderType.LIMIT,
        price         = DUMMY_LIMIT_PRICE,
        tag           = "TEST_STOCK_CYCLE",
    )

    print(f"[ 3 ] Placing LIMIT BUY order: {angel_symbol} qty={req.qty} @ {DUMMY_LIMIT_PRICE} on NFO")
    print("      (this is where AngelBroker._lookup_symbol's real scrip-master match happens)")
    try:
        order_id = await broker.place_order(req)
        print(f"      Order ID returned : {order_id!r}")
        if not order_id:
            print("      ERROR: empty order ID — place_order failed silently")
            return
    except Exception as exc:
        print(f"      EXCEPTION: {exc}")
        print("      This is the exact failure mode to watch for -- symboltoken resolution")
        print("      against AngelOne's real NSE scrip master for this stock's option chain.")
        return
    print()

    # ── 5. Fetch order status ────────────────────────────────
    print(f"[ 4 ] Fetching order status for {order_id}...")
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

    # ── 6. Cancel the order ──────────────────────────────────
    print(f"[ 5 ] Cancelling order {order_id}...")
    try:
        cancelled = await broker.cancel_order(order_id)
        print(f"      Cancelled  : {cancelled}")
    except Exception as exc:
        print(f"      EXCEPTION cancelling: {exc}")
    print()

    # ── 7. Confirm cancelled ─────────────────────────────────
    print(f"[ 6 ] Confirming cancel status...")
    await asyncio.sleep(1)
    try:
        fill2 = await broker.get_order_status(order_id)
        print(f"      Final status : {fill2.status}")
    except Exception as exc:
        print(f"      EXCEPTION: {exc}")
    print()

    print(SEP)
    print("  STOCK trade cycle test COMPLETE")
    print(SEP)


asyncio.run(main())
