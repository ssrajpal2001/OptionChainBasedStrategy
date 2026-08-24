"""
Diagnose whether the AngelOne credentials stored for a client binding
actually work -- lighter and faster than test_angel_trade_cycle.py /
test_angel_stock_trade_cycle.py (no order is placed). Use this FIRST when
AngelOne login is failing (e.g. "Invalid API Key or App not found" on
AngelOne's own publisher-login page) to narrow down whether the problem is:
  - which credential fields are actually populated for this binding,
  - whether headless auth (client_code + password + TOTP) succeeds,
  - whether an already-stored access_token is still valid,
  - whether a genuinely authenticated session can call a real, harmless
    AngelOne endpoint (getProfile) -- the strongest possible confirmation
    that the api_key/app registration is correct, since a bad/inactive app
    would fail here even if generateSession() itself returned a token.

Usage:
  python3 scripts/check_angelone_credentials.py [client_id] [binding_id]
  python3 scripts/check_angelone_credentials.py ssrajpal2001 SA5770

Defaults to ssrajpal2001 and the first AngelOne binding found.
Never prints secrets in full -- api_key/secret/password/totp are masked.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CLIENT_ID  = sys.argv[1] if len(sys.argv) > 1 else "ssrajpal2001"
BINDING_ID = sys.argv[2] if len(sys.argv) > 2 else None

SEP = "=" * 55


def _mask(v: str, keep: int = 4) -> str:
    if not v:
        return "(empty)"
    v = str(v)
    if len(v) <= keep:
        return "*" * len(v)
    return v[:keep] + "*" * (len(v) - keep)


async def main():
    print(SEP)
    print("  AngelOne Credentials Check")
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
        print("       Check the binding_id spelling, or that provider='angelone' in the DB row.")
        return

    print(f"  Client  : {CLIENT_ID}")
    print(f"  Binding : {angel['binding_id']}")
    print()

    # ── 2. Report which credential fields are actually populated ─────────
    print("[ 1 ] Credential fields present on this binding:")
    print(f"      api_key      : {_mask(angel.get('api_key', ''))}")
    print(f"      api_secret   : {_mask(angel.get('api_secret', ''))}")
    print(f"      client_code  : {_mask(angel.get('client_code', ''))}")
    print(f"      user_id      : {_mask(angel.get('user_id', ''))}")
    print(f"      password     : {'SET' if angel.get('password') else '(empty)'}")
    print(f"      totp_secret  : {'SET' if angel.get('totp_secret') else '(empty)'}")
    print(f"      access_token : {'SET' if angel.get('access_token') else '(empty)'}")
    print()
    if not angel.get("api_key"):
        print("      WARNING: api_key is empty -- AngelOne login cannot work at all without it.")
        print()

    # ── 3. Build broker object and authenticate ──────────────
    from config.client_profiles import BrokerBinding
    from execution_bridge.broker_angel import AngelBroker

    binding_obj = BrokerBinding(**{k: angel.get(k) for k in BrokerBinding.__dataclass_fields__
                                   if k in angel})
    broker = AngelBroker(binding_obj, CLIENT_ID)

    auth_path = "OAuth access_token" if binding_obj.access_token else (
        "headless (client_code/user_id + password + TOTP)"
        if (binding_obj.client_code or binding_obj.user_id) and binding_obj.password
        else "NONE -- neither access_token nor headless creds are set"
    )
    print(f"[ 2 ] Authenticating with AngelOne via: {auth_path}")
    try:
        ok = await broker.authenticate()
    except Exception as exc:
        print(f"      EXCEPTION during authenticate(): {exc}")
        return
    if not ok:
        print("      FAILED — authenticate() returned False.")
        print("      Check the server log around this run for the specific SmartAPI error")
        print("      (AngelBroker logs 'Headless auth failed: <response>' on failure).")
        return
    print("      OK — SmartConnect session established locally.")
    print()

    # ── 4. Prove the session is REALLY valid against AngelOne's servers ──
    # A locally-successful generateSession() does not by itself prove the
    # api_key/app registration is correct on Angel's side -- calling a real
    # read-only endpoint is the strongest confirmation available.
    # NOTE: SmartConnect.getProfile(refreshToken) takes the REFRESH token,
    # not the access token (confirmed via SmartApi's own source) -- that's
    # only ever populated by the headless generateSession() path (Path 2 in
    # AngelBroker.authenticate()). The OAuth access_token path (Path 1) never
    # sets a refresh_token locally, so getProfile() can't be called that way
    # there -- step 5's rmsLimit() check (no refresh token needed) is the
    # real confirmation for that path instead.
    refresh_token = getattr(broker._smartapi, "refresh_token", None)
    if refresh_token:
        print("[ 3 ] Confirming the session is genuinely accepted by AngelOne (getProfile)...")
        try:
            profile = await asyncio.to_thread(broker._smartapi.getProfile, refresh_token)
            if profile and profile.get("status"):
                data = profile.get("data") or {}
                print(f"      OK — AngelOne accepted the session.")
                print(f"      Name        : {data.get('name', '?')}")
                print(f"      Client code : {data.get('clientcode', '?')}")
                print(f"      Email       : {_mask(data.get('email', ''), keep=3)}")
            else:
                print(f"      FAILED — getProfile() rejected: {profile}")
                print("      This means the api_key/app is not genuinely valid on AngelOne's side,")
                print("      even though generateSession() locally reported success.")
        except Exception as exc:
            print(f"      EXCEPTION calling getProfile(): {exc}")
            print("      If this says something like 'Invalid Token' or 'Invalid API key',")
            print("      the app registered on smartapi.angelone.in is the problem, not this app.")
    else:
        print("[ 3 ] Skipping getProfile() — no refresh_token available on this session")
        print("      (expected for the OAuth access_token path; only headless login gets one).")
        print("      Step 4 below (rmsLimit/funds) is the real confirmation for this path.")
    print()

    # ── 5. Funds check — a second, independent real-endpoint confirmation ─
    print("[ 4/5 ] Confirming funds endpoint (rmsLimit) also works...")
    try:
        funds = await broker.get_funds()
        print(f"      Available : {funds.get('available')}")
        print(f"      Used      : {funds.get('used')}")
    except Exception as exc:
        print(f"      EXCEPTION: {exc}")
    print()

    print(SEP)
    print("  Credentials check COMPLETE")
    print(SEP)


asyncio.run(main())
