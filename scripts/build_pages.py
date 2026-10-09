"""Build script: splits monitor.html into separate admin/client per-tab
pages (ui_layer/templates/pages/*.html), served by dashboard_server.py's
/admin/{tab} and /client/{tab} routes -- so a visitor only downloads
their own role's markup instead of the full ~681KB monolithic file.

RUN THIS AGAIN AFTER EVERY monitor.html EDIT. The split pages are
generated artifacts, not hand-maintained -- editing them directly will
be silently lost the next time this script runs. Run from the repo
root: `python scripts/build_pages.py`.

All line-number boundaries below are hardcoded against monitor.html's
CURRENT structure (see _OFF below for the one-time-already-applied
offset history) -- if monitor.html's line count changes anywhere BEFORE
these boundaries (i.e. almost any edit to the login screen, navbar, or
shared JS component), every boundary after that point shifts and this
script's assertions will catch structural mismatches but NOT silently
wrong output -- re-verify boundaries (grep for the x-show="screen==='
admin'"/"client'" markers etc., same technique used to build this the
first time) if assertions fail or output looks wrong after a big edit.

Verified structure (via stack-based div-depth tracing, not the original
file's own HTML comments, several of which are stale/mislabeled):
  <body>
    <div class="z-10 flex-col">          [3228]  OUTERMOST, closes @10186
      <div x-show="screen!=='login'">    [3388]  closes @10185
        <header> navbar+drawer </header> [3391-3581]
        admin subnav + <main admin>      [3583-6415] self-contained, balanced
        <main x-show="screen==='client'">[6421]  closes @10176 (</main>)
          <div x-show="clientPhase==='active'"> [6664]  closes @10174
            topbar + subnav              [6667-6706]
            5 sibling tabs               [6709-10173]
        <footer>                         [10179-10183]
      [broker-add modal -- client only, sibling of the z-10 div]  [10189-10272]

  CONFIRMED PRE-EXISTING BUG (not introduced by this split): within the
  "positions" tab (7894-9983), two divs never close inside that range --
  div(7894) itself (the tab wrapper) and div(7931) (a nested "broker
  tabs" wrapper). In the original monolithic file this is silently
  absorbed by the browser's forgiving parser; in split files it must be
  patched explicitly so client_positions.html is well-formed on its own.
  The orphaned closing tag this produces downstream (original line 10174,
  commented "end active") is EXCLUDED from history's extracted range
  since its true matching open no longer exists in that file.
"""
import os
import re

SRC = "ui_layer/templates/monitor.html"
OUT_DIR = "ui_layer/templates/pages"

lines = open(SRC, encoding="utf-8").read().split("\n")

def seg(a, b):
    return "\n".join(lines[a - 1:b])

_OFF = -12  # 2026-10-09: monitor.html grew by 5 lines (login/session-restore
            # redirect edits), then 4 more (per-tab gating for loadTelemetry/
            # loadIvMatrix/loadAdminEvents), then 3 more (gating
            # checkPendingManualEntries to the positions tab), then shrank by
            # 24 (CDN Tailwind script + inline config replaced with one
            # <link> to the static build) after boundaries were originally
            # mapped; every absolute line number below is shifted by this
            # fixed, cumulative offset.

HEAD_LOGIN_NAV = seg(1, 3582 + _OFF)       # head + shared JS + login + top navbar + mobile drawer
ADMIN_SHELL    = seg(3583 + _OFF, 3646 + _OFF)    # admin subnav + <main admin> open + banner alerts
CLIENT_SHELL   = seg(6417 + _OFF, 6708 + _OFF)    # <main client> open + alerts + onboarding/pending + topbar + subnav
FOOTER_ONLY    = seg(10179 + _OFF, 10183 + _OFF)  # just <footer>...</footer>, no surrounding div-closes
BROKER_MODAL   = seg(10189 + _OFF, 10272 + _OFF)  # client-only broker-add modal
FINAL_CLOSE    = seg(10273 + _OFF, 10277 + _OFF)  # blank lines + </body></html>

ADMIN_TABS = {
    "dashboard":  (3647 + _OFF, 4096 + _OFF, "adminTab='dashboard'"),
    "strategies": (4097 + _OFF, 5350 + _OFF, "adminTab='strategies'; loadStrategyConfig(); loadStrategyRegistry(); loadIndexConfig(adminSelectedIndex)"),
    "feeder":     (5351 + _OFF, 5782 + _OFF, "adminTab='feeder'"),
    "trap":       (5783 + _OFF, 5930 + _OFF, "adminTab='trap'"),
    "clients":    (5931 + _OFF, 6414 + _OFF, "adminTab='clients'; loadClientProfiles(); loadRiskSummary()"),
}
CLIENT_TABS = {
    "dashboard":  (6709 + _OFF, 6785 + _OFF, "clientMenu='dashboard'; loadClientPositions()"),
    "strategies": (6786 + _OFF, 6920 + _OFF, "clientMenu='strategies'"),
    "brokers":    (6921 + _OFF, 7893 + _OFF, "clientMenu='brokers'"),
    "positions":  (7894 + _OFF, 9983 + _OFF, "clientMenu='positions'; loadClientPositions()"),
    "history":    (9984 + _OFF, 10173 + _OFF, "clientMenu='history'; loadClientHistory()"),  # excludes orphaned line (orig 10174)
}

# ── Convert tab-click handlers (in the SHARED fragments) to real navigation ──
def convert_nav(html: str) -> str:
    html = html.replace(
        '''@click="clientMenu=m.id; mobileNav=false;
                        m.id==='positions'&&loadClientPositions();
                        m.id==='history'&&loadClientHistory();
                        m.id==='dashboard'&&loadClientPositions()">''',
        '''@click="mobileNav=false; window.location='/client/'+m.id">''',
    )
    html = html.replace(
        '@click="adminTab=m.id; mobileNav=false">',
        '''@click="mobileNav=false; window.location='/admin/'+m.id">''',
    )
    return html

HEAD_LOGIN_NAV = convert_nav(HEAD_LOGIN_NAV)

_ADMIN_DESKTOP_SUBS = [
    ('''@click="adminTab='dashboard'">''', '''@click="window.location='/admin/dashboard'">'''),
    ('''@click="adminTab='strategies'; loadStrategyConfig(); loadStrategyRegistry(); loadIndexConfig(adminSelectedIndex)">''',
     '''@click="window.location='/admin/strategies'">'''),
    ('''@click="adminTab='feeder'">''', '''@click="window.location='/admin/feeder'">'''),
    ('''@click="adminTab='trap'">''', '''@click="window.location='/admin/trap'">'''),
    ('''@click="adminTab='clients'; loadClientProfiles(); loadRiskSummary()">''',
     '''@click="window.location='/admin/clients'">'''),
]
for old, new in _ADMIN_DESKTOP_SUBS:
    assert old in ADMIN_SHELL, f"admin subnav pattern not found: {old!r}"
    ADMIN_SHELL = ADMIN_SHELL.replace(old, new)

_CLIENT_DESKTOP_OLD = '''@click="clientMenu=m.id; m.id==='positions'&&loadClientPositions(); m.id==='history'&&loadClientHistory()"'''
_CLIENT_DESKTOP_NEW = '''@click="window.location='/client/'+m.id"'''
assert _CLIENT_DESKTOP_OLD in CLIENT_SHELL, "client subnav pattern not found"
CLIENT_SHELL = CLIENT_SHELL.replace(_CLIENT_DESKTOP_OLD, _CLIENT_DESKTOP_NEW)

os.makedirs(OUT_DIR, exist_ok=True)

# Shrink output size (faster download, smaller disk footprint) without
# touching <script> contents -- regex-editing live JS is not worth the risk
# on a trading dashboard for a cosmetic byte-count win. Only strips HTML
# comments and blank/whitespace-only lines outside <script> blocks.
_SCRIPT_SPLIT = re.compile(r"(<script\b[^>]*>.*?</script>)", re.S | re.I)
_HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)

def _shrink(html: str) -> str:
    chunks = _SCRIPT_SPLIT.split(html)
    out = []
    for i, chunk in enumerate(chunks):
        if i % 2 == 1:  # a <script>...</script> block -- leave byte-for-byte
            out.append(chunk)
            continue
        chunk = _HTML_COMMENT.sub("", chunk)
        lines = [ln for ln in chunk.split("\n") if ln.strip()]
        out.append("\n".join(lines))
    return "\n".join(out)

def write_admin_page(fname: str, body_init: str, tab_html: str):
    head = HEAD_LOGIN_NAV.replace(
        '<body x-data="terminal" x-cloak class="relative">',
        f'<body x-data="terminal" x-cloak x-init="{body_init}" class="relative">',
    )
    assert head != HEAD_LOGIN_NAV, "body tag pattern not found"
    parts = [
        head, ADMIN_SHELL, tab_html,
        '</main><!-- closes admin main, orig line 6415 -->',
        FOOTER_ONLY,
        '</div><!-- close screen!=login wrapper (orig ~10185) -->',
        '</div><!-- close z-10 flex-col outer wrapper (orig ~10186) -->',
        FINAL_CLOSE,
    ]
    out = _shrink("\n".join(parts))
    with open(os.path.join(OUT_DIR, fname), "w", encoding="utf-8") as f:
        f.write(out)
    print(f"wrote {fname}: {len(out)} bytes")

def write_client_page(fname: str, body_init: str, tab_html: str, is_positions: bool):
    head = HEAD_LOGIN_NAV.replace(
        '<body x-data="terminal" x-cloak class="relative">',
        f'<body x-data="terminal" x-cloak x-init="{body_init}" class="relative">',
    )
    assert head != HEAD_LOGIN_NAV, "body tag pattern not found"
    parts = [head, CLIENT_SHELL, tab_html]
    if is_positions:
        # Confirmed pre-existing gap: this tab's own content never closes
        # its per-deployment template loop (7981), per-broker template
        # loop (7940), "broker tabs" div wrapper (7931), or its own outer
        # tab-wrapper div (7894) within its extracted range. LIFO order,
        # innermost first.
        parts.append('</template><!-- PATCH: closes per-deployment loop (orig template@7981) -->')
        parts.append('</template><!-- PATCH: closes per-broker loop (orig template@7940) -->')
        parts.append('</div><!-- PATCH: closes nested broker-tabs wrapper (orig div@7931) -->')
        parts.append('</div><!-- PATCH: closes positions tab wrapper (orig div@7894) -->')
    parts += [
        '</div><!-- close clientPhase==active wrapper (orig ~10174) -->',
        '</main><!-- closes client main, orig line 10176 -->',
        FOOTER_ONLY,
        '</div><!-- close screen!=login wrapper (orig ~10185) -->',
        '</div><!-- close z-10 flex-col outer wrapper (orig ~10186) -->',
        BROKER_MODAL,
        FINAL_CLOSE,
    ]
    out = _shrink("\n".join(parts))
    with open(os.path.join(OUT_DIR, fname), "w", encoding="utf-8") as f:
        f.write(out)
    print(f"wrote {fname}: {len(out)} bytes")

for tab, (a, b, init) in ADMIN_TABS.items():
    write_admin_page(f"admin_{tab}.html", init, seg(a, b))

for tab, (a, b, init) in CLIENT_TABS.items():
    write_client_page(f"client_{tab}.html", init, seg(a, b), is_positions=(tab == "positions"))

print("Done.")
