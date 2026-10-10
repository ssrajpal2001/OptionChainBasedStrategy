r"""Build script: splits monitor.html into separate admin/client per-tab
pages (ui_layer/templates/pages/*.html), served by dashboard_server.py's
/admin/{tab} and /client/{tab} routes -- so a visitor only downloads
their own role's markup instead of the full ~681KB monolithic file.

RUN THIS AGAIN AFTER EVERY monitor.html EDIT. The split pages are
generated artifacts, not hand-maintained -- editing them directly will
be silently lost the next time this script runs. Run from the repo
root: `python scripts/build_pages.py`.

All line-number boundaries below are ABSOLUTE line numbers in the current
monitor.html, found by grepping stable, unique text anchors (not a
hand-tracked offset from some earlier baseline -- that approach was used
through 2026-10-09 and became unreliable once edits started landing in the
MIDDLE of the file instead of only before all boundaries, since different
regions then shift by different amounts). RUN THIS AGAIN AFTER EVERY
monitor.html EDIT, and if an assertion fails, re-derive ALL boundaries
fresh the same way rather than patching one number:

    grep -n '<body x-data="terminal"' ui_layer/templates/monitor.html
    grep -n 'x-show="adminTab === .dashboard.\|strategies.\|feeder.\|trap.\|clients.'"'"' ui_layer/templates/monitor.html
    grep -n 'x-show="clientMenu === .dashboard.\|strategies.\|brokers.\|positions.\|history.'"'"' ui_layer/templates/monitor.html
    grep -n '<main x-show="screen === .client.'"'"'\|^<footer\|end admin layout\|end clients tab\|BROKER ADD MODAL\|^</body>' ui_layer/templates/monitor.html

Each tab's content ends exactly one line before the NEXT tab's own
x-show/x-if marker (or, for the last tab in each group, at the line noted
below). ADMIN_SHELL/CLIENT_SHELL are everything between the shared chrome
and the first tab's own marker.

CONFIRMED PRE-EXISTING BUG (not introduced by this split, still present):
within the "positions" tab, two divs never close inside that range -- the
tab's own outer wrapper div and a nested "broker tabs" wrapper div, each
paired with an unclosed x-for template. In the original monolithic file
this is silently absorbed by the browser's forgiving parser; in split
files it must be patched explicitly (see the 4 PATCH lines in
write_client_page) so client_positions.html is well-formed on its own.
The orphaned closing tag this produces downstream (commented "end active"
in the source) is EXCLUDED from history's extracted range since its true
matching open no longer exists in that file.
"""
import os
import re

SRC = "ui_layer/templates/monitor.html"
OUT_DIR = "ui_layer/templates/pages"

lines = open(SRC, encoding="utf-8").read().split("\n")

def seg(a, b):
    return "\n".join(lines[a - 1:b])

# 2026-10-10: re-derived as absolute line numbers (see the module docstring
# for the grep commands used) after the previous hand-tracked-offset scheme
# broke on an edit landing between ADMIN_SHELL and CLIENT_SHELL instead of
# before both.
HEAD_LOGIN_NAV = seg(1, 3573)         # head + shared JS + login + top navbar + mobile drawer
ADMIN_SHELL    = seg(3574, 3637)      # admin subnav + <main admin> open + banner alerts
CLIENT_SHELL   = seg(6433, 6720)      # <main client> open + alerts + onboarding/pending + topbar + subnav
FOOTER_ONLY    = seg(10232, 10236)    # just <footer>...</footer>, no surrounding div-closes
BROKER_MODAL   = seg(10242, 10325)    # client-only broker-add modal
FINAL_CLOSE    = seg(10326, 10330)    # blank lines + </body></html>

ADMIN_TABS = {
    "dashboard":  (3638, 4087, "adminTab='dashboard'"),
    "strategies": (4088, 5362, "adminTab='strategies'; loadStrategyConfig(); loadStrategyRegistry(); loadIndexConfig(adminSelectedIndex)"),
    "feeder":     (5363, 5794, "adminTab='feeder'"),
    "trap":       (5795, 5942, "adminTab='trap'"),
    "clients":    (5943, 6425, "adminTab='clients'; loadClientProfiles(); loadRiskSummary()"),
}
CLIENT_TABS = {
    "dashboard":  (6721, 6797, "clientMenu='dashboard'; loadClientPositions()"),
    "strategies": (6798, 6932, "clientMenu='strategies'"),
    "brokers":    (6933, 7905, "clientMenu='brokers'"),
    "positions":  (7906, 10036, "clientMenu='positions'; loadClientPositions()"),
    "history":    (10037, 10225, "clientMenu='history'; loadClientHistory()"),  # excludes orphaned line
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
        parts.append('</template><!-- PATCH: closes per-deployment loop -->')
        parts.append('</template><!-- PATCH: closes per-broker loop -->')
        parts.append('</div><!-- PATCH: closes nested broker-tabs wrapper -->')
        parts.append('</div><!-- PATCH: closes positions tab wrapper -->')
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
