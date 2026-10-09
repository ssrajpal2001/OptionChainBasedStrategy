"""Build script: compiles ui_layer/static/tailwind.css from the real
monitor.html + split pages, replacing the Tailwind CDN JIT script (which
re-scanned and recompiled the whole utility set in-browser on every single
page load -- paid once per login session before the admin/client page split,
once per tab click after it).

RUN THIS AGAIN AFTER EVERY monitor.html EDIT that adds/removes a class
(same rule as build_pages.py -- run that FIRST so pages/*.html reflects the
edit, THEN this, so the CSS build sees the real current class usage across
both). Run from the repo root: `python scripts/build_tailwind.py`.

Downloads the official Tailwind v4 standalone CLI (no npm/node needed) into
a local, gitignored cache on first run; reused on every later run.
"""
import os
import platform
import stat
import subprocess
import urllib.request

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE_DIR = os.path.join(_ROOT, ".tailwind_cli")
_INPUT = os.path.join(_ROOT, "ui_layer", "tailwind_input.css")
_OUTPUT = os.path.join(_ROOT, "ui_layer", "static", "tailwind.css")

_PLATFORM_ASSET = {
    ("Windows", "AMD64"): "tailwindcss-windows-x64.exe",
    ("Linux", "x86_64"):  "tailwindcss-linux-x64",
    ("Darwin", "x86_64"): "tailwindcss-macos-x64",
    ("Darwin", "arm64"):  "tailwindcss-macos-arm64",
}


def _cli_path() -> str:
    asset = _PLATFORM_ASSET.get((platform.system(), platform.machine()))
    if not asset:
        raise SystemExit(f"No known Tailwind CLI asset for {platform.system()}/{platform.machine()}")
    path = os.path.join(_CACHE_DIR, asset)
    if not os.path.exists(path):
        os.makedirs(_CACHE_DIR, exist_ok=True)
        url = f"https://github.com/tailwindlabs/tailwindcss/releases/latest/download/{asset}"
        print(f"Downloading Tailwind CLI from {url} ...")
        urllib.request.urlretrieve(url, path)
        st = os.stat(path)
        os.chmod(path, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def main() -> None:
    cli = _cli_path()
    os.makedirs(os.path.dirname(_OUTPUT), exist_ok=True)
    subprocess.run(
        [cli, "-i", _INPUT, "-o", _OUTPUT, "--minify"],
        cwd=os.path.join(_ROOT, "ui_layer"),
        check=True,
    )
    size = os.path.getsize(_OUTPUT)
    print(f"Built {_OUTPUT}: {size} bytes")


if __name__ == "__main__":
    main()
