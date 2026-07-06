"""Dashboard diagnostics + stopped-deployment position-card fixes."""
import os

REPO = os.path.dirname(os.path.abspath(__file__))


def _fix_admin_console(path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    marker = '''            self._dashboard_task = asyncio.create_task(
                self._dashboard.serve(
                    host=self._dashboard_host,
                    port=self._dashboard_port,
                ),
                name="dashboard_server",
            )
'''
    if marker not in text:
        raise RuntimeError("admin_console.py: expected dashboard_task block not found")

    add_on = '''            def _dashboard_done(task: asyncio.Task) -> None:
                if task.cancelled():
                    return
                exc = task.exception()
                if exc:
                    logger.exception("Dashboard server task crashed: %s", exc)

            self._dashboard_task.add_done_callback(_dashboard_done)
'''
    new = text.replace(marker, marker + add_on, 1)

    print_marker = '''            print(
                f"\\n[Dashboard] http://{self._dashboard_host}:{self._dashboard_port}  "
                f"(WebSocket: ws://{self._dashboard_host}:{self._dashboard_port}/ws)\\n",
                flush=True,
            )
'''
    if print_marker not in new:
        raise RuntimeError("admin_console.py: expected print block not found")

    log_addition = '''            logger.info(
                "Dashboard starting on http://%s:%d",
                self._dashboard_host, self._dashboard_port,
            )
'''
    new = new.replace(print_marker, print_marker + log_addition, 1)

    with open(path, "w", encoding="utf-8") as f:
        f.write(new)
    print("patched", path)


def _fix_dashboard_server_serve(path: str, text: str) -> str:
    """Wrap serve() body so any crash is logged."""
    start_marker = "        await self._client_db.initialise()\n"
    end_marker = "                boot_task.cancel()\n"

    if "Dashboard: initializing server on %s:%d" in text:
        print("dashboard_server.py: serve() already wrapped, skipping")
        return text

    start = text.find(start_marker)
    if start == -1:
        raise RuntimeError("dashboard_server.py: 'await self._client_db.initialise()' not found")
    end = text.find(end_marker, start)
    if end == -1:
        raise RuntimeError("dashboard_server.py: 'boot_task.cancel()' not found")
    end += len(end_marker)

    block = text[start:end]

    init_log = '        logger.info("Dashboard: initializing server on %s:%d ...", host, port)\n\n'

    indented_lines = []
    for line in block.splitlines(keepends=True):
        if line.strip() == "" and line.endswith("\n"):
            indented_lines.append("\n")
        else:
            indented_lines.append("    " + line)
    indented_block = "".join(indented_lines)

    wrapper = (
        init_log
        + "        try:\n"
        + indented_block
        + "\n        except Exception:\n"
        + '            logger.exception("Dashboard: failed to start on %s:%d", host, port)\n'
    )

    return text[:start] + wrapper + text[end:]


def _fix_dashboard_server_positions(path: str, text: str) -> str:
    """Show stopped deployments that still hold an open position."""
    if "_has_open_position" in text:
        print("dashboard_server.py: position-card fix already applied, skipping")
        return text

    old_block = '''            cid = user.get("client_id", "")
            try:
                _all_deps = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
                # Only show position cards for deployments that are currently running (is_running=1).
                # Stopped deployments (is_running=0) have no live book — looking them up produces
                # log spam and "No open position" noise for cards the user intentionally stopped.
                deployments = [d for d in _all_deps if int(d.get("is_running", 0) or 0) == 1]
            except Exception:
                deployments = []

            def _ic_legs(pos, product="NRML"):'''

    new_block = '''            cid = user.get("client_id", "")
            try:
                _all_deps = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
            except Exception:
                _all_deps = []

            def _is_running(dep: dict) -> bool:
                return int(dep.get("is_running", 0) or 0) == 1

            def _has_open_position(dep: dict) -> bool:
                """A stopped deployment may still have a live open position that must be visible."""
                if _is_running(dep):
                    return False
                sname = dep.get("strategy_name", "")
                underlying = dep.get("underlying") or dep.get("assigned_instrument") or ""
                bid = dep.get("binding_id", "")
                if sname == "sell_straddle":
                    strat = _srv._find_ss_book(cid, bid, underlying)
                    pos = getattr(strat, "_position", None) if strat else None
                    return pos is not None and getattr(pos, "status", "") == "open"
                if sname == "iron_condor":
                    strat = _find(getattr(_srv, "_iron_condors", []), underlying)
                    pos = getattr(strat, "_position", None) if strat else None
                    return pos is not None and getattr(pos, "status", "") == "open"
                if sname == "trap_scanner":
                    strat = _srv._find_trap_book(cid, bid, underlying)
                    pos = getattr(strat, "_position", None) if strat else None
                    return pos is not None
                return False

            # Show running deployments (live cards) OR stopped deployments that still
            # hold an open position (so the user can monitor/close it).
            deployments = [d for d in _all_deps if _is_running(d) or _has_open_position(d)]

            def _ic_legs(pos, product="NRML"):'''

    if old_block not in text:
        raise RuntimeError("dashboard_server.py: expected positions block not found")
    return text.replace(old_block, new_block, 1)


def _fix_dashboard_server_trap_finder(path: str, text: str) -> str:
    """Add helper to locate a per-binding trap-scanner book."""
    if "def _find_trap_book" in text:
        print("dashboard_server.py: _find_trap_book already present, skipping")
        return text

    old_block = '''        return None

    def stop(self) -> None:
        self._ws_bridge.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True'''

    new_block = '''        return None

    def _find_trap_book(self, client_id: str, binding_id: str, underlying: str):
        """Locate the per-binding trap-scanner book for this deployment."""
        if self._trap_scanner_manager is not None:
            b = self._trap_scanner_manager.find(client_id, binding_id, underlying)
            if b is not None:
                return b
            logger.info("_find_trap_book miss: cid=%s bid=%s und=%s books=%s",
                        client_id, binding_id, underlying,
                        list(self._trap_scanner_manager._books.keys()))
        return None

    def stop(self) -> None:
        self._ws_bridge.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True'''

    if old_block not in text:
        raise RuntimeError("dashboard_server.py: expected stop() block not found")
    return text.replace(old_block, new_block, 1)


def _fix_dashboard_server(path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    text = _fix_dashboard_server_positions(path, text)
    text = _fix_dashboard_server_trap_finder(path, text)
    text = _fix_dashboard_server_serve(path, text)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print("patched", path)


if __name__ == "__main__":
    _fix_admin_console(os.path.join(REPO, "management", "admin_console.py"))
    _fix_dashboard_server(os.path.join(REPO, "ui_layer", "dashboard_server.py"))
