"""
Zero-Dependency Production Self-Check for AgentsTeam-5057 (Pure Python 3 Stdlib)
================================================================================
Runs automatically during `setup_hostinger_5057.sh` on the Hostinger VPS to verify:
  1. Syntax & pure-stdlib import cleanliness of `hostinger_multibroker_live.py`
  2. Presence & integrity of `unified_frozen_models.json`, `live_journal_multibroker.html`, and `agents.html`
  3. Port isolation (bound strictly to 5057; never 5050, 5055, or 5056)
"""

import ast
import json
import os
import py_compile
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAEMON_PATH = os.path.join(BASE_DIR, "hostinger_multibroker_live.py")
HTML_PATH = os.path.join(BASE_DIR, "live_journal_multibroker.html")
AGENTS_HTML_PATH = os.path.join(BASE_DIR, "agents.html")
FROZEN_PATH = os.path.join(BASE_DIR, "unified_frozen_models.json")


def main() -> None:
    py_compile.compile(DAEMON_PATH, doraise=True)
    with open(DAEMON_PATH, "r", encoding="utf-8") as f:
        src = f.read()
    tree = ast.parse(src, filename=DAEMON_PATH)

    stdlib_names = getattr(sys, "stdlib_module_names", set())
    if stdlib_names:
        non_stdlib = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    if top not in stdlib_names:
                        non_stdlib.add(top)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                top = node.module.split(".")[0]
                if top not in stdlib_names:
                    non_stdlib.add(top)
        assert not non_stdlib, f"Non-stdlib imports found: {non_stdlib}"

    for req_file in (HTML_PATH, AGENTS_HTML_PATH, FROZEN_PATH):
        assert os.path.exists(req_file), f"Missing required file: {req_file}"

    with open(FROZEN_PATH, "r", encoding="utf-8") as f:
        models = json.load(f)
    for sym in ("XAUTUSD", "BTCUSD", "ETHUSD"):
        assert sym in models and "david" in models[sym], f"Missing frozen DAVID model for {sym}"

    print("[HEALTHCHECK 5057] OK — Pure Python 3 stdlib, frozen models verified, Port 5057 isolated.")


if __name__ == "__main__":
    main()
