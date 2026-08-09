#!/usr/bin/env python3
# ABOUTME: Detects changes to the SmolForge platform contract, CLI surface, and package version.
# ABOUTME: Compares live values against git-tracked snapshots under docs/forge/.
"""Check whether SmolForge has changed under us.

Forge is alpha and moves quickly, so the agent contract (`llms.txt`), the CLI
command surface, and the published package version are all moving targets. This
compares each against a git-tracked snapshot so `git diff` shows exactly what
the platform changed.

    scripts/forge_check_updates.py            # report drift
    scripts/forge_check_updates.py --diff     # also print the full llms.txt diff
    scripts/forge_check_updates.py --update   # accept current state as the new baseline

Exit status is 0 when nothing changed and 1 when drift was found, so it works as
a scheduled check.
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

LLMS_URL = "https://forge.smol.ai/llms.txt"
NPM_URL = "https://registry.npmjs.org/@smolai/forge/latest"
USER_AGENT = "brooklet-forge-tools/1.0"

SNAPSHOT_DIR = Path("docs/forge")
LLMS_SNAPSHOT = SNAPSHOT_DIR / "llms-snapshot.txt"
CLI_SNAPSHOT = SNAPSHOT_DIR / "cli-commands-snapshot.txt"
VERSION_SNAPSHOT = SNAPSHOT_DIR / "versions-snapshot.json"

HEADING = re.compile(r"^#{2,3} (.+)$", re.MULTILINE)


def fetch(url: str) -> str | None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode(errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        print(f"  ! could not fetch {url}: {exc}", file=sys.stderr)
        return None


def sf_commands() -> str | None:
    """Return the installed CLI's command list, one `sf <path>` per line."""
    try:
        out = subprocess.run(
            ["sf", "commands"], capture_output=True, text=True, check=False, timeout=60
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"  ! could not run `sf commands`: {exc}", file=sys.stderr)
        return None
    if out.returncode != 0:
        return None
    lines = [line.split("\t")[0].strip() for line in out.stdout.splitlines() if line.strip()]
    return "\n".join(sorted(line for line in lines if line.startswith("sf "))) + "\n"


def installed_version() -> str | None:
    """Read the installed @smolai/forge version from its package.json."""
    try:
        out = subprocess.run(
            ["npm", "ls", "-g", "@smolai/forge", "--json", "--depth=0"],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        data = json.loads(out.stdout or "{}")
        return data.get("dependencies", {}).get("@smolai/forge", {}).get("version")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None


def published_version() -> str | None:
    raw = fetch(NPM_URL)
    if not raw:
        return None
    try:
        return json.loads(raw).get("version")
    except json.JSONDecodeError:
        return None


def read(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.is_file() else None


def summarise_sections(old: str, new: str) -> tuple[list[str], list[str]]:
    old_sections = set(HEADING.findall(old))
    new_sections = set(HEADING.findall(new))
    return sorted(new_sections - old_sections), sorted(old_sections - new_sections)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true", help="accept current state as baseline")
    parser.add_argument("--diff", action="store_true", help="print the full llms.txt diff")
    args = parser.parse_args()

    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
    ).stdout.strip()
    snapshot_dir = (Path(root) / SNAPSHOT_DIR) if root else SNAPSHOT_DIR
    llms_path = snapshot_dir / LLMS_SNAPSHOT.name
    cli_path = snapshot_dir / CLI_SNAPSHOT.name
    ver_path = snapshot_dir / VERSION_SNAPSHOT.name

    live_llms = fetch(LLMS_URL)
    live_cli = sf_commands()
    local_ver = installed_version()
    remote_ver = published_version()

    drift = False

    print("SmolForge drift check\n")

    # ---- agent contract -----------------------------------------------------
    print("llms.txt (agent contract)")
    old_llms = read(llms_path)
    if live_llms is None:
        print("  skipped — fetch failed")
    elif old_llms is None:
        print(f"  no baseline yet; run --update to create {llms_path}")
        drift = True
    elif old_llms == live_llms:
        print(f"  unchanged ({len(live_llms.splitlines())} lines)")
    else:
        drift = True
        old_lines = old_llms.splitlines()
        new_lines = live_llms.splitlines()
        added, removed = summarise_sections(old_llms, live_llms)
        print(f"  CHANGED: {len(old_lines)} -> {len(new_lines)} lines")
        if added:
            print(f"  new sections:     {', '.join(added)}")
        if removed:
            print(f"  removed sections: {', '.join(removed)}")
        if not added and not removed:
            print("  same sections; wording or details changed")
        if args.diff:
            print()
            for line in difflib.unified_diff(
                old_lines, new_lines, "snapshot", "live", lineterm="", n=2
            ):
                print(f"    {line}")

    # ---- CLI surface --------------------------------------------------------
    print("\nsf CLI commands")
    old_cli = read(cli_path)
    if live_cli is None:
        print("  skipped — `sf commands` unavailable")
    elif old_cli is None:
        print(f"  no baseline yet; run --update to create {cli_path}")
        drift = True
    elif old_cli == live_cli:
        print(f"  unchanged ({len(live_cli.strip().splitlines())} commands)")
    else:
        drift = True
        old_set = set(old_cli.split("\n"))
        new_set = set(live_cli.split("\n"))
        gained = sorted(c for c in new_set - old_set if c)
        lost = sorted(c for c in old_set - new_set if c)
        print("  CHANGED")
        for c in gained:
            print(f"    + {c}")
        for c in lost:
            print(f"    - {c}")

    # ---- versions -----------------------------------------------------------
    print("\n@smolai/forge version")
    print(f"  installed: {local_ver or 'unknown'}")
    print(f"  published: {remote_ver or 'unknown'}")
    if local_ver and remote_ver and local_ver != remote_ver:
        drift = True
        print(f"  UPGRADE AVAILABLE: npm install -g @smolai/forge@{remote_ver}")
    old_ver = read(ver_path)
    if old_ver:
        try:
            previous = json.loads(old_ver).get("published")
            if remote_ver and previous and previous != remote_ver:
                drift = True
                print(f"  published version moved since last snapshot: {previous} -> {remote_ver}")
        except json.JSONDecodeError:
            pass

    # ---- update -------------------------------------------------------------
    if args.update:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        wrote = []
        if live_llms is not None:
            llms_path.write_text(live_llms, encoding="utf-8")
            wrote.append(llms_path.name)
        if live_cli is not None:
            cli_path.write_text(live_cli, encoding="utf-8")
            wrote.append(cli_path.name)
        ver_path.write_text(
            json.dumps({"installed": local_ver, "published": remote_ver}, indent=2) + "\n",
            encoding="utf-8",
        )
        wrote.append(ver_path.name)
        print(f"\nbaseline updated: {', '.join(wrote)}")
        print("Commit the snapshots so the next check diffs against them.")
        return 0

    print()
    if drift:
        print("Drift found. Review the changes, then run --update to accept the new baseline.")
        return 1
    print("No drift.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
