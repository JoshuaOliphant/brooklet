#!/usr/bin/env python3
# ABOUTME: Small CLI for this project's Forge issue tracker (list, show, create, close, comment).
# ABOUTME: Exists because the `sf` CLI has no issue subcommand; issues are REST-only.
"""Work with the project's Forge issues from the terminal.

    scripts/forge_issue.py list                      # open issues
    scripts/forge_issue.py list --state all
    scripts/forge_issue.py show 12
    scripts/forge_issue.py create --title "..." --body "..." --label type:bug --label P2
    scripts/forge_issue.py comment 12 --body "..."
    scripts/forge_issue.py close 12 --reason "fixed in abc1234"
    scripts/forge_issue.py labels

The Forge repository is read from `git config forge.repo`, falling back to the
`forge` remote's URL. `origin` is deliberately ignored, because this project
keeps GitHub on `origin` and Forge on a separate remote.

The token comes from the OS credential helper that `sf auth git-credential`
populates. It is never printed and never written to disk.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request

BASE = "https://forge.smol.ai"
# Forge sits behind a CDN that rejects the default Python-urllib User-Agent.
USER_AGENT = "brooklet-forge-tools/1.0"


def git(args: list[str]) -> str:
    out = subprocess.run(["git", *args], capture_output=True, text=True, check=False, timeout=15)
    return out.stdout.strip() if out.returncode == 0 else ""


def repo_slug() -> str:
    configured = git(["config", "--get", "forge.repo"])
    if configured:
        return configured.strip("/")
    url = git(["remote", "get-url", "forge"])
    if url and "forge.smol.ai" in url:
        slug = url.split("forge.smol.ai", 1)[1].lstrip(":/")
        if slug.endswith(".git"):
            slug = slug[: -len(".git")]
        return slug.strip("/")
    raise SystemExit(
        "No Forge repo configured. Run: git config forge.repo <owner>/<name>"
    )


def token(slug: str) -> str:
    query = f"protocol=https\nhost=forge.smol.ai\npath={slug}.git\n\n"
    out = subprocess.run(
        ["git", "credential", "fill"],
        input=query,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    for line in out.stdout.splitlines():
        if line.startswith("password="):
            return line[len("password=") :]
    raise SystemExit(
        "No Forge token found. Run: sf auth git-credential <owner>/<repo>"
    )


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, object]:
    slug = repo_slug()
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        f"{BASE}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {token(slug)}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode(errors="replace")
            return response.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")
    except urllib.error.URLError as exc:
        return 0, str(exc)


def api_path(suffix: str = "") -> str:
    return f"/api/repos/{repo_slug()}{suffix}"


def die(status: int, body: object) -> None:
    print(f"Forge API error {status}: {str(body)[:400]}", file=sys.stderr)
    raise SystemExit(1)


def cmd_list(args: argparse.Namespace) -> int:
    query = "" if args.state == "all" else f"?state={args.state}"
    status, body = call("GET", api_path(f"/issues{query}"))
    if status != 200 or not isinstance(body, dict):
        die(status, body)
    issues = body.get("issues", [])
    if not issues:
        print("(no issues)")
        return 0
    for issue in issues:
        labels = ",".join(sorted(label["name"] for label in issue.get("labels") or []))
        marker = " " if issue.get("state") == "open" else "x"
        print(f"[{marker}] #{issue['number']:<4} {labels:<22} {issue['title']}")
    print(f"\n{len(issues)} issue(s)")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    status, body = call("GET", api_path(f"/issues/{args.number}"))
    if status != 200 or not isinstance(body, dict):
        die(status, body)
    issue = body["issue"]
    labels = ", ".join(sorted(label["name"] for label in issue.get("labels") or []))
    print(f"#{issue['number']}  {issue['title']}")
    print(f"state: {issue['state']}    labels: {labels or '(none)'}")
    print(f"\n{issue.get('body') or '(no body)'}")
    for comment in body.get("comments") or []:
        print(f"\n--- comment ---\n{comment.get('body', '')}")
    return 0


def cmd_create(args: argparse.Namespace) -> int:
    status, body = call(
        "POST", api_path("/issues"), {"title": args.title, "body": args.body or ""}
    )
    if status not in (200, 201) or not isinstance(body, dict):
        die(status, body)
    number = body["issue"]["number"]
    if args.label:
        lstatus, lbody = call(
            "POST", api_path(f"/issues/{number}/labels"), {"labels": args.label}
        )
        if lstatus not in (200, 201):
            print(f"warning: labels not applied ({lstatus}): {str(lbody)[:200]}", file=sys.stderr)
    print(f"created #{number}: {args.title}")
    return 0


def cmd_comment(args: argparse.Namespace) -> int:
    status, body = call(
        "POST", api_path(f"/issues/{args.number}/comments"), {"body": args.body}
    )
    if status not in (200, 201):
        die(status, body)
    print(f"commented on #{args.number}")
    return 0


def cmd_close(args: argparse.Namespace) -> int:
    if args.reason:
        call("POST", api_path(f"/issues/{args.number}/comments"), {"body": args.reason})
    status, body = call("PATCH", api_path(f"/issues/{args.number}"), {"state": "closed"})
    if status != 200:
        die(status, body)
    print(f"closed #{args.number}")
    return 0


def cmd_labels(_: argparse.Namespace) -> int:
    status, body = call("GET", api_path("/labels"))
    if status != 200 or not isinstance(body, dict):
        die(status, body)
    for label in body.get("labels", []):
        print(f"{label['name']:<16} {label.get('description') or ''}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Work with this project's Forge issues.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("list", help="list issues")
    p.add_argument("--state", choices=["open", "closed", "all"], default="open")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("show", help="show one issue with comments")
    p.add_argument("number", type=int)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("create", help="create an issue")
    p.add_argument("--title", required=True)
    p.add_argument("--body", default="")
    p.add_argument("--label", action="append", default=[])
    p.set_defaults(func=cmd_create)

    p = sub.add_parser("comment", help="comment on an issue")
    p.add_argument("number", type=int)
    p.add_argument("--body", required=True)
    p.set_defaults(func=cmd_comment)

    p = sub.add_parser("close", help="close an issue")
    p.add_argument("number", type=int)
    p.add_argument("--reason", help="posted as a comment before closing")
    p.set_defaults(func=cmd_close)

    p = sub.add_parser("labels", help="list labels")
    p.set_defaults(func=cmd_labels)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
