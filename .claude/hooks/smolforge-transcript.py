#!/usr/bin/env python3
# ABOUTME: Uploads a Claude Code session transcript to SmolForge, linked to a commit SHA.
# ABOUTME: Runs as a PostToolUse hook after `git commit`, or manually for dry-runs and backfill.
"""Upload Claude Code transcripts to SmolForge.

Opt-in by design. The hook is inert unless the repository sets:

    git config --bool forge.transcripts.enabled true

Because transcripts on a public Forge repository are readable without
authentication, uploading is never enabled implicitly.

Modes
-----
    smolforge-transcript.py                 # hook mode: reads hook JSON on stdin
    smolforge-transcript.py --dry-run       # build the payload, print stats, upload nothing
    smolforge-transcript.py --upload        # upload explicitly, ignoring the enable flag

What gets published
-------------------
Only human-readable conversation text: `text` blocks from user and assistant
messages. Internal reasoning (`thinking`), tool calls (`tool_use`) and tool
output (`tool_result`) are excluded, since tool output routinely contains file
contents and credentials that do not belong in a published transcript.

Credentials
-----------
The Forge token is read from the OS credential helper via `git credential fill`,
the same store `sf auth git-credential` writes to. The token is never printed,
never written to disk, and never placed in a URL or argument.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

FORGE_URL = "https://forge.smol.ai"
PUBLISHED_BLOCK_TYPES = {"text"}
# Forge sits behind a CDN that rejects the default Python-urllib User-Agent.
USER_AGENT = "brooklet-forge-tools/1.0"


def run_git(args: list[str], cwd: Path | None = None) -> str:
    """Return stdout of a git command, or "" when it fails."""
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def transcripts_enabled(repo: Path) -> bool:
    return run_git(["config", "--bool", "--get", "forge.transcripts.enabled"], repo) == "true"


def slug_from_url(url: str) -> str | None:
    """Return "<owner>/<name>" when a remote URL points at Forge."""
    if "forge.smol.ai" not in url:
        return None
    slug = url.split("forge.smol.ai", 1)[1].lstrip(":/")
    if slug.endswith(".git"):
        slug = slug[: -len(".git")]
    return slug.strip("/") or None


def resolve_repo_slug(repo: Path) -> str | None:
    """Return "<owner>/<name>" for the Forge repository.

    Prefers an explicit `forge.repo` config value, then any remote whose URL
    points at Forge. Remotes are matched by URL rather than by name, since which
    remote holds Forge is a local naming choice.
    """
    configured = run_git(["config", "--get", "forge.repo"], repo)
    if configured:
        return configured.strip("/")

    for remote in run_git(["remote"], repo).splitlines():
        slug = slug_from_url(run_git(["remote", "get-url", remote.strip()], repo))
        if slug:
            return slug
    return None


def read_token(slug: str) -> str | None:
    """Fetch the Forge token from the OS credential helper without echoing it."""
    query = f"protocol=https\nhost=forge.smol.ai\npath={slug}.git\n\n"
    try:
        out = subprocess.run(
            ["git", "credential", "fill"],
            input=query,
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    for line in out.stdout.splitlines():
        if line.startswith("password="):
            return line[len("password=") :]
    return None


def find_transcript(session_id: str) -> Path | None:
    """Locate <session_id>.jsonl under ~/.claude/projects/.

    Session files sit directly in the per-project directory; there is no
    `sessions/` subdirectory.
    """
    root = Path.home() / ".claude" / "projects"
    if not root.is_dir():
        return None
    for candidate in root.glob(f"*/{session_id}.jsonl"):
        return candidate
    matches = sorted(root.rglob(f"{session_id}.jsonl"))
    return matches[0] if matches else None


def extract_text(content: object) -> str:
    """Collapse a message's content into published text, dropping non-text blocks."""
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") in PUBLISHED_BLOCK_TYPES:
            text = block.get("text", "")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n\n".join(parts)


def build_messages(path: Path) -> list[dict]:
    """Convert a Claude Code JSONL transcript into Forge's message list."""
    messages: list[dict] = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("type") not in ("user", "assistant"):
                continue
            # Subagent conversations are recorded separately; keep the main thread.
            if entry.get("isSidechain"):
                continue
            text = extract_text(entry.get("message", {}).get("content"))
            if not text:
                continue
            messages.append(
                {
                    "role": entry["type"],
                    "content": text,
                    "timestamp": entry.get("timestamp", ""),
                }
            )
    return messages


def post_transcript(slug: str, token: str, payload: dict) -> tuple[int, str]:
    request = urllib.request.Request(
        f"{FORGE_URL}/api/repos/{slug}/transcripts",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")
    except urllib.error.URLError as exc:
        return 0, str(exc)


def hook_payload_from_stdin() -> dict:
    if sys.stdin.isatty():
        return {}
    try:
        return json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="build payload, upload nothing")
    parser.add_argument("--upload", action="store_true", help="upload even if disabled")
    parser.add_argument("--session", help="session id (defaults to hook session, else newest)")
    parser.add_argument("--sha", help="commit SHA to link (defaults to HEAD)")
    args = parser.parse_args()

    hook = hook_payload_from_stdin() if not (args.dry_run or args.upload) else {}

    repo = Path(os.environ.get("CLAUDE_PROJECT_DIR") or Path.cwd())
    toplevel = run_git(["rev-parse", "--show-toplevel"], repo)
    if toplevel:
        repo = Path(toplevel)

    # Hook mode: only react to an actual commit, and only when explicitly enabled.
    if hook:
        command = hook.get("tool_input", {}).get("command", "")
        if "git commit" not in command:
            return 0
        if not transcripts_enabled(repo):
            return 0

    session_id = args.session or hook.get("session_id") or os.environ.get("CLAUDE_SESSION_ID", "")
    transcript = find_transcript(session_id) if session_id else None

    if transcript is None:
        slug_dir = Path.home() / ".claude" / "projects"
        candidates = sorted(
            slug_dir.glob("*/*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        if not candidates:
            print("smolforge-transcript: no transcript found", file=sys.stderr)
            return 0 if hook else 1
        transcript = candidates[0]
        session_id = transcript.stem

    slug = resolve_repo_slug(repo)
    if not slug:
        print(
            "smolforge-transcript: no Forge repo; set `git config forge.repo <owner>/<name>` "
            "or add a `forge` remote",
            file=sys.stderr,
        )
        return 0 if hook else 1

    sha = args.sha or run_git(["rev-parse", "HEAD"], repo)
    messages = build_messages(transcript)
    if not messages:
        print("smolforge-transcript: transcript held no publishable text", file=sys.stderr)
        return 0 if hook else 1

    payload = {
        "session_id": session_id,
        "agent_type": "claude-code",
        "messages": messages,
    }
    if sha:
        payload["commit_sha"] = sha

    if args.dry_run:
        roles: dict[str, int] = {}
        for message in messages:
            roles[message["role"]] = roles.get(message["role"], 0) + 1
        chars = sum(len(m["content"]) for m in messages)
        print(f"transcript:  {transcript}")
        print(f"session_id:  {session_id}")
        print(f"repo:        {slug}")
        print(f"commit_sha:  {sha or '(none)'}")
        print(f"messages:    {len(messages)} ({roles})")
        print(f"payload:     {chars} chars of text, {len(json.dumps(payload))} bytes JSON")
        print(f"token:       {'present in credential store' if read_token(slug) else 'MISSING'}")
        first = messages[0]["content"].replace("\n", " ")[:160]
        print(f"first msg:   [{messages[0]['role']}] {first}")
        print("\n(dry run — nothing uploaded)")
        return 0

    if not (args.upload or transcripts_enabled(repo)):
        print(
            "smolforge-transcript: disabled. Enable with "
            "`git config --bool forge.transcripts.enabled true`, or pass --upload.",
            file=sys.stderr,
        )
        return 1

    token = read_token(slug)
    if not token:
        print("smolforge-transcript: no Forge token in credential store", file=sys.stderr)
        return 0 if hook else 1

    status, body = post_transcript(slug, token, payload)
    if status in (200, 201):
        print(f"smolforge-transcript: uploaded {len(messages)} messages for {session_id[:8]}")
        return 0
    print(f"smolforge-transcript: upload failed ({status}): {body[:300]}", file=sys.stderr)
    return 0 if hook else 1


if __name__ == "__main__":
    sys.exit(main())
