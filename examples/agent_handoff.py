# ABOUTME: Cross-session handoff pattern using stream.produce() and consumer groups.
# ABOUTME: One agent writes a handoff event; the next picks it up exactly once — no replay.

"""Agent Handoff — session-to-session memory via brooklet.

When one Claude Code session finishes a task, it can write a structured handoff
event so the *next* session picks up exactly where it left off — no replay of
old handoffs, no missed context.

This works because brooklet tracks a byte offset per consumer group. The second
session's ``read`` resumes from after the event the first run consumed, so a
third run of ``read`` with the same group returns nothing at all.

This is the gapless-resume property that makes brooklet useful for agent workflows:
unlike ``tail -f`` or re-reading a file, offset tracking gives you exactly the
events written *since your last read*.

Usage::

    # Session 1 ends — write a handoff
    uv run python examples/agent_handoff.py write ./handoff-demo \\
        --task "Implement auth middleware" \\
        --summary "Done. Tests pass. PR #42 waiting for review." \\
        --next "Add rate limiting to /api/login"

    # Session 2 starts — pick up the handoff (prints it once)
    uv run python examples/agent_handoff.py read ./handoff-demo --group sess-2

    # Session 2 restarts — nothing to read (already consumed)
    uv run python examples/agent_handoff.py read ./handoff-demo --group sess-2

The ``--group`` flag is the key: two sessions sharing a group name share the
same offset. Use a unique group per session to read independently.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

TOPIC = "agent/handoffs"


def cmd_write(args: argparse.Namespace) -> int:
    """Produce one handoff event to the agent/handoffs topic."""
    import brooklet

    stream = brooklet.open(args.stream_dir)

    event = {
        "task": args.task,
        "summary": args.summary,
        "next": args.next or "",
        "written_at": datetime.now(UTC).isoformat(),
    }

    stream.produce(TOPIC, event, source="agent-handoff")

    print(f"Handoff written to '{TOPIC}'")
    print(f"  task:    {args.task}")
    print(f"  summary: {args.summary}")
    if args.next:
        print(f"  next:    {args.next}")
    print(
        f"\nConsume with: uv run python examples/agent_handoff.py read"
        f" {args.stream_dir} --group <name>"
    )
    return 0


def cmd_read(args: argparse.Namespace) -> int:
    """Consume pending handoff events for this group (batch, no follow)."""
    import brooklet

    stream = brooklet.open(args.stream_dir)

    count = 0
    for event in stream.consume(TOPIC, group=args.group):
        count += 1
        print(f"--- Handoff #{count} (seq={event.get('_seq')}) ---")
        print(f"  task:    {event.get('task', '')}")
        print(f"  summary: {event.get('summary', '')}")
        if event.get("next"):
            print(f"  next:    {event['next']}")
        print(f"  written: {event.get('written_at', event.get('_ts', ''))}")
        if args.verbose:
            payload = {k: v for k, v in event.items() if not k.startswith("_")}
            print(f"  raw: {json.dumps(payload, indent=4)}")
        print()

    if count == 0:
        print(f"No pending handoffs for group '{args.group}'.")
        print("(Either no handoffs have been written, or this group already consumed them.)")
    else:
        print(f"Read {count} handoff(s) for group '{args.group}'.")
        print("Next read with the same group will return nothing — offset is now saved.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent_handoff.py",
        description="Session handoff protocol: write and read cross-session context.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # write subcommand
    w = sub.add_parser("write", help="Write a handoff event for the next session")
    w.add_argument("stream_dir", help="Brooklet stream directory")
    w.add_argument("--task", required=True, help="What was being worked on")
    w.add_argument("--summary", required=True, help="What was accomplished")
    w.add_argument("--next", default="", help="Recommended next action (optional)")
    w.set_defaults(func=cmd_write)

    # read subcommand
    r = sub.add_parser("read", help="Read pending handoffs for this consumer group")
    r.add_argument("stream_dir", help="Brooklet stream directory")
    r.add_argument("--group", default="agent", help="Consumer group name (default: agent)")
    r.add_argument("--verbose", action="store_true", help="Print full event payload")
    r.set_defaults(func=cmd_read)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
