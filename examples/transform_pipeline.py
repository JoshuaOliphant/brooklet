# ABOUTME: Example showing the consume → transform → produce ETL pattern with brooklet.
# ABOUTME: Reads raw git-log JSONL, enriches events, and writes to a derived topic via produce().

"""Transform Pipeline — consume, enrich, and produce a derived topic.

Brooklet's ``produce()`` method exists for exactly this pattern: a consumer
reads raw events, transforms them, and writes the results to a new topic.
The derived topic is a first-class brooklet topic — it has offsets, can be
followed, and carries ``_src`` provenance so you know where it came from.

This example turns a raw git-log JSONL stream into an enriched commit feed:

  - adds ``is_fix`` (True when the commit message starts with "fix")
  - adds ``summary`` (first 72 chars of the message, for compact display)
  - records ``author_length`` as a silly but illustrative enrichment

Seeding the source topic::

    git log --format='{"hash":"%h","author":"%an","msg":"%s","date":"%aI"}' -20 \\
      | brooklet produce git/log --stream-dir ./demo

Running the pipeline::

    uv run python examples/transform_pipeline.py ./demo

Inspecting the derived topic::

    brooklet cat git/enriched --stream-dir ./demo

Running it again with the same group produces nothing — the consumer group
``pipeline`` is already at the end of ``git/log``. This is offset tracking
at work: the pipeline is idempotent and safe to restart.
"""

from __future__ import annotations

import argparse
import sys

SOURCE_TOPIC = "git/log"
DERIVED_TOPIC = "git/enriched"
PIPELINE_GROUP = "pipeline"


def enrich(raw: dict) -> dict:
    """Add derived fields to a raw git-log event.

    This is the transform step — the place where domain logic lives.
    Keep it pure: no I/O, just dict-in / dict-out.
    """
    msg = raw.get("msg", "")
    return {
        **raw,
        "is_fix": msg.lower().startswith("fix"),
        "summary": msg[:72],
        "author_length": len(raw.get("author", "")),
    }


def run_pipeline(stream_dir: str, group: str, verbose: bool) -> int:
    """Read from SOURCE_TOPIC, enrich each event, write to DERIVED_TOPIC."""
    import brooklet

    stream = brooklet.open(stream_dir)

    # Verify the source topic exists before starting.
    registered = set(stream.topics())
    if SOURCE_TOPIC not in registered:
        print(
            f"Source topic '{SOURCE_TOPIC}' not found in {stream_dir}.\n"
            f"Seed it first:\n\n"
            "  git log --format='{\"hash\":\"%h\",\"author\":\"%an\",\"msg\":\"%s\"}'"
            " -20 \\\n"
            f"    | brooklet produce {SOURCE_TOPIC} --stream-dir {stream_dir}\n",
            file=sys.stderr,
        )
        return 1

    produced = 0
    skipped = 0

    for raw in stream.consume(SOURCE_TOPIC, group=group):
        # Strip brooklet envelope fields before transforming — they'll be re-added.
        payload = {k: v for k, v in raw.items() if not k.startswith("_")}

        if not payload.get("hash"):
            skipped += 1
            continue

        enriched = enrich(payload)
        stream.produce(DERIVED_TOPIC, enriched, source="transform-pipeline")
        produced += 1

        if verbose:
            fix_marker = " [FIX]" if enriched["is_fix"] else ""
            print(f"  {enriched['hash']}  {enriched['author']}  {enriched['summary']}{fix_marker}")

    if produced == 0 and skipped == 0:
        print(f"Nothing to process — group '{group}' is already caught up on '{SOURCE_TOPIC}'.")
        print(
            f"(Inspect the derived topic: brooklet cat {DERIVED_TOPIC}"
            f" --stream-dir {stream_dir})"
        )
    else:
        print(f"\nPipeline complete: {produced} enriched, {skipped} skipped.")
        print(f"Derived topic '{DERIVED_TOPIC}' now has {produced} new events.")
        print("\nInspect with:")
        print(f"  brooklet cat {DERIVED_TOPIC} --stream-dir {stream_dir}")
        print(f"  brooklet cat {DERIVED_TOPIC} --stream-dir {stream_dir} | jq '.is_fix'")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="transform_pipeline.py",
        description="Consume git/log events, enrich them, and produce to git/enriched.",
    )
    parser.add_argument("stream_dir", help="Brooklet stream directory")
    parser.add_argument(
        "--group",
        default=PIPELINE_GROUP,
        help=f"Consumer group name (default: {PIPELINE_GROUP})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Print each enriched commit"
    )
    args = parser.parse_args()
    return run_pipeline(args.stream_dir, args.group, args.verbose)


if __name__ == "__main__":
    sys.exit(main())
