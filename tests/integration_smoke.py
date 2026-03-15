# ABOUTME: Smoke test — registers Claude Code history and consumes first 5 events
# ABOUTME: Verifies the full brooklet pipeline works end-to-end with real data

import brooklet


def main():
    s = brooklet.open("/tmp/brooklet-integration-test")
    s.register(
        "claude-history",
        path="/Users/joshuaoliphant/.claude/history.jsonl",
        mode="single-file",
    )

    for i, event in enumerate(s.consume("claude-history", group="smoke-test")):
        key_list = list(event)[:5]
        print(f"{event['_seq']}: {key_list}")
        if i >= 4:
            break

    print(f"Topics: {s.topics()}")


if __name__ == "__main__":
    main()
