# ABOUTME: CI health check that consumes pytest summary events from brooklet
# ABOUTME: Demonstrates piping brooklet-pytest --output into a downstream consumer

"""CI health check — consumes pytest/summaries topic and gates on test health.

Usage:
    # Generate report logs from your test suite
    pytest --report-log=reports/run-001.jsonl
    pytest --report-log=reports/run-002.jsonl

    # Produce summaries to a brooklet topic
    brooklet-pytest "reports/run-*.jsonl" --glob --output pytest/summaries

    # Run the health check against the summaries
    uv run examples/ci_health_check.py reports/

Exit codes:
    0 — all checks pass
    1 — failures detected or performance regression
"""

import sys

import brooklet

SLOW_TEST_THRESHOLD_S = 5.0


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: ci_health_check.py <stream-dir>", file=sys.stderr)
        return 1

    stream_dir = sys.argv[1]
    stream = brooklet.open(stream_dir)

    # Consume the summaries topic produced by brooklet-pytest --output
    try:
        summaries = list(stream.consume("pytest/summaries", group="ci-health-check"))
    except KeyError:
        print("No pytest/summaries topic found. Run brooklet-pytest --output first.")
        return 1

    if not summaries:
        print("No new summaries to check (already consumed).")
        return 0

    print(f"Checking {len(summaries)} run(s)...\n")

    issues = []

    for summary in summaries:
        run_id = summary.get("run_id", "unknown")
        failed = summary.get("failed", 0)
        errored = summary.get("errored", 0)
        total = summary.get("total", 0)
        slowest = summary.get("slowest", [])

        # Check 1: Any failures?
        if failed > 0 or errored > 0:
            issues.append(f"  FAIL: {run_id} — {failed} failed, {errored} errored out of {total}")
            for f in summary.get("failures", []):
                issues.append(f"        {f['nodeid']}: {f.get('longrepr', '')[:80]}")

        # Check 2: Any tests exceeding the slow threshold?
        slow_tests = [t for t in slowest if t.get("duration", 0) > SLOW_TEST_THRESHOLD_S]
        if slow_tests:
            for t in slow_tests:
                issues.append(
                    f"  SLOW: {t['nodeid']} took {t['duration']:.1f}s "
                    f"(threshold: {SLOW_TEST_THRESHOLD_S}s)"
                )

        # Summary line
        status = "PASS" if (failed == 0 and errored == 0) else "FAIL"
        print(f"  [{status}] {run_id}: {total} tests, {failed} failed, {errored} errored")

    print()

    if issues:
        print("Issues found:")
        for issue in issues:
            print(issue)
        return 1

    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
