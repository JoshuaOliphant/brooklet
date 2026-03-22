# ABOUTME: BDD feature file for pytest adapter — registration, consumption, stats
# ABOUTME: Generated from acceptance criteria AC-PT-1 through AC-PT-6

Feature: pytest report log adapter
    Consume pytest-reportlog JSONL through brooklet's API,
    filter to test results, track offsets, and produce run summaries.

    Background:
        Given a stream opened at an empty directory

    @ac-pt-1
    Scenario: Register a single pytest report log
        Given a pytest report log at "results.jsonl" with 5 test events
        When the user registers it as "pytest/results" in single-file mode
        Then "pytest/results" appears in the stream topics list
        And consuming "pytest/results" yields test events with envelope fields

    @ac-pt-2
    Scenario: Register a directory of report logs
        Given 3 pytest report log files in the stream directory
        When the user registers them as "pytest/history" in glob mode
        Then consuming "pytest/history" yields events from all 3 files

    @ac-pt-3
    Scenario: Filter to test outcomes only
        Given a pytest report log with mixed event types
        When the consumer filters with is_test_result
        Then only TestReport call events are yielded

    @ac-pt-4
    Scenario: Incremental consumption across runs
        Given 2 pytest report log files registered as a glob topic
        And a consumer has read all events from the glob topic
        When a new run-003 file appears
        And the consumer reads the glob topic again
        Then only events from the new file are yielded

    @ac-pt-5
    Scenario: Appended events are consumable
        Given a pytest report log registered in single-file mode
        When new test events are appended to the report log
        Then the appended events are consumable

    @ac-pt-6
    Scenario: Summary stats per run
        Given a pytest report log with 3 passed 1 failed and 1 skipped test
        When aggregate_run processes the events
        Then the RunStats contains total 5 passed 3 failed 1 skipped 1
        And the slowest list has 5 entries sorted by duration descending
        And the failures list contains the failed test with longrepr
