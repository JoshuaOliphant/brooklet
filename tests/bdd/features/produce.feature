# ABOUTME: BDD feature file for stream.produce() — the brooklet write path
# ABOUTME: Generated from acceptance criteria AC-1 through AC-12 in specs/produce-and-scout.md

Feature: Stream produce
    Produce events to local topics with envelope auto-injection,
    auto-registration in the unified namespace, and path-style nesting.

    Background:
        Given a stream opened at an empty directory

    @ac-1
    Scenario: Produce creates topic directory and writes event
        When the user produces an event to topic "my-topic" with type "hello"
        Then a directory "my-topic" exists inside the stream directory
        And a file "my-topic/data.jsonl" exists inside the stream directory
        And the file "my-topic/data.jsonl" contains exactly 1 JSON line
        And the JSON line contains the original payload field type "hello"

    @ac-2
    Scenario: Produce auto-injects envelope fields
        When the user produces an event to topic "events" with type "hello" and source "test-src"
        Then the written event contains a valid ISO 8601 timestamp in _ts
        And the written event contains _seq set to 1
        And the written event contains _src set to "test-src"

    @ac-3
    Scenario: Produce assigns monotonic sequence numbers
        Given 3 events have been produced to topic "counter"
        When the user produces an event to topic "counter" with type "fourth"
        Then the last written event has _seq set to 4

    @ac-4
    Scenario: Produce preserves existing envelope fields
        When the user produces an event with _ts "2026-01-01T00:00:00Z" to topic "preserve"
        Then the written event contains _ts "2026-01-01T00:00:00Z"
        And the written event contains _seq set to 1

    @ac-5
    Scenario: Produce appends atomically
        Given 2 events have been produced to topic "append-test"
        When the user produces an event to topic "append-test" with type "third"
        Then the file "append-test/data.jsonl" contains exactly 3 JSON lines
        And the first 2 lines are unchanged

    @ac-6
    Scenario: Produce auto-registers topic in the registry
        When the user produces an event to topic "scout/stats" with type "metric"
        Then "scout/stats" appears in the stream topics list
        And consuming "scout/stats" with group "test" yields the produced event

    @ac-7
    Scenario: Produce and consume roundtrip
        Given 3 events have been produced to topic "output"
        When a consumer reads from "output" with group "reader"
        Then all 3 events are yielded with correct envelope fields
        And consuming again from "output" with group "reader" yields 0 events

    @ac-8
    Scenario: Produce with source parameter
        When the user produces an event to topic "sourced" with type "test" and source "scout"
        Then the written event contains _src set to "scout"

    @ac-9
    Scenario: Produce errors on name collision with registered source
        Given an external source registered as "claude/history"
        When the user tries to produce to topic "claude/history"
        Then a ValueError is raised with message containing "already registered"

    @ac-10
    Scenario: Path-style topic names create nested directories
        When the user produces an event to topic "scout/session-stats" with type "nested"
        Then a directory "scout/session-stats" exists inside the stream directory
        And a file "scout/session-stats/data.jsonl" exists inside the stream directory

    @ac-11
    Scenario: Topic names with path traversal are rejected
        When the user tries to produce to topic "../escape"
        Then a ValueError is raised with message containing "path traversal"

    @ac-12
    Scenario: Producing non-dict data raises TypeError
        When the user tries to produce a string "not a dict" to topic "bad"
        Then a TypeError is raised
