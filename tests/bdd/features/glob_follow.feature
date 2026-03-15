# ABOUTME: BDD acceptance tests for glob+follow mode (AC-13 through AC-16)
# ABOUTME: Covers tailing existing files, detecting new files, offset persistence, and catch-up

Feature: Glob follow mode
  Consumer with mode="glob" and follow=True should catch up on existing
  files and then tail for new lines and new files matching the pattern.

  Background:
    Given a glob source directory

  @bdd @ac-13
  Scenario: Glob follow detects new lines in existing files
    Given the directory contains 2 JSONL files with 1 event each
    And a consumer following the glob source
    When new lines are appended to an existing file
    Then the consumer yields the new events
    And the consumer has seen 3 total events

  @bdd @ac-14
  Scenario: Glob follow detects new files matching the pattern
    Given the directory contains 2 JSONL files with 1 event each
    And a consumer following the glob source
    When a new JSONL file is created in the directory
    Then the consumer yields events from the new file
    And the consumer has seen 3 total events

  @bdd @ac-15
  Scenario: Glob follow maintains offset across restarts
    Given the directory contains 2 JSONL files with 1 event each
    And a glob consumer that has processed all files
    When the consumer is stopped and restarted with follow
    And new lines are appended to an existing file
    Then the consumer yields only the new events
    And the consumer has seen 1 total events

  @bdd @ac-16
  Scenario: Glob follow catches up before tailing
    Given the directory contains 3 JSONL files with 1 event each
    When a new consumer starts with follow mode
    Then all existing events are yielded first
    And the consumer has seen 3 total events
