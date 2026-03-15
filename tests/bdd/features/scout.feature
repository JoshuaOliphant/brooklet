# ABOUTME: BDD acceptance tests for scout analytics module (AC-17 through AC-29)
# ABOUTME: Covers session scanning, token/tool reporting, output modes, and CLI

Feature: Scout analytics module
  Scout scans Claude Code session JSONL files and reports analytics
  (tokens, tools, models, session stats). It exercises brooklet consume()
  with glob mode and produce() for JSONL output.

  Background:
    Given a project directory with session JSONL files

  @bdd @ac-17
  Scenario: Scout scans sessions from a project path
    Given the project has 5 session files
    When scout scans the project directory
    Then scout processes all 5 sessions
    And cumulative statistics are reported

  @bdd @ac-18
  Scenario: Scout reports token usage
    Given the project has session files with assistant messages
    When scout scans the project directory
    Then the output includes total input tokens
    And the output includes total output tokens
    And the output includes total cache read tokens
    And the output includes total cache create tokens

  @bdd @ac-19
  Scenario: Scout reports tool call frequency
    Given the project has session files with tool_use content blocks
    When scout scans the project directory
    Then the output includes a ranked list of tool names by call count

  @bdd @ac-20
  Scenario: Scout reports session duration and event counts
    Given the project has session files with timestamped events
    When scout scans the project directory
    Then the output includes total sessions scanned
    And the output includes average events per session
    And the output includes average session duration

  @bdd @ac-21
  Scenario: Scout reports model usage breakdown
    Given the project has session files with different models
    When scout scans the project directory
    Then the output includes a count of sessions per model

  @bdd @ac-22
  Scenario: Scout re-scan includes new session files
    Given the project has 5 session files
    And scout has already processed those sessions
    When 2 new session files appear
    And scout scans the project directory again
    Then all 7 sessions are processed
    And cumulative stats reflect all 7 sessions

  @bdd @ac-23
  Scenario: Streaming log output
    Given the project has 3 session files
    When scout scans with streaming output
    Then each session prints a stats block
    And a cumulative totals block is printed

  @bdd @ac-24
  Scenario: Rich live table output
    Given the project has 3 session files
    When scout scans with rich output
    Then a rich table is rendered with session data

  @bdd @ac-25
  Scenario: Current session only mode
    Given the project has 5 session files
    When scout scans with the current flag
    Then only the most recently modified session is processed

  @bdd @ac-26
  Scenario: Current session with follow and rich
    Given the project has an active session file
    When scout identifies the current session
    Then only the most recently modified session is selected
    And the session can be consumed with follow mode

  @bdd @ac-27
  Scenario: Follow mode for new sessions
    Given the project has 2 session files
    When scout scans the project directory
    Then the batch results can be extended by follow mode

  @bdd @ac-28
  Scenario: JSONL output via produce
    Given the project has 3 session files
    When scout scans with output topic "scout/stats"
    Then each session's stats are produced to the topic
    And the events have _src set to "scout"

  @bdd @ac-29
  Scenario: Configurable project path
    Given a user with a specific project path
    When the user runs scout with that path argument
    Then scout scans sessions under that specific directory
