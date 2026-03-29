# Agent Coordination via Brooklet Topics

_2026-03-29 — Design spec for multi-agent systems coordinated through JSONL event streams._

## Core Idea

Brooklet topics are the coordination protocol between agents. Each agent:
- **Consumes** from one or more input topics
- **Does real work** (writes code, runs commands, transforms data)
- **Produces** results to output topics
- **Produces errors** to error topics when things go wrong

No agent "just comments." Every agent fully completes its piece and hands off
a concrete artifact to the next stage via a topic.

## Architecture

```
┌─────────────┐    topic:     ┌─────────────┐    topic:      ┌─────────────┐
│   Detector  │───detected/───│  Analyzer   │───analyzed/────│  Executor   │
│             │   events      │             │   plans        │             │
└─────────────┘               └─────────────┘                └──────┬──────┘
                                                                    │
                                                    topic:          │
                                                    executed/       │
                                                    results         │
                                                                    ▼
                                                             ┌──────────────┐
                                                             │   Verifier   │
                                                             └──────┬───────┘
                                                                    │
                                              topic: verified/outcomes
                                              topic: errors/verify (on failure)
```

Each box is an independent agent process. Each arrow is a brooklet topic.
Agents don't know about each other — they only know their input and output topics.

## Agent Contract

Every agent follows the same shape:

```python
def agent_loop(stream, input_topic, output_topic, error_topic, group):
    for event in stream.consume(input_topic, group=group, follow=True):
        try:
            result = do_work(event)
            stream.produce(output_topic, result, source=group)
        except Exception as e:
            stream.produce(error_topic, {
                "original_event": event,
                "error": str(e),
                "agent": group,
                "stage": input_topic,
            }, source=group)
```

This is the entire coordination model. No RPC, no queues, no broker.

## Event Flow Conventions

### Namespace Convention

```
<pipeline>/<stage>       — happy path output
<pipeline>/errors/<stage> — errors from that stage
<pipeline>/dead-letter    — events that failed recovery
```

Example for a test-fix pipeline:

```
testfix/detected          — test failures found
testfix/analyzed          — root cause analysis complete
testfix/planned           — fix plan with file edits
testfix/executed          — fix applied (patch/commit)
testfix/verified          — fix confirmed (tests pass)
testfix/errors/analyzed   — analysis failures
testfix/errors/executed   — fix application failures
testfix/dead-letter       — gave up after retries
```

### Event Schema

Every event carries enough context for the next agent to work independently:

```json
{
  "_ts": "2026-03-29T10:00:00Z",
  "_seq": 1,
  "_src": "analyzer",
  "pipeline": "testfix",
  "stage": "analyzed",
  "correlation_id": "run-2026-03-29-001",
  "payload": { "...stage-specific data..." }
}
```

The `correlation_id` ties events across stages for a single unit of work.

## Error Handling & Recovery

Errors are just events. Recovery is just another agent.

```
errors/<stage> topic
       │
       ▼
┌──────────────┐
│  Recovery    │──→ retries original stage (re-produces to input topic)
│  Agent       │──→ or escalates to dead-letter
└──────────────┘
```

Recovery strategies (per stage):
- **Retry** — re-produce the original event to the input topic (with retry count)
- **Skip** — log and move on (produce to dead-letter with reason)
- **Escalate** — produce to a human-attention topic
- **Compensate** — undo partial work from a failed stage

```json
{
  "original_event": { "..." },
  "error": "patch failed to apply",
  "agent": "executor",
  "retry_count": 2,
  "max_retries": 3,
  "strategy": "retry"
}
```

## Concrete Pipeline: Test Fix

The first pipeline to build. Dogfoods brooklet in this repo.

### Stage 1: Detector

**Input:** pytest-reportlog JSONL (external source, already produced by `brooklet pytest scan`)
**Output:** `testfix/detected` — one event per failing test

```json
{
  "correlation_id": "run-2026-03-29-001",
  "test_id": "tests/test_consumer.py::test_follow_mode",
  "failure_type": "AssertionError",
  "traceback": "...",
  "duration": 0.23,
  "report_file": "reports/test-results.jsonl"
}
```

This stage already exists — it's `brooklet pytest scan` with minor reshaping.

### Stage 2: Analyzer

**Input:** `testfix/detected`
**Output:** `testfix/analyzed` — root cause + affected files

The analyzer reads the failing test, its traceback, and the relevant source code.
It produces a structured analysis:

```json
{
  "correlation_id": "run-2026-03-29-001",
  "test_id": "tests/test_consumer.py::test_follow_mode",
  "root_cause": "Off-by-one in byte offset calculation after file truncation",
  "affected_files": ["src/brooklet/consumer.py"],
  "affected_lines": {"src/brooklet/consumer.py": [142, 145]},
  "confidence": "high",
  "analysis": "The consumer resumes at saved byte offset but the file was truncated..."
}
```

This is the LLM-powered stage. It calls Claude with the test + source context.

### Stage 3: Planner

**Input:** `testfix/analyzed`
**Output:** `testfix/planned` — concrete edit plan

```json
{
  "correlation_id": "run-2026-03-29-001",
  "edits": [
    {
      "file": "src/brooklet/consumer.py",
      "old": "offset = self._saved_offset",
      "new": "offset = min(self._saved_offset, file_size)"
    }
  ],
  "test_command": "uv run pytest tests/test_consumer.py::test_follow_mode -v"
}
```

### Stage 4: Executor

**Input:** `testfix/planned`
**Output:** `testfix/executed` — applies the edits, runs the test

```json
{
  "correlation_id": "run-2026-03-29-001",
  "edits_applied": true,
  "patch": "--- a/src/brooklet/consumer.py\n+++ b/...",
  "branch": "fix/test-follow-mode-offset"
}
```

### Stage 5: Verifier

**Input:** `testfix/executed`
**Output:** `testfix/verified` — runs full test suite, confirms fix

```json
{
  "correlation_id": "run-2026-03-29-001",
  "tests_passed": true,
  "total": 47,
  "failed": 0,
  "regression": false
}
```

## What Brooklet Provides

| Need | Brooklet feature |
|------|-----------------|
| Agent-to-agent communication | Topics (produce/consume) |
| Crash recovery | Byte offset persistence per consumer group |
| Parallel agents | Multiple consumer groups on same topic |
| Pipeline replay | Reset offsets, re-consume from beginning |
| Audit trail | Every event persisted as JSONL |
| Error routing | Error topics are just more topics |
| Live tailing | `follow=True` with watchdog |
| No infrastructure | Files on disk, no broker |

## What Brooklet Does NOT Provide (Yet)

| Need | Current gap | Possible approach |
|------|------------|-------------------|
| Agent lifecycle | No process management | External (systemd, supervisord, or just scripts) |
| Work distribution | No competing consumers | Shared consumer group with locking (future) |
| Backpressure | Unbounded topics | Consumer lag monitoring (future) |
| Schema validation | Freeform JSON | Optional schema registry (future) |
| Timeout/SLA | No deadline tracking | Watchdog agent on event timestamps |

## Implementation Plan

### Phase 1: Agent harness

A thin `brooklet.agent` module that codifies the agent contract:
- Consume → work → produce loop
- Automatic error routing
- Correlation ID propagation
- Configurable retry policy

### Phase 2: Test-fix pipeline

Build the 5-stage pipeline above using the harness:
- Detector (wraps existing pytest adapter)
- Analyzer (LLM-powered)
- Planner (LLM-powered)
- Executor (applies edits, runs tests)
- Verifier (runs full suite)

### Phase 3: CLI integration

```bash
# Run a single agent
brooklet agent run analyzer --input testfix/detected --output testfix/analyzed

# Show pipeline status
brooklet agent status testfix

# Replay from a specific stage
brooklet agent replay testfix --from analyzed
```

### Phase 4: Generalize

Extract the pipeline patterns so users can define their own multi-agent
workflows with brooklet as the coordination layer.
