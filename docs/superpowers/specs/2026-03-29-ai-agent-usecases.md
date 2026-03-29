# AI Agent Use Cases for Brooklet

_Captured 2026-03-29 — ideas for agents that react to JSONL event streams._

## The Common Pattern

All use cases share the same shape:

```
[External JSONL source] → register() → consume(follow=True) → [Agent Logic] → produce() → [Next Stage]
```

Brooklet gives agents three things they need:
1. **Resumability** — byte offsets mean crash-and-restart is free
2. **Decoupling** — agents communicate through topics, not direct calls
3. **Zero infrastructure** — no Kafka, no Redis, just files on disk

---

## 1. CI/CD Failure Triage Agent

**Status:** Exploring — see `2026-03-29-ci-triage-agent-design.md`

An agent consumes pytest-reportlog JSONL (already supported via `brooklet pytest scan`). When tests fail, the agent:
- Consumes failure events (test name, traceback, duration)
- Reads the relevant source files and recent git diff
- Produces a `triage/findings` topic with root-cause hypotheses and suggested fixes
- A second agent (or human) consumes `triage/findings` to act on them

**Why brooklet fits:** Glob mode watches for new report files as CI runs land. Consumer groups let a triage agent and a metrics agent each track their own position independently.

## 2. Code Review Preparation Agent

Register a topic pointing at a JSONL log of PR events (commits pushed, files changed). An agent in follow mode:
- Reacts to each new commit by reading the diff
- Checks for common issues (missing tests, security patterns, style violations)
- Produces structured review comments to a `reviews/pending` topic
- A downstream agent or integration posts them to GitHub

**Why brooklet fits:** The agent resumes from its last offset after restart — no duplicate reviews. Multiple reviewers (security agent, style agent) each get their own consumer group.

## 3. Log Anomaly Detection + Auto-Remediation

Register application log files (structured JSONL) as external sources. An agent tails them:
- Detects anomaly patterns (error rate spikes, new exception types, latency jumps)
- Produces alerts to an `alerts/anomalies` topic
- A remediation agent consumes alerts and takes action: restarts services, scales resources, opens incidents
- Produces an `actions/taken` audit trail topic

**Why brooklet fits:** This is a **pipeline of agents**, each consuming one topic and producing to the next. Brooklet's consumer groups let you replay the full chain from any point.

## 4. Session Analytics → Coaching Agent

The existing `claude_analytics` adapter already extracts token usage, tool calls, and model info from Claude Code sessions. Layer an agent on top:
- Consumes the `scout/stats` topic
- Identifies patterns: excessive token burn, underused tools, repeated failed tool calls
- Produces coaching suggestions to a `scout/coaching` topic
- Could feed back into a Claude Code session as context for the next task

**Why brooklet fits:** The analytics pipeline already exists. The coaching agent is just another consumer group on the same topic.

## 5. Multi-Agent Research Pipeline

A coordinator agent produces research tasks to a `research/tasks` topic. Worker agents:
- Each consumes from `research/tasks` with their own consumer group (or a shared one for work distribution)
- Perform web searches, code analysis, document reading
- Produce findings to `research/results`
- The coordinator consumes results and synthesizes a final report

**Why brooklet fits:** JSONL files are the coordination layer — no broker to run. Agents can be restarted mid-pipeline and resume exactly where they left off.

## 6. Deployment Watchdog

Register deployment tool output (Kubernetes events, Terraform plan JSONL, deploy scripts that log structured output):
- An agent tails deployment events in follow mode
- Reacts to rollout failures, resource quota issues, or drift detection
- Produces remediation plans to a `deploy/actions` topic
- After human approval (consuming and acking), executes the fix

**Why brooklet fits:** External tools already produce the JSONL — brooklet just registers the files and adds consumer coordination. No changes to existing tooling.

## 7. File System Change Reactor

An agent watches a directory of config files or data drops:
- Register a glob pattern for incoming data files
- Agent consumes new records as files appear
- Transforms, validates, enriches the data
- Produces cleaned output to a local topic for downstream consumers

**Why brooklet fits:** Glob + follow mode with watchdog already handles new file detection. The agent doesn't need to implement any filesystem watching logic.
