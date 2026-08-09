# brooklet watch: gapless resume demo

*2026-04-11T01:33:34Z by Showboat 0.6.1*
<!-- showboat-id: 10d4ed63-3494-4903-8aff-74335458b12f -->

This demo proves brooklet watch's killer feature: **gapless resumability across restarts**. When you kill a watcher and relaunch it (or Claude Code Monitor's TaskStop kills the process and you relaunch later), brooklet picks up exactly where it left off — no replay, no missed events. `tail -f` structurally cannot do this: it offers "last N lines then follow" by default, or "follow from byte 0" with `tail -c +0 -f`, but never "resume from the last consumer position" — so across a restart you either miss events written during the gap or replay everything from the top.

This is an executable document. Run `showboat verify docs/demos/watch-gapless-resume.md` from the repo root to confirm the demo still matches the real behavior.

> **Note:** commands use the absolute path to `brooklet` in this repo's dev venv so the demo runs reproducibly from any cwd. If you have brooklet installed globally (`uv tool install brooklet`), you can substitute `brooklet` for the full path.

## Setup: fresh scratch stream

We start with an empty scratch directory and seed two events into a topic called `demo`. A local topic's events land in numbered segment files (`data-NNNN.jsonl`), starting at `data-0001.jsonl`.

```bash
rm -rf /tmp/brooklet-watch-demo && mkdir /tmp/brooklet-watch-demo && cd /tmp/brooklet-watch-demo && BROOKLET=/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet && echo '{"_ts":"2026-01-01T10:00:00Z","n":1,"msg":"first"}'  | $BROOKLET produce demo && echo '{"_ts":"2026-01-01T10:00:01Z","n":2,"msg":"second"}' | $BROOKLET produce demo && cat demo/data-0001.jsonl
```

```output
{"_ts": "2026-01-01T10:00:00Z", "n": 1, "msg": "first", "_seq": 1}
{"_ts": "2026-01-01T10:00:01Z", "n": 2, "msg": "second", "_seq": 2}
```

## First watch run: consume the seed events

Start `brooklet watch` in the background, let it catch up, then stop it. Notice both seed events are delivered as compact one-liners — `#N HH:MM:SS key=val` — with the time parsed from the pinned `_ts` field.

```python3
import subprocess, time
brooklet = "/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet"
p = subprocess.Popen(
    [brooklet, "watch", "demo", "--group", "resumer"],
    cwd="/tmp/brooklet-watch-demo",
)
time.sleep(1.5)
p.terminate()
p.wait(timeout=5)
```

```output
#1 10:00:00 n=1 msg=first
#2 10:00:01 n=2 msg=second
```

## The gap: produce more events while no watcher is running

This is where the demo diverges from `tail -f`. We now produce two more events after stopping the watcher. A plain tail-based approach would either miss these entirely (start-from-current-end) or replay the earlier ones too (start-from-beginning). Brooklet's byte-offset tracking does exactly what you want: skip what the watcher already saw, deliver only what is new.

```bash
cd /tmp/brooklet-watch-demo && BROOKLET=/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet && echo '{"_ts":"2026-01-01T10:00:02Z","n":3,"msg":"during gap"}' | $BROOKLET produce demo && echo '{"_ts":"2026-01-01T10:00:03Z","n":4,"msg":"still gap"}' | $BROOKLET produce demo && wc -l demo/data-0001.jsonl
```

```output
       4 demo/data-0001.jsonl
```

## Second watch run: resume from saved offset

Restart `brooklet watch` with the **same group name** (`resumer`). The output below shows only events 3 and 4 — the two that were produced during the gap. Events 1 and 2 are not replayed because brooklet's consumer-group offset file recorded how far the first watcher got before it died.

> **About the `#N` prefix:** it is the topic-monotonic `_seq` assigned at produce time, not a per-run counter — so the second run resumes at `#3` / `#4`, the true position in the topic. The payload fields (`n=3`, `n=4`) agree, confirming that brooklet skipped events 1–2. See `brooklet-a2c` for the reasoning behind making this prefix topic-monotonic.

```python3
import subprocess, time
brooklet = "/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet"
p = subprocess.Popen(
    [brooklet, "watch", "demo", "--group", "resumer"],
    cwd="/tmp/brooklet-watch-demo",
)
time.sleep(1.5)
p.terminate()
p.wait(timeout=5)
```

```output
#3 10:00:02 n=3 msg=during gap
#4 10:00:03 n=4 msg=still gap
```

## The offset file: how brooklet knows where to resume

Brooklet persists the consumer-group position as a byte offset into the topic's segment files. A local topic is registered as a glob over `data-NNNN.jsonl`, so the saved state has to name a segment as well as a position inside it. Both are packed into one integer as `segment_number * 10**18 + byte_offset`.

```bash
wc -c /tmp/brooklet-watch-demo/demo/data-0001.jsonl && cat /tmp/brooklet-watch-demo/.brooklet/offsets/resumer-demo.json && echo
```

```output
     278 /tmp/brooklet-watch-demo/demo/data-0001.jsonl
{"offset": 1000000000000000278}
```

Unpacked, `1000000000000000278` is segment **1**, byte **278** — and 278 is exactly the size of `data-0001.jsonl`, so the consumer is caught up to the end. Any events appended later will be delivered on the next `brooklet watch` — not earlier, not later, not twice.

The file name `resumer-demo.json` is the group and topic joined by `-`. Each field is percent-escaped first (`/` becomes `%2F`, `-` becomes `%2D`), so a path-style topic like `scout/stats` lands in a flat file and no two distinct group/topic pairs can share one offset file.

## Takeaway

Same group name, different process, zero replay. This is the property that distinguishes `brooklet watch` from `tail -f` and is the primary reason to pair it with Claude Code's Monitor tool — so that `TaskStop` followed by relaunch gives you a gapless stream of events.

To prove the demo still reflects reality, run `showboat verify docs/demos/watch-gapless-resume.md` from the repo root. Verify re-executes every code block and diffs the captured output. A zero exit code means the demo is still accurate; a non-zero exit code with a diff means the implementation has drifted from this document and one of them needs updating.
