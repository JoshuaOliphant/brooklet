# brooklet watch: gapless resume demo

*2026-04-11T01:33:34Z by Showboat 0.6.1*
<!-- showboat-id: 10d4ed63-3494-4903-8aff-74335458b12f -->

This demo proves brooklet watch's killer feature: **gapless resumability across restarts**. When you kill a watcher and relaunch it (or Claude Code Monitor's TaskStop kills the process and you relaunch later), brooklet picks up exactly where it left off — no replay, no missed events. `tail -f` structurally cannot do this: it offers start-from-current-end (miss the gap) or start-from-beginning (replay everything), never "resume from the last consumer position".

This is an executable document. Run `showboat verify docs/demos/watch-gapless-resume.md` from the repo root to confirm the demo still matches the real behavior.

> **Note:** commands use the absolute path to `brooklet` in this repo's dev venv so the demo runs reproducibly from any cwd. If you have brooklet installed globally (`uv tool install brooklet`), you can substitute `brooklet` for the full path.

## Setup: fresh scratch stream

We start with an empty scratch directory and seed two events into a topic called `demo`.

```bash
rm -rf /tmp/brooklet-watch-demo && mkdir /tmp/brooklet-watch-demo && cd /tmp/brooklet-watch-demo && BROOKLET=/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet && echo '{"_ts":"2026-01-01T10:00:00Z","n":1,"msg":"first"}'  | $BROOKLET produce demo && echo '{"_ts":"2026-01-01T10:00:01Z","n":2,"msg":"second"}' | $BROOKLET produce demo && cat demo/data.jsonl
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
cd /tmp/brooklet-watch-demo && BROOKLET=/Users/joshuaoliphant/Library/CloudStorage/Dropbox/python_workspace/brooklet/.venv/bin/brooklet && echo '{"_ts":"2026-01-01T10:00:02Z","n":3,"msg":"during gap"}' | $BROOKLET produce demo && echo '{"_ts":"2026-01-01T10:00:03Z","n":4,"msg":"still gap"}' | $BROOKLET produce demo && wc -l demo/data.jsonl
```

```output
       4 demo/data.jsonl
```

## Second watch run: resume from saved offset

Restart `brooklet watch` with the **same group name** (`resumer`). The output below shows only events 3 and 4 — the two that were produced during the gap. Events 1 and 2 are not replayed because brooklet's consumer-group offset file recorded how far the first watcher got before it died.

> **About the `#N` prefix:** it is a per-consumer-run counter that restarts at 1 on every Consumer instance, not a topic-wide monotonic sequence. The payload fields (`n=3`, `n=4`) are the authoritative markers showing that brooklet skipped events 1–2. See `brooklet-a2c` for a discussion of making this prefix topic-monotonic instead.

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
#1 10:00:02 n=3 msg=during gap
#2 10:00:03 n=4 msg=still gap
```

## The offset file: how brooklet knows where to resume

Brooklet persists the consumer-group position as a byte offset into the topic's data file. Here is the saved state after the second run — a single integer representing how many bytes of `demo/data.jsonl` the `resumer` group has consumed.

```bash
wc -c /tmp/brooklet-watch-demo/demo/data.jsonl && cat /tmp/brooklet-watch-demo/.brooklet/offsets/resumer-demo.json && echo
```

```output
     278 /tmp/brooklet-watch-demo/demo/data.jsonl
{"offset": 278}
```

Saved offset (**278**) equals the file size (**278**), which means the consumer is caught up to the end. Any events appended later will be delivered on the next `brooklet watch` — not earlier, not later, not twice.

## Takeaway

Same group name, different process, zero replay. This is the property that distinguishes `brooklet watch` from `tail -f` and is the primary reason to pair it with Claude Code's Monitor tool — so that `TaskStop` followed by relaunch gives you a gapless stream of events.

To prove the demo still reflects reality, run `showboat verify docs/demos/watch-gapless-resume.md` from the repo root. Verify re-executes every code block and diffs the captured output. A zero exit code means the demo is still accurate; a non-zero exit code with a diff means the implementation has drifted from this document and one of them needs updating.
