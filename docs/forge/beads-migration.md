# Beads → Forge issues migration

Brooklet's tracker moved from beads to Forge issues
(https://forge.smol.ai/joshua-oliphant/brooklet). `.beads/` is kept as a
read-only archive; nothing is created or updated there any more.

All 43 active beads issues (40 open + 3 in progress) were triaged against the
code on `main` before migrating, rather than copied across. Each was judged on
two questions: is it still true, and is it still worth doing. The outcome:

| outcome | count |
|---|---|
| carried forward as Forge issues | 14 |
| already done on `main` | 3 |
| discarded | 25 |
| the migration task itself | 1 |

Seven further issues (#1–#7) were opened from a parallel documentation audit and
have no beads ancestor.

## Carried forward

| beads | Forge | note |
|---|---|---|
| `brooklet-uoj` | #15 | narrowed: the produce path was already fixed, `register` still stores relative paths |
| `brooklet-6ob` | #16 | |
| `brooklet-38w` | #9 | absorbs `brooklet-9wf` (its CLI half) |
| `brooklet-7n6` | #10 | absorbs `brooklet-322` |
| `brooklet-i3u` | #11 | absorbs `brooklet-9lx` |
| `brooklet-t9m` | #17 | reframed: the real defect is in `envelope.wrap()`, not the formatter |
| `brooklet-3z2` | #18 | |
| `brooklet-3vz` | #19 | |
| `brooklet-0h4` | #12 | refiled as a bug — a producer exiting 0 having written nothing is a silent failure |
| `brooklet-5mg` | #13 | retitled to the one decision it blocks, dropping the benchmark suite |
| `brooklet-9ew` | #14 | absorbs the one leftover assertion from `brooklet-oq6` |
| `brooklet-jip` | #20 | |
| `brooklet-b32` | #21 | reframed from a coverage percentage to the public contract those lines implement |
| `brooklet-rvt` | #8 | absorbs `brooklet-57u` and `brooklet-hcx`; scoped as an external plugin repo |

## Already done on `main`

- `brooklet-a2c` — `_seq` is topic-monotonic, assigned at produce time and
  preserved on read behind a strict int-validity gate
  (`core/envelope.py:13-22,63-67`), recorded in DEC-015.
- `brooklet-f0f` — Typer already prints the whole resolution chain in `--help`
  (`cli/app.py:46-53`). The `.brooklet.toml` and git-root layers the issue wanted
  documented do not exist on `main`.
- `brooklet-oq6` — three of its four sub-items landed in
  `tests/test_otel.py:75-97`; the fourth moved to #14.

## Discarded, and why

Grouped by the reason they did not survive.

**Referenced code that does not exist on `main` (5).** `brooklet-85b`,
`brooklet-8q6`, `brooklet-1fu`, `brooklet-7lz` all target a config module —
`src/brooklet/config.py`, `find_config_file`, `_user_config_path`,
`tests/test_config.py` — that lives only on an unmerged branch. `brooklet-3sb`
and `brooklet-82g` were meta-tasks whose only deliverable was editing another
issue's notes field. Whether that config branch lands at all is now #7.

**Consciously deferred at design time and never missed (4).** `brooklet-cfq`,
`brooklet-cs0`, `brooklet-yh5` and `brooklet-0gb` are the leftover AC-7 through
AC-11 items from the pytest adapter, each listed under "Out of Scope" in
`docs/superpowers/specs/2026-03-22-pytest-adapter-design.md:185-190`. Two are also
unimplementable against the current stats schema, which keeps only failed nodeids
plus the top-five slowest tests.

**Duplicated something already shipped (7).** `brooklet-imz` and `brooklet-2m6`
(replay) are `brooklet cat` plus a one-line `_seq` filter for the read-only case,
and `seek --to-seq` for the rewind case. `brooklet-q1u` duplicates the lag
calculation now tracked in #10. `brooklet-0xm` duplicates the existing `produce`
span, which already yields per-event latency. `brooklet-9wf`, `brooklet-322` and
`brooklet-9lx` were thin CLI wrappers folded into their library halves.

**Premise did not hold (3).** `brooklet-qqc` assumed offset saves were on the
per-event hot path; they happen roughly twice a second at the default poll
interval. `brooklet-2gm` adds no-op methods for a call site that does not exist,
guarding a failure that would be immediate and loud. `brooklet-yjf` asked a
question the code already answers — the no-op classes stand in for an absent
optional dependency, not for brooklet behaviour, so the no-mock-mode rule does
not apply.

**Cost exceeded value (6).** `brooklet-9z6` would re-express 20 existing
`tests/test_watch.py` assertions as Gherkin with no coverage gain. `brooklet-93j`
would extract one contrib adapter while leaving two equally opinionated ones
built in. `brooklet-4xc` would enable real OTel exporters during tests, buying
nothing testable. `brooklet-57u` and `brooklet-hcx` were sub-steps of an adapter
with zero lines of code, folded into #8. `brooklet-cs0` also falls here on
scope grounds.

## Method

Five subagents triaged in parallel, one per theme, each required to verify claims
against the source rather than trust the issue text — several issues described
behaviour that had since changed, and one claimed a fix that had landed only on an
unmerged branch. Kept issues were rewritten to stand alone with `file.py:line`
citations, since the beads history they referenced is no longer the working
tracker.
