# Issue tracker: Forge

Issues and specs for this repo live on **Forge**, at
https://forge.smol.ai/joshua-oliphant/brooklet. They are **not** on GitHub —
`JoshuaOliphant/brooklet` on GitHub has zero issues and is a code mirror only.

All operations go through `python3 scripts/forge_issue.py`, which wraps the Forge
REST API. Do not use `gh issue` for this repo.

## Prerequisites

The script needs two things per machine:

```bash
sf auth git-credential joshua-oliphant/brooklet   # installs the API token
git config forge.repo joshua-oliphant/brooklet    # names the repo
```

Without `forge.repo` the script exits with "No Forge repo configured" — that is a
setup gap, not evidence that the tracker is empty.

## Conventions

- **Create an issue**: `python3 scripts/forge_issue.py create --title "..." --body "..." --label type:bug --label P2`. Use a heredoc for multi-line bodies.
- **Read an issue**: `python3 scripts/forge_issue.py show <number>`
- **List issues**: `python3 scripts/forge_issue.py list` (open) or `--state closed`.
- **Comment**: `python3 scripts/forge_issue.py comment <number> --body "..."`
- **Close**: `python3 scripts/forge_issue.py close <number> --reason "fixed in abc1234"`
- **Labels**: `python3 scripts/forge_issue.py labels`

### Known defect in the wrapper

`--state all` is broken. `cmd_list` at `scripts/forge_issue.py:113` drops the
query string for `all`, but the bare `GET /api/repos/:slug/issues` endpoint
returns **open issues only**. So `list --state all` silently omits every closed
issue. To see closed work, run `--state closed` as a separate call. Do not read an
empty or short `--state all` result as "there are no issues".

### Label rules

Forge has no type or priority field, so labels carry that meaning. Every issue
gets exactly one `type:` label (`type:bug`, `type:feature`, `type:task`) and one
priority label (`P1`–`P4`).

### No dependency model

Forge issues cannot express blocking relationships. When ordering matters, write
it into the body as a `Blocked by: #7` line near the top. Nothing enforces it.

### Bodies must stand alone

An issue body is read by someone with no session history. Cite concrete
`file.py:line` references so it can be acted on cold.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

## When a skill says "publish to the issue tracker"

Create a Forge issue with `scripts/forge_issue.py create`, including its `type:`
and priority labels.

## When a skill says "fetch the relevant ticket"

Run `python3 scripts/forge_issue.py show <number>`.

## Wayfinding operations

Used by `/wayfinder`. Forge supports neither sub-issues nor native dependencies,
so both are represented in issue bodies.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes /
  Decisions-so-far / Fog body.
- **Child ticket**: a normal issue with `Part of #<map>` as the first line of the
  body, and a matching entry in a task list in the map's body. Type goes in a
  `wayfinder:<type>` label (`research`/`prototype`/`grilling`/`task`).
- **Blocking**: a `Blocked by: #<n>, #<n>` line at the top of the child body. A
  ticket is unblocked when every issue it names is closed.
- **Frontier query**: `list` the open issues, keep those whose body names the map,
  drop any with an unclosed blocker or a `Claimed by:` line; first in map order wins.
- **Claim**: Forge has no assignee field exposed by the wrapper — comment
  `Claimed by: <who>` on the ticket as the session's first write.
- **Resolve**: `comment` the answer, then `close`, then append a context pointer
  to the map's Decisions-so-far.
