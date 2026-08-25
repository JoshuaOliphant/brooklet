# Issue tracker: GitHub

Issues for this repo live on **GitHub**, at
https://github.com/JoshuaOliphant/brooklet/issues.

Use the `gh` CLI, or the GitHub MCP tools when `gh` is unavailable (it is absent
in Claude Code web/remote containers, which have the MCP tools instead).

```bash
gh issue list                                  # open work
gh issue list --state all
gh issue view 23
gh issue create --title "..." --body "..." --label bug
gh issue comment 23 --body "..."
gh issue close 23 --comment "fixed in abc1234"
gh label list
```

## Conventions

### Labels and priority

This repo carries GitHub's default label set (`bug`, `enhancement`,
`documentation`, `good first issue`, …). It has **no priority labels and no
`type:` labels** — those were a Forge-era convention and did not come across.

So: label the kind, and put priority in the body as a `**Priority:** P2` line
near the top. If you would rather have real priority labels, create `P1`–`P4`
once with `gh label create` and update this file.

### Ordering and structure

Unlike the previous tracker, GitHub does model relationships. Use them:

- `Blocked by #7` in the body — GitHub renders it as a cross-reference.
- Task lists (`- [ ] #12`) for breakdown.
- Sub-issues where a parent genuinely has children.

Nothing enforces blocking; it is still a reading convention, but the links are
live.

### Bodies must stand alone

An issue body is read by someone with no session history. Cite concrete
`file.py:line` references so it can be acted on cold.

### Open before working

Open an issue before starting non-trivial work, and cite its number in the
commit message.

## The un-migrated Forge backlog

The tracker moved beads → Forge → GitHub. The Forge issues were **not**
migrated: roughly 21 (numbered #1–#21 there) were open at the move.

**Forge issue numbers are not GitHub issue numbers.** GitHub's numbering starts
from this repo's own history and already uses that range for unrelated PRs and
issues — #22 is a closed PR, #23–#26 are current issues. Never resolve an old
`#12` reference against GitHub without checking what it actually points at.

`scripts/forge_issue.py` is retained only to export that backlog. It needs a
Forge token:

```bash
sf auth git-credential joshua-oliphant/brooklet
git config forge.repo joshua-oliphant/brooklet
python3 scripts/forge_issue.py list
```

Note a known defect in that wrapper: `--state all` is broken (`cmd_list` at
`scripts/forge_issue.py:113` drops the query string, and the bare endpoint
returns open issues only), so run `--state closed` separately. Do not read a
short `--state all` result as "there are no issues".

Older `brooklet-xxx` ids are beads ids; `docs/forge/beads-migration.md` maps
them to Forge numbers.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

## When a skill says "publish to the issue tracker"

Create a GitHub issue with `gh issue create`, labelled by kind, with a
`**Priority:**` line in the body.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number>`.

## Wayfinding operations

Used by `/wayfinder`.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes /
  Decisions-so-far / Fog body.
- **Child ticket**: a normal issue linked from a task list in the map's body, or
  a real sub-issue. Put `Part of #<map>` as the first line. Type goes in a
  `wayfinder:<type>` label (`research`/`prototype`/`grilling`/`task`).
- **Blocking**: a `Blocked by: #<n>, #<n>` line at the top of the child body. A
  ticket is unblocked when every issue it names is closed.
- **Frontier query**: `gh issue list` the open issues, keep those whose body
  names the map, drop any with an unclosed blocker or a `Claimed by:` line;
  first in map order wins.
- **Claim**: assign yourself (`gh issue edit <n> --add-assignee @me`) — GitHub
  has a real assignee field, so a `Claimed by:` comment is no longer needed.
- **Resolve**: comment the answer, then close, then append a context pointer to
  the map's Decisions-so-far.

The `wayfinder:*` labels do not exist yet; create them on first use.
