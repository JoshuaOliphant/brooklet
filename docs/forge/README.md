# docs/forge — archive

Brooklet's issue tracker and remote are both **GitHub**. This directory is a
read-only archive from the period when the project trialled Forge (smol.ai).
Nothing here describes current practice; see `docs/agents/issue-tracker.md`.

| file | what it is |
| --- | --- |
| `beads-migration.md` | The beads → Forge triage: 43 issues judged, 14 carried forward, 3 already done, 25 discarded, with reasoning. Maps old `brooklet-xxx` ids to Forge numbers. |
| `platform-notes.md` | What was learned about the Forge platform — Actions semantics, the simulating runner, the wiki allowlist, the CLI surface. Accurate as of the dates it cites, unmaintained. |
| `llms-snapshot.txt`, `cli-commands-snapshot.txt`, `versions-snapshot.json` | Contract snapshots that `scripts/forge_check_updates.py` diffed against, to detect platform drift. |

The Forge backlog was **not** migrated — roughly 21 issues were open there at the
move. `scripts/forge_issue.py` is retained to export them and needs a Forge
token. Forge issue numbers are not GitHub issue numbers.
