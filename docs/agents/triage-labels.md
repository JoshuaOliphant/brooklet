# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those
roles to the actual label strings used in this repo's issue tracker (GitHub —
see `issue-tracker.md`).

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the
corresponding label string from this table.

## None of these exist on GitHub yet

The GitHub repo carries only the default label set (`bug`, `enhancement`,
`documentation`, `good first issue`, …). Create the five triage labels on first
use:

```bash
gh label create needs-triage --description "Maintainer needs to evaluate this issue"
```

Create the label before using it. Whether `gh issue create --label` errors on a
missing label or silently drops it is **unverified here** — either way the label
does not end up on the issue, so creating it first is the reliable path.

These are orthogonal to the kind label and the `**Priority:**` body line, which
every issue still needs — see `issue-tracker.md`. A triaged bug carries `bug`, a
triage role, and a priority line.

Edit the right-hand column to match whatever vocabulary you actually use.
