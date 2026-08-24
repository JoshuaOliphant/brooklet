# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those
roles to the actual label strings used in this repo's issue tracker (Forge — see
`issue-tracker.md`).

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the
corresponding label string from this table.

## None of these exist on Forge yet

As of 2026-08-23 the Forge repo carries only `P1`–`P4`, `type:bug`,
`type:feature`, and `type:task`. Whether `scripts/forge_issue.py create --label`
auto-creates a missing label or errors on it is **untested** — verify on the first
triage run before relying on it, and create the five by hand if it errors.

These are orthogonal to the `type:` and priority labels, which every issue still
needs. A triaged bug carries three labels: `type:bug`, a `P` level, and a triage
role.

Edit the right-hand column to match whatever vocabulary you actually use.
