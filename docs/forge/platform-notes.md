# Forge platform notes (archived)

Brooklet's tracker and remote are on **GitHub**. This file is the research
that was gathered while the project trialled Forge (smol.ai), kept because it
was expensive to establish and stays useful if the platform is revisited.

Everything below was accurate as of the dates it cites and is **not**
maintained. Nothing here describes current practice.

---

## Forge Platform Notes

**Issues live on Forge; the git remote points at GitHub.** These are two
separate things, and conflating them has caused a wrong "the tracker is empty"
conclusion before. Verified 2026-08-23:

```
$ git remote -v
origin  https://github.com/JoshuaOliphant/brooklet.git (fetch)
origin  https://github.com/JoshuaOliphant/brooklet.git (push)
```

There is **one** remote, and it is GitHub. There is no `forge` or `github`
remote. So `git push origin main` pushes code to GitHub, and the two-remote
mirroring described in earlier revisions of this file does not apply to this
clone — `git push github main` fails with "does not appear to be a git
repository".

Code and issues are therefore split across hosts:

| what | where | how |
| --- | --- | --- |
| code | GitHub, via `origin` | `git push origin main` |
| issues | Forge, `joshua-oliphant/brooklet` | `python3 scripts/forge_issue.py` |

`scripts/forge_issue.py` does not read `origin`. It reads `git config
forge.repo`, so it keeps working regardless of where the remote points. If it
prints "No Forge repo configured", that is the missing config, **not** an empty
tracker:

```bash
git config forge.repo joshua-oliphant/brooklet
```

Note that `sf` does infer its repository from `origin`. With `origin` on GitHub,
bare `sf` commands no longer resolve to the Forge repo and need an explicit
repository argument.

To restore the documented two-remote arrangement, add Forge as the primary and
demote GitHub to a named mirror:

```bash
git remote rename origin github
git remote add origin https://forge.smol.ai/joshua-oliphant/brooklet.git
```

Then push both; mirroring is manual. Automating it as a Forge action has to wait
until Forge Actions actually execute steps rather than simulating them (see
below); until then a Forge-side sync job would report success without pushing
anything.

Two sharp edges worth knowing:

- Forge creates **server-side commits on `main`** without being asked. It added
  an "Add MIT license" commit that rewrote the existing `LICENSE`, replacing the
  copyright holder with the Forge username. Check `git log origin/main` after
  the first push and reconcile before assuming the hosts match.
- Forge reads workflows from `.github/workflows/` — the same directory GitHub
  uses — and honours its own extra keys there. Definitions are only re-read on a
  push to the **default branch**, though existing definitions fire on a push to
  any branch.

### Forge Actions: what actually runs

`runner` accepts exactly `worker` and `container`, and **`worker` is the
default**. The `worker` runner *simulates* steps: it never executes your
commands, string-matches them to emit canned output, and fails randomly about
10% of the time. That is why an `uv sync` and a 478-test suite each "passed" in
under a second, and why a docs-only push once reported `publish` as failed.
**A green Forge Actions run on the default runner is not evidence that anything
ran.** GitHub Actions remains the real CI.

Forge silently drops `strategy`/`matrix`, `env`, `if`, `timeout-minutes` and
`runs-on`, which is why a two-version matrix collapses to one job. Of the
trigger events it only recognises `push`, `pull_request`, `workflow_dispatch`
and `schedule` — and `schedule` is limited to an operator allowlist, so cron
never fires here. `push.branches` works; `push.paths` is parsed and then
ignored; `branches-ignore` is not parsed at all, which makes it *worse* than
omitting it.

**`release` is not a recognised event.** A workflow whose events Forge
recognises none of falls back to push-on-default-branch — which is exactly why
`publish.yml` ran on every push to main. The fix is to declare an event Forge
does know: `publish.yml` now also lists `workflow_dispatch`, which GitHub treats
as a harmless added manual trigger. Keep that in mind before adding any
release-triggered workflow.

The `container` runner is genuinely real — Forge's own repo (`swyx/forge`) runs
its CI on it, with jobs taking one to three minutes and `npm ci` alone taking
~28s. Our workflows get the fake runner purely because they never declare
`runner: container`.

But opting in would not help brooklet yet, because the container is Node-shaped:

- `packages/runner/Dockerfile` pins `docker.io/cloudflare/sandbox:0.12.3`, the
  **default** image variant, which ships no Python interpreter. Upstream also
  builds a `python` variant (CPython 3.11.14), which Forge does not use — and
  3.11 is below this project's 3.12 floor anyway.
- `RUNNER_ALLOWED_HOSTS` contains no `pypi.org`, `files.pythonhosted.org` or
  `astral.sh`, and there is no per-workflow field to add a host. `github.com` *is*
  allowed, so bootstrapping an interpreter might work, but `uv sync` still cannot
  reach the package index.

Container jobs also reject `uses:` steps and never receive Action secrets. So
Python CI on Forge waits on two small upstream changes (a different image tag and
a wider egress allowlist), not on anything in this repository.

Re-checked 2026-08-20 against `@smolai/forge` 0.4.0 GA: still blocked. The
container CI docs describe the standard image as carrying "pinned pnpm and Bun
toolchains" and nothing else, and list `uses:` steps among the cases that "fail
closed". One restriction was added rather than lifted — container workflows
"currently accept public repositories only". GitHub Actions remains the only CI
here that proves anything ran.

Be aware Forge's own workflows are written against GitHub semantics Forge does
not fully implement: `ci.yml` uses `branches-ignore`, which Forge does not parse,
so it fires on every branch, and `git-ingest-contracts.yml` uses `push.paths`,
which Forge parses and ignores.

The Forge **Wiki** is still unavailable to this account. Re-probed 2026-08-20
with the git-credential token, the write endpoints remain allowlisted:

    POST /api/repos/joshua-oliphant/brooklet/wiki/builds -> 403 "Forge Wiki is not enabled for this account"
    POST /api/repos/joshua-oliphant/brooklet/wiki/ask    -> 403 (same)
    GET  /api/repos/joshua-oliphant/brooklet/wiki/search-index -> 404 "Wiki has not been generated"

`POST .../wiki/builds` does exist and is the enable-and-build call, even though it
is absent from `llms.txt`. Read endpoints are reachable; the gate is on writes.

Do not read `GET /api/repos/:owner/:repo/wiki` as an access signal. It returns
500 for this repository *and* for `swyx/forge`, which has a working wiki — the
500 is a server-side bug on that route, not an account verdict. Probing it first
led to a wrong "the allowlist lifted" conclusion on 2026-08-20. **Test access with
`POST .../wiki/builds` and read the error body**, which is specific and honest.

Wiki pages are model-derived from the source, never hand-authored; the only way to
steer them is a committed `.forge/wiki.json`. Docs add that only repository writers
and admins may start builds, capped at 100 manual builds per user per UTC day.

The contract now documents a stateless Streamable HTTP MCP server at
`POST /mcp/wiki`, with tools `read_wiki_structure`, `read_wiki_contents` and
`ask_question`. Public read tools need no token; `ask_question` needs a PAT
carrying the `wiki:ask` scope. It would be the most directly useful thing Forge
has added for agent work here, but it is unreachable until the account is taken
off the Wiki allowlist — `/wiki/ask` returns the same 403 as `/wiki/builds`.

`sf content` is the hand-authored counterpart: immutable Markdown drafts,
anchored review threads, atomic publish, and pointer rollback to a prior
release. It has not been exercised here. Note that `sf` keeps its own keychain
credential separate from the git-credential token: `sf auth status` can report
`authenticated: false` while `scripts/forge_issue.py` and direct API calls work
fine. Run `sf auth login` before assuming the CLI is broken.

### CLI surface

`sf` 0.4.0 GA carries 74 commands, up from 39 in 0.4.0-preview.0. Nothing was
removed, and none of these groups existed when this project moved to Forge:

- `sf pr create|list|view` — pull requests without leaving the terminal.
- `sf agent create|message|events|approve|cancel|profiles` — durable
  repository-agent threads at an explicit execution tier, with an approval gate.
- `sf release enqueue|status|watch` — submit an exact feature SHA to the
  protected production merge queue.
- `sf content …` — the Markdown publishing pipeline described above.
- `sf benchmark` — plan or run an exact-tree Forge/GitHub comparison.

`sf gist import` also still ships, but the Gists API was deleted from the
platform and `GET /api/snippets/:id` replaced it. The command is dead; do not
build on it.

Nothing in this list is used by this repository yet.

Forge is alpha and changes often. Check for platform drift with:

```bash
python3 scripts/forge_check_updates.py          # diff live contract vs docs/forge/ snapshots
python3 scripts/forge_check_updates.py --update # accept a new baseline, then commit it
```

Claude Code session transcripts can be attached to Forge commits via
`.claude/hooks/smolforge-transcript.py`. It is **off by default**, because
transcripts on a public Forge repository are readable without authentication.

```bash
python3 .claude/hooks/smolforge-transcript.py --dry-run   # inspect what would be sent
git config --bool forge.transcripts.enabled true          # opt in
```

Only human-readable `text` blocks are published; internal reasoning and tool
output are excluded, because tool output routinely contains file contents.

