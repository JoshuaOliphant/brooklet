#!/bin/bash
# Guardrail hook: blocks edits to sensitive files and destructive git commands.
# Runs as a PreToolUse hook — exit 2 blocks the action, exit 0 allows it.
# Fail-closed: if we can't parse input, block the action.
set -euo pipefail

# Validate jq is available
if ! command -v jq &>/dev/null; then
  echo "GUARDRAIL ERROR: jq not installed, blocking action for safety" >&2
  exit 2
fi

INPUT=$(cat)

# Parse fields — fail closed on parse errors
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty') || { echo "GUARDRAIL ERROR: failed to parse tool_name" >&2; exit 2; }
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty') || true
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty') || true

# Block editing sensitive files (anchored to path boundaries)
if [ "$TOOL" = "Edit" ] || [ "$TOOL" = "Write" ]; then
  if echo "$FILE" | grep -qE '(^|/)\.env($|\.)|secrets\.'; then
    echo "Blocked: cannot modify sensitive file $FILE" >&2
    exit 2
  fi
fi

# Block destructive git commands (short and long flags)
if [ "$TOOL" = "Bash" ]; then
  if echo "$CMD" | grep -qE 'git\s+(reset\s+--hard|push\s+(--force|-f)|clean\s+-[a-zA-Z]*f)'; then
    echo "Blocked: destructive git command not allowed" >&2
    exit 2
  fi
fi

exit 0
