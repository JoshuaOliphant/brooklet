#!/bin/bash
# Guardrail hook: blocks edits to sensitive files and destructive git commands.
# Runs as a PreToolUse hook — exit 2 blocks the action, exit 0 allows it.

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# Block editing sensitive files
if [ "$TOOL" = "Edit" ] || [ "$TOOL" = "Write" ]; then
  if echo "$FILE" | grep -qE '\.env|secrets\.'; then
    echo "Blocked: cannot modify sensitive file $FILE" >&2
    exit 2
  fi
fi

# Block destructive git commands
if [ "$TOOL" = "Bash" ]; then
  if echo "$CMD" | grep -qE 'git (reset --hard|push --force|clean -f)'; then
    echo "Blocked: destructive git command not allowed" >&2
    exit 2
  fi
fi

exit 0
