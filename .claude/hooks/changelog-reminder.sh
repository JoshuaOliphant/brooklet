#!/usr/bin/env bash
# ABOUTME: PreToolUse hook that reminds to update CHANGELOG.md before tagging a release
# ABOUTME: Triggers on git tag commands and version bumps in pyproject.toml

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')

if [ "$TOOL" = "Bash" ]; then
    CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')
    if echo "$CMD" | grep -qE 'git tag\s+v'; then
        echo "CHANGELOG REMINDER: You are creating a release tag. Before proceeding:"
        echo "  1. Run: git-cliff --github-repo JoshuaOliphant/brooklet -c cliff.toml -o CHANGELOG.md"
        echo "  2. Commit the updated CHANGELOG.md"
        echo "  3. Then create the tag"
    fi
elif [ "$TOOL" = "Edit" ] || [ "$TOOL" = "Write" ]; then
    FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
    if [ "$(basename "$FILE")" = "pyproject.toml" ]; then
        NEW=$(echo "$INPUT" | jq -r '.tool_input.new_string // .tool_input.content // empty')
        if echo "$NEW" | grep -qE 'version\s*=\s*"'; then
            echo "CHANGELOG REMINDER: Version is being bumped. Remember to update CHANGELOG.md before tagging:"
            echo "  git-cliff --github-repo JoshuaOliphant/brooklet -c cliff.toml -o CHANGELOG.md"
        fi
    fi
fi
