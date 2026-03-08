#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: scripts/agent_worktree_start.sh <agent-name> <branch-name> [base-branch]"
  exit 1
fi

AGENT_NAME="$1"
BRANCH_NAME="$2"
BASE_BRANCH="${3:-main}"
WORKTREE_DIR=".worktrees/${AGENT_NAME}-${BRANCH_NAME}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Run this from inside a git repository."
  exit 1
fi

if [[ -e "$WORKTREE_DIR" ]]; then
  echo "Worktree already exists: $WORKTREE_DIR"
  echo "Reuse it: cd $WORKTREE_DIR"
  exit 0
fi

if git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}"; then
  echo "Local branch already exists: ${BRANCH_NAME}"
  exit 1
fi

if git ls-remote --heads origin "$BRANCH_NAME" | grep -qE "[[:space:]]refs/heads/${BRANCH_NAME}$"; then
  echo "Remote branch already exists: ${BRANCH_NAME}"
  exit 1
fi

git fetch origin "$BASE_BRANCH"
git worktree add -b "$BRANCH_NAME" "$WORKTREE_DIR" "origin/$BASE_BRANCH"

echo "Worktree ready: $WORKTREE_DIR"
echo "Next:"
echo "  cd $WORKTREE_DIR"
echo "  ../scripts/bootstrap_env.sh"
