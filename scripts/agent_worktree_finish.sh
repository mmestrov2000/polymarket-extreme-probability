#!/usr/bin/env bash
set -euo pipefail

BASE_BRANCH="main"
PR_TITLE=""
PR_BODY=""
OPEN_PR=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      BASE_BRANCH="$2"
      shift 2
      ;;
    --title)
      PR_TITLE="$2"
      shift 2
      ;;
    --body)
      PR_BODY="$2"
      shift 2
      ;;
    --pr)
      OPEN_PR=1
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: scripts/agent_worktree_finish.sh [--base main] [--pr] [--title \"...\"] [--body \"...\"]"
      exit 1
      ;;
  esac
done

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Run this from inside a git worktree repository."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is not clean. Commit or stash changes first."
  exit 1
fi

BRANCH_NAME="$(git branch --show-current)"
if [[ -z "$BRANCH_NAME" ]]; then
  echo "Could not detect current branch."
  exit 1
fi

if [[ "$BRANCH_NAME" == "$BASE_BRANCH" ]]; then
  echo "Refusing to push directly from base branch: $BASE_BRANCH"
  exit 1
fi

git push -u origin "$BRANCH_NAME"
echo "Branch pushed: $BRANCH_NAME"

if [[ "$OPEN_PR" -eq 1 ]]; then
  if ! command -v gh >/dev/null 2>&1; then
    echo "GitHub CLI not found. Install gh or create the PR manually."
    exit 1
  fi

  if [[ -n "$PR_TITLE" && -n "$PR_BODY" ]]; then
    gh pr create --base "$BASE_BRANCH" --head "$BRANCH_NAME" --title "$PR_TITLE" --body "$PR_BODY"
  else
    gh pr create --base "$BASE_BRANCH" --head "$BRANCH_NAME" --fill
  fi
fi

echo "Finish flow complete."
