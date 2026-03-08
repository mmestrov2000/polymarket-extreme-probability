# Scripts Overview

Scripts are optional accelerators. Team workflow does not depend on them.

## General Helpers
- `bootstrap_env.sh`: initialize local Python environment for this repo.
- `agent_worktree_start.sh`: convenience wrapper to create a worktree branch.
- `agent_worktree_finish.sh`: convenience wrapper to push branch and optionally open a PR.

## Local Codex Helpers
- `local/install_codex_skills.sh`: install template skills into local Codex profile.

Use equivalent native tooling (`git`, your package manager, GitHub UI/CLI) if you do not want script-based workflows.
