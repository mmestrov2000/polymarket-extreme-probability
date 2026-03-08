# Project Template

This repository is a reusable project starter for teams building software with or without Codex.

## What You Get
- Standard repo structure
- Canonical context files: spec, architecture, tasks
- Premade skills for main/planner/feature/review/test/release roles
- Tool-agnostic team workflow in `docs/PLAYBOOK.md`
- Optional local scripts for faster personal workflows

## Quick Start (New Project)
1. Create a new repo from this GitHub template.
2. Produce or refine `PROJECT_SPEC.md`, `ARCHITECTURE.md`, and `TASKS.md`.
3. For each implementation task, use the optional agent pipeline:
   - `feature-planner-agent` -> `feature-agent` -> `review-agent` -> `test-agent` -> `release-agent`
4. Open PRs for all change-making work.

## Repository Structure
- `PROJECT_SPEC.md` - canonical spec and acceptance criteria
- `ARCHITECTURE.md` - architecture and folder layout
- `TASKS.md` - task breakdown and ownership
- `docs/PLAYBOOK.md` - single operational workflow guide
- `AGENTS.md` - global repository rules
- `prompts/prompts.md` - optimized templates for each agent prompt
- `skills/` - reusable Codex skill definitions
- `scripts/` - optional helper scripts (see `scripts/README.md`)
- `scripts/local/` - optional Codex-only local scripts
- `.github/workflows/` - CI skeleton
