# Team Playbook

This playbook defines team-wide, tool-agnostic collaboration rules.

## Canonical Context
- `PROJECT_SPEC.md`: product requirements, acceptance criteria, and test strategy
- `ARCHITECTURE.md`: design, boundaries, and repository layout
- `TASKS.md`: scoped tasks, ownership, status, and implementation plans

## Agent Pipeline (Optional)
1. Main Agent: bootstrap or re-baseline spec, architecture, and tasks.
2. Feature Planner Agent: produce implementation plan for one task.
3. Feature Agent: implement code changes from approved plan.
4. Review Agent: identify bugs, regressions, and weak tests.
5. Test Agent: add or improve edge-case and regression coverage.
6. Release Agent: run validation checks and summarize readiness.

## Team Development Flow
1. Create a branch from latest `main`.
2. Implement scoped changes.
3. Run tests and checks required for changed areas.
4. Update `TASKS.md` status.
5. Open PR and request review.

## Branching Rules
- Never commit directly on `main`.
- Use one branch per task.
- Keep PRs scoped and reviewable.

## Optional Local Accelerators
- Scripts under `scripts/` are optional helper commands.
- Codex-specific local scripts live under `scripts/local/`.
- Teammates can follow this playbook without using repo scripts.

## Prompts and Skills (Optional)
- Prefer installed skills from `skills/`.
- If needed, use templates from `prompts/prompts.md`.

## New Project Startup
1. Create repository from this template.
2. Produce or refine `PROJECT_SPEC.md`, `ARCHITECTURE.md`, and `TASKS.md`.
3. Optionally use `main-agent` for faster bootstrap.
4. Execute delivery through PR-driven workflow.
