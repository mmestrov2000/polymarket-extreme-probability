#!/usr/bin/env bash
set -euo pipefail

choose_bootstrap_python() {
  if [[ -n "${BOOTSTRAP_PYTHON:-}" ]]; then
    echo "${BOOTSTRAP_PYTHON}"
    return 0
  fi

  if command -v python3.12 >/dev/null 2>&1; then
    echo "python3.12"
    return 0
  fi

  echo "python3"
}

BOOTSTRAP_PYTHON_CMD="$(choose_bootstrap_python)"

if [[ ! -d ".venv" ]]; then
  "${BOOTSTRAP_PYTHON_CMD}" -m venv .venv
  echo "Created virtual environment at .venv with ${BOOTSTRAP_PYTHON_CMD}"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "Using virtual environment Python: $(python --version 2>&1)"

if ! python - <<'PY'
import sys

raise SystemExit(0 if sys.version_info >= (3, 9, 10) else 1)
PY
then
  echo "Warning: this virtual environment uses Python < 3.9.10."
  echo "Notebook analysis dependencies will install, but optional py_clob_client tooling is skipped on this interpreter."
  if command -v python3.12 >/dev/null 2>&1; then
    echo "For full repo tooling support, recreate the environment with Python 3.12:"
    echo "  rm -rf .venv && python3.12 -m venv .venv"
  fi
fi

# Keep bootstrap resilient in offline/hackathon environments.
if ! python -m pip install --upgrade pip setuptools wheel; then
  echo "Warning: could not upgrade packaging tools (offline or restricted network)."
  echo "Continuing with existing virtual environment tooling."
fi

if [[ -f "requirements.txt" ]]; then
  if ! pip install -r requirements.txt; then
    echo "Warning: failed to install requirements.txt."
    echo "If you are offline, rerun when network is available."
  fi
fi

if [[ -f "requirements-dev.txt" ]]; then
  if ! pip install -r requirements-dev.txt; then
    echo "Warning: failed to install requirements-dev.txt."
    echo "If you are offline, rerun when network is available."
  fi
fi

if [[ -f "pyproject.toml" && ! -f "requirements.txt" ]]; then
  if grep -qE "^\[project\]" pyproject.toml; then
    if ! pip install -e .; then
      echo "Warning: failed to install local project package."
      echo "If you are offline, rerun when network/dependencies are available."
    fi
  elif grep -qE "^\[tool\.poetry\]" pyproject.toml; then
    echo "Detected Poetry project. Install dependencies with: poetry install"
  fi
fi

echo "Virtual environment is ready and active for this shell."
echo "Run: source .venv/bin/activate"
