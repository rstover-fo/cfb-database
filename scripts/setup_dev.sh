#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
VENV_DIR="${CFB_VENV_DIR:-${REPO_ROOT}/.venv}"

if [[ "${VENV_DIR}" != /* ]]; then
  VENV_DIR="${REPO_ROOT}/${VENV_DIR}"
fi

python_is_compatible() {
  local candidate="$1"

  command -v "${candidate}" >/dev/null 2>&1 \
    && "${candidate}" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'
}

if [[ "${CFB_PYTHON+x}" == "x" ]]; then
  PYTHON_BIN="${CFB_PYTHON}"
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "Python executable not found: ${PYTHON_BIN}" >&2
    exit 1
  fi
  if ! python_is_compatible "${PYTHON_BIN}"; then
    echo "Python 3.11 or newer is required: ${PYTHON_BIN}" >&2
    exit 1
  fi
else
  PYTHON_BIN=""
  for candidate in \
    "${VENV_DIR}/bin/python" \
    "${REPO_ROOT}/.venv/bin/python" \
    python3.14 python3.13 python3.12 python3.11 python3; do
    if python_is_compatible "${candidate}"; then
      PYTHON_BIN="${candidate}"
      break
    fi
  done

  if [[ -z "${PYTHON_BIN}" ]]; then
    echo "Python 3.11 or newer was not found. Set CFB_PYTHON to a compatible interpreter." >&2
    exit 1
  fi
fi

echo "Using Python interpreter: ${PYTHON_BIN}"

if [[ -e "${VENV_DIR}" && ! -d "${VENV_DIR}" ]]; then
  echo "Virtual environment path is not a directory: ${VENV_DIR}" >&2
  exit 1
fi

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Creating virtual environment at ${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

if ! "${VENV_DIR}/bin/python" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'; then
  echo "Existing virtual environment must use Python 3.11 or newer: ${VENV_DIR}" >&2
  exit 1
fi

echo "Installing root and MCP development dependencies"
"${VENV_DIR}/bin/python" -m pip install \
  --editable "${REPO_ROOT}[dev,compute,flatfiles]" \
  --editable "${REPO_ROOT}/mcp[dev]"

echo "Development environment ready: ${VENV_DIR}"
