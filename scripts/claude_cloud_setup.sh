#!/usr/bin/env bash
set -euo pipefail

# Project hooks also run locally. Local setup remains an explicit command.
if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" ]]; then
  exit 0
fi

cfb_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
bash "$cfb_script_dir/setup_dev.sh"
