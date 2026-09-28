#!/usr/bin/env bash
#
# run_tests.sh — Run pytest on Databricks workspace (FUSE mount).
#
# The workspace filesystem doesn't support __pycache__ directory creation,
# so PYTHONDONTWRITEBYTECODE=1 must be set before Python starts.
# This wrapper handles that automatically.
#
# Usage:
#   ./scripts/run_tests.sh                    # run all tests
#   ./scripts/run_tests.sh tests/test_transform_rules.py  # specific file
#   ./scripts/run_tests.sh -k test_sign      # pytest expression

set -euo pipefail

export PYTHONDONTWRITEBYTECODE=1
export PYTHONPYCACHEPREFIX=/tmp/pycache_ng_banking

BUNDLE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BUNDLE_ROOT"

exec python -m pytest "$@"