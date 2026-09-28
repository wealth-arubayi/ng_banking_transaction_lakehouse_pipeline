#!/usr/bin/env bash
#
# check_undeployed.sh — Detect changes that haven't been deployed via DABs.
#
# Checks four layers:
#   0. Deployment status — are the bundle's jobs/pipelines live in the workspace?
#   1. Git  — uncommitted edits and unpushed commits (code not even in the repo yet)
#   2. Bundle — `databricks bundle validate` (is the config structurally deployable?)
#   3. Drift — files modified after the last known deployment timestamp
#
# Usage:
#   ./scripts/check_undeployed.sh              # default target (dev)
#   ./scripts/check_undeployed.sh -t staging   # specific target
#   ./scripts/check_undeployed.sh --skip-validate  # skip bundle validate (faster)
#
# Exit codes:  0 = clean / deployable,  1 = undeployed changes found,  2 = error

set -euo pipefail

TARGET="dev"
SKIP_VALIDATE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--target) TARGET="$2"; shift 2 ;;
    --skip-validate) SKIP_VALIDATE=true; shift ;;
    -h|--help)
      echo "Usage: $0 [-t <target>] [--skip-validate]"
      exit 0 ;;
    *) echo "Unknown argument: $1"; exit 2 ;;
  esac
done

BUNDLE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BUNDLE_ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
HAS_CHANGES=false

echo "=========================================="
echo "  Undeployed Changes Check (target: $TARGET)"
echo "=========================================="

# ── 0. Deployment status: are the bundle's resources live? ──
echo -e "\n${CYAN}[0/4] Workspace — deployed bundle status${NC}"

DEPLOY_MARKER="$BUNDLE_ROOT/.last_deploy_$TARGET"
DEPLOY_HASH_FILE="$BUNDLE_ROOT/.last_deploy_hash_$TARGET"

if command -v databricks >/dev/null 2>&1; then
  # List jobs defined in the bundle's resource YAMLs
  JOB_NAMES=$(grep -ohP '^\s+ng_banking_\w+' resources/*.yml 2>/dev/null | sed 's/^[[:space:]]*//' | sort -u || true)

  if [[ -n "$JOB_NAMES" ]]; then
    echo "  Bundle defines these jobs:"
    for job in $JOB_NAMES; do
      JOB_ID=$(databricks jobs list --filter "name = '$job'" -o json 2>/dev/null | jq -r '.jobs[0].job_id // empty' 2>/dev/null || true)
      if [[ -n "$JOB_ID" ]]; then
        echo -e "    ${GREEN}✓ $job${NC}  (job_id: $JOB_ID)"
      else
        echo -e "    ${RED}✗ $job${NC}  (not deployed)"
        HAS_CHANGES=true
      fi
    done
  fi

  # Compare local bundle hash with last deployed hash
  CURRENT_HASH=$(find notebooks/ resources/ sql/ scripts/ databricks.yml -type f -exec sha256sum {} + 2>/dev/null | sha256sum | cut -d' ' -f1 || true)
  if [[ -f "$DEPLOY_HASH_FILE" ]]; then
    DEPLOYED_HASH=$(cat "$DEPLOY_HASH_FILE")
    if [[ "$CURRENT_HASH" == "$DEPLOYED_HASH" ]]; then
      echo -e "  ${GREEN}Bundle content hash matches last deployment.${NC}"
    else
      echo -e "  ${RED}Bundle content hash differs from last deployment (code changed).${NC}"
      echo "    Deployed: $DEPLOYED_HASH"
      echo "    Current:  $CURRENT_HASH"
      HAS_CHANGES=true
    fi
  else
    echo -e "  ${YELLOW}No deployment hash on file — never deployed (or marker deleted).${NC}"
    HAS_CHANGES=true
  fi
else
  echo -e "  ${YELLOW}Databricks CLI not found — skipping workspace checks.${NC}"
fi

if [[ -f "$DEPLOY_MARKER" ]]; then
  echo "  Last deployment timestamp: $(cat "$DEPLOY_MARKER")"
else
  echo -e "  ${YELLOW}No deployment marker (.last_deploy_$TARGET).${NC}"
fi

# ── 1. Git: uncommitted changes ──────────────────────────────
echo -e "\n${CYAN}[1/4] Git — uncommitted changes${NC}"

if git rev-parse --git-dir >/dev/null 2>&1; then
  UNCOMMITTED=$(git status --porcelain -- notebooks/ resources/ sql/ scripts/ databricks.yml | grep -v '^??' || true)
  UNTRACKED=$(git status --porcelain -- notebooks/ resources/ sql/ scripts/ databricks.yml | grep '^??' || true)

  if [[ -n "$UNCOMMITTED" ]]; then
    echo -e "  ${RED}Modified (not committed):${NC}"
    echo "$UNCOMMITTED" | sed 's/^/    /'
    HAS_CHANGES=true
  else
    echo -e "  ${GREEN}No uncommitted changes to tracked files.${NC}"
  fi

  if [[ -n "$UNTRACKED" ]]; then
    echo -e "  ${YELLOW}Untracked (new, not in Git):${NC}"
    echo "$UNTRACKED" | sed 's/^/    /'
    HAS_CHANGES=true
  fi

  # Unpushed commits
  UNPUSHED=$(git log origin/"$(git branch --show-current)"..HEAD --oneline 2>/dev/null || true)
  if [[ -n "$UNPUSHED" ]]; then
    echo -e "\n  ${RED}Unpushed commits:${NC}"
    echo "$UNPUSHED" | sed 's/^/    /'
    HAS_CHANGES=true
  else
    echo -e "  ${GREEN}All commits pushed.${NC}"
  fi
else
  echo -e "  ${YELLOW}Not a Git repo — skipping Git checks.${NC}"
fi

# ── 2. Bundle validation ─────────────────────────────────────
echo -e "\n${CYAN}[2/4] Bundle — validate (target: $TARGET)${NC}"

if [[ "$SKIP_VALIDATE" == true ]]; then
  echo -e "  ${YELLOW}Skipped (--skip-validate).${NC}"
else
  if command -v databricks >/dev/null 2>&1; then
    if databricks bundle validate -t "$TARGET" >/dev/null 2>&1; then
      echo -e "  ${GREEN}Bundle validation passed.${NC}"
    else
      echo -e "  ${RED}Bundle validation FAILED — run: databricks bundle validate -t $TARGET${NC}"
      HAS_CHANGES=true
    fi
  else
    echo -e "  ${YELLOW}Databricks CLI not found — skipping bundle validation.${NC}"
  fi
fi

# ── 3. Drift: files changed since last deployment marker ─────
echo -e "\n${CYAN}[3/4] Drift — files changed since last deployment${NC}"

DEPLOY_MARKER="$BUNDLE_ROOT/.last_deploy_$TARGET"

if [[ -f "$DEPLOY_MARKER" ]]; then
  DRIFT_FILES=$(find "$BUNDLE_ROOT/notebooks" "$BUNDLE_ROOT/resources" "$BUNDLE_ROOT/sql" \
    -type f -newer "$DEPLOY_MARKER" 2>/dev/null || true)

  if [[ -n "$DRIFT_FILES" ]]; then
    echo -e "  ${RED}Files modified after last deployment:${NC}"
    echo "$DRIFT_FILES" | sed "s|$BUNDLE_ROOT/||" | sed 's/^/    /'
    HAS_CHANGES=true
  else
    echo -e "  ${GREEN}No files modified since last deployment.${NC}"
  fi
else
  echo -e "  ${YELLOW}No deployment marker found (.last_deploy_$TARGET).${NC}"
  echo -e "  ${YELLOW}Run this after deploying to start tracking drift:${NC}"
  echo -e "    echo -e "    ./scripts/commit_and_deploy.sh -t $TARGET"
  HAS_CHANGES=true
fi

# ── Summary ──────────────────────────────────────────────────
echo -e "\n=========================================="
if [[ "$HAS_CHANGES" == true ]]; then
  echo -e "${RED}  ⚠ UNDEPLOYED CHANGES DETECTED${NC}"
  echo -e "  Deploy with:  ./scripts/commit_and_deploy.sh -t $TARGET"
  echo "=========================================="
  exit 1
else
  echo -e "${GREEN}  ✓ All clear — no undeployed changes.${NC}"
  echo "=========================================="
  exit 0
fi
