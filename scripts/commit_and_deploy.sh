#!/usr/bin/env bash
#
# commit_and_deploy.sh — Commit changes to Git, push to GitHub, deploy via DABs.
#
# Workflow:
#   1. Run check_undeployed.sh to show what will be deployed
#   2. Stage and commit all changes (auto-generates message from diff summary)
#   3. Push to the remote (GitHub)
#   4. Validate the bundle (fails fast on config errors)
#   5. Deploy the bundle to the Databricks workspace
#   6. Stamp deployment markers (timestamp + content hash)
#
# Usage:
#   ./scripts/commit_and_deploy.sh                    # default target (dev)
#   ./scripts/commit_and_deploy.sh -t staging         # specific target
#   ./scripts/commit_and_deploy.sh -m "fix: ..."      # custom commit message
#   ./scripts/commit_and_deploy.sh --skip-commit        # deploy only (no git commit)
#   ./scripts/commit_and_deploy.sh --dry-run            # show what would happen
#
# Exit codes: 0 = success, 1 = deploy had undeployed changes, 2 = error

set -euo pipefail

TARGET="dev"
COMMIT_MSG=""
SKIP_COMMIT=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--target) TARGET="$2"; shift 2 ;;
    -m|--message) COMMIT_MSG="$2"; shift 2 ;;
    --skip-commit) SKIP_COMMIT=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help)
      echo "Usage: $0 [-t <target>] [-m <message>] [--skip-commit] [--dry-run]"
      exit 0 ;;
    *) echo "Unknown argument: $1"; exit 2 ;;
  esac
done

BUNDLE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BUNDLE_ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}==========================================${NC}"
echo -e "${BOLD}  Commit & Deploy (target: $TARGET)${NC}"
echo -e "${BOLD}==========================================${NC}"

# ── 1. Pre-flight: show undeployed changes ───────────────────
echo -e "\n${CYAN}[1/5] Pre-flight check${NC}"
if [[ -x "$BUNDLE_ROOT/scripts/check_undeployed.sh" ]]; then
  $BUNDLE_ROOT/scripts/check_undeployed.sh -t "$TARGET" --skip-validate || true
else
  echo "  check_undeployed.sh not found — skipping pre-flight."
fi

# ── 2. Git: stage, commit, push ──────────────────────────────
echo -e "\n${CYAN}[2/5] Git — commit and push${NC}"

if [[ "$SKIP_COMMIT" == true ]]; then
  echo -e "  ${YELLOW}Skipped (--skip-commit).${NC}"
elif ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo -e "  ${YELLOW}Not a Git repo — skipping commit/push.${NC}"
elif [[ -z "$(git status --porcelain)" ]]; then
  echo -e "  ${GREEN}Working tree clean — nothing to commit.${NC}"
else
  # Auto-generate commit message if not provided
  if [[ -z "$COMMIT_MSG" ]]; then
    CHANGED=$(git diff --name-only --cached --stat 2>/dev/null || git diff --name-only --stat 2>/dev/null || true)
    FILE_COUNT=$(echo "$CHANGED" | grep -c . || echo "0")
    COMMIT_MSG="deploy: update $FILE_COUNT files ($TARGET)"
  fi

  if [[ "$DRY_RUN" == true ]]; then
    echo -e "  ${YELLOW}[dry-run] Would commit: $COMMIT_MSG${NC}"
    git status --short | sed 's/^/    /'
  else
    git add -A
    git commit -m "$COMMIT_MSG"
    echo -e "  ${GREEN}Committed: $COMMIT_MSG${NC}"

    # Push to remote
    CURRENT_BRANCH=$(git branch --show-current)
    if git push origin "$CURRENT_BRANCH" 2>/dev/null; then
      echo -e "  ${GREEN}Pushed to origin/$CURRENT_BRANCH${NC}"
    else
      echo -e "  ${RED}Push failed — check your remote and credentials.${NC}"
      echo -e "  Commit is local only. Push manually: git push origin $CURRENT_BRANCH"
    fi
  fi
fi

# ── 3. Bundle validate ──────────────────────────────────────
echo -e "\n${CYAN}[3/5] Bundle validate${NC}"

if ! command -v databricks >/dev/null 2>&1; then
  echo -e "  ${RED}Databricks CLI not found. Install: https://docs.databricks.com/en/dev-tools/cli/install.html${NC}"
  exit 2
fi

if [[ "$DRY_RUN" == true ]]; then
  echo -e "  ${YELLOW}[dry-run] Would run: databricks bundle validate -t $TARGET${NC}"
else
  echo "  Validating bundle..."
  if databricks bundle validate -t "$TARGET" 2>&1; then
    echo -e "  ${GREEN}Bundle validation passed.${NC}"
  else
    echo -e "  ${RED}Bundle validation FAILED. Fix errors above before deploying.${NC}"
    exit 2
  fi
fi

# ── 4. Deploy ────────────────────────────────────────────────
echo -e "\n${CYAN}[4/5] Deploy to workspace${NC}"

if [[ "$DRY_RUN" == true ]]; then
  echo -e "  ${YELLOW}[dry-run] Would run: databricks bundle deploy -t $TARGET${NC}"
else
  echo "  Deploying..."
  if databricks bundle deploy -t "$TARGET" 2>&1; then
    echo -e "  ${GREEN}Deployment successful.${NC}"
  else
    echo -e "  ${RED}Deployment FAILED. See errors above.${NC}"
    exit 2
  fi
fi

# ── 5. Stamp deployment markers ─────────────────────────────
echo -e "\n${CYAN}[5/5] Stamp deployment markers${NC}"

if [[ "$DRY_RUN" == true ]]; then
  echo -e "  ${YELLOW}[dry-run] Would stamp .last_deploy_$TARGET and .last_deploy_hash_$TARGET${NC}"
else
  # Timestamp marker
  date -u +%Y-%m-%dT%H:%M:%SZ > "$BUNDLE_ROOT/.last_deploy_$TARGET"
  echo "  Timestamp: $(cat "$BUNDLE_ROOT/.last_deploy_$TARGET")"

  # Content hash marker (sha256 of all tracked source files)
  find notebooks/ resources/ sql/ scripts/ databricks.yml -type f -exec sha256sum {} + 2>/dev/null \
    | sha256sum | cut -d' ' -f1 > "$BUNDLE_ROOT/.last_deploy_hash_$TARGET"
  echo "  Hash: $(cat "$BUNDLE_ROOT/.last_deploy_hash_$TARGET")"

  echo -e "  ${GREEN}Markers stamped. Future check_undeployed.sh runs will compare against these.${NC}"
fi

# ── Summary ──────────────────────────────────────────────────
echo -e "\n${BOLD}==========================================${NC}"
if [[ "$DRY_RUN" == true ]]; then
  echo -e "${YELLOW}  ○ DRY RUN COMPLETE — no changes made.${NC}"
else
  echo -e "${GREEN}  ✓ DEPLOY COMPLETE (target: $TARGET)${NC}"
fi
echo -e "${BOLD}==========================================${NC}"