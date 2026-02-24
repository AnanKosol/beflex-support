#!/usr/bin/env bash
set -euo pipefail

SRC_ROOT="${SRC_ROOT:-/opt/beflex-workspace}"
DST_ROOT="${DST_ROOT:-/opt/allops-raku-export/repo}"
BRANCH="${BRANCH:-main}"

COMMIT_MSG="${1:-chore: sync latest raku backend/frontend from workspace}"

echo "[1/5] Sync backend"
rsync -av --delete "$SRC_ROOT/allops-raku-backend/" "$DST_ROOT/allops-raku-backend/"

echo "[2/5] Sync frontend"
rsync -av --delete "$SRC_ROOT/allops-raku-frontend/" "$DST_ROOT/allops-raku-frontend/"

echo "[3/5] Git status"
cd "$DST_ROOT"
git status --short

if [[ -z "$(git status --porcelain)" ]]; then
  echo "No changes to commit."
  exit 0
fi

echo "[4/5] Commit"
git add allops-raku-backend allops-raku-frontend
git commit -m "$COMMIT_MSG"

echo "[5/5] Push"
git push origin "$BRANCH"

echo "Done. Latest commits:"
git log --oneline -n 3
