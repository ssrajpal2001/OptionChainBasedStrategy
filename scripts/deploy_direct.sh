#!/usr/bin/env bash
# deploy_direct.sh -- sync the local working tree straight to the EC2 box over SSH and
# restart terminus, skipping the commit/push/git-pull round-trip entirely.
#
# Matches the workflow of the user's other app (E:\AlgoSoft\Option_Buying\algo): local git
# stays as version control (commit/push to GitHub separately), but DEPLOY is direct.
#
# 2026-10-15 REAL INCIDENT, fixed same night: the server's own git tracking was never told
# about any of these direct syncs -- its HEAD stayed frozen at whatever commit it was last
# `git reset --hard` to, while the actual files on disk silently drifted ahead (including,
# worse, carrying over whatever UNCOMMITTED local changes happened to be sitting in this
# working tree at sync time -- tar syncs the real working tree, not just git's committed
# view). The user's own `Deploy_Fresh_Start_EC2.bat` (still `git pull`-based, unchanged, and
# still the thing run every morning) then hit a real merge conflict and aborted, because git
# on the server saw local modifications it never agreed to. Fixed by requiring a clean,
# already-pushed commit before every deploy, and reconciling the server's git state to match
# origin right after the sync -- this script and the user's own git-pull-based routine can
# now never diverge from each other again.
#
# Uses tar piped over ssh (not rsync -- not available in this Windows Git Bash environment
# and not worth installing just for this). ponytail: no incremental delete-of-removed-files
# support this way (rsync's own --delete would give that) -- acceptable for now since a
# normal deploy only adds/changes files; if a file ever needs removing server-side, do it
# by hand over `ssh terminus`. Revisit with real rsync only if that gap actually bites.
#
# Requires: the `terminus` SSH host alias in ~/.ssh/config (HostName 13.200.171.160,
# User ec2-user, IdentityFile ~/.ssh/algotrading_ec2) -- added 2026-10-15.
#
# Usage: bash scripts/deploy_direct.sh
set -e
cd "$(dirname "$0")/.."

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
LOCAL_SHA="$(git rev-parse --short HEAD)"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "ERROR: working tree has uncommitted changes. Commit them first --"
    echo "       a deploy must always correspond to a real, pushed commit, so the"
    echo "       server's own git tracking (used by Deploy_Fresh_Start_EC2.bat's"
    echo "       git pull) never drifts out of sync again (real incident, 2026-10-15)."
    exit 1
fi

if ! git merge-base --is-ancestor HEAD "origin/${BRANCH}" 2>/dev/null; then
    echo "ERROR: local HEAD (${LOCAL_SHA}) isn't pushed to origin/${BRANCH} yet."
    echo "       Push it first: git push origin ${BRANCH}"
    exit 1
fi

echo "=== deploy_direct: syncing working tree (local HEAD=${LOCAL_SHA}, clean, pushed) ==="

# 1. Sync the working tree straight to the server. The exclude list keeps the server's own
#    runtime state (data/, logs/, .git/, .keys/) completely untouched -- this only ever
#    overwrites CODE.
tar --exclude-from=.rsync-exclude -cf - . | ssh terminus "mkdir -p ~/OptionChainBasedStrategy && tar -xf - -C ~/OptionChainBasedStrategy"

# 2. Reconcile the server's git tracking to match origin exactly -- since we just confirmed
#    HEAD is clean and already pushed, this is a pure no-op on file CONTENT (tar already put
#    the right bytes there) but keeps git's own metadata truthful, so a future `git pull`
#    (the user's own Deploy_Fresh_Start_EC2.bat) never hits a phantom merge conflict again.
echo "=== reconciling server git state ==="
ssh terminus "cd ~/OptionChainBasedStrategy && git fetch origin && git reset --hard origin/${BRANCH}"

# 3. Restart terminus on the server using the code that was JUST synced -- no git pull
#    needed here (already reconciled above), the tar sync + reset IS the deploy.
echo "=== restarting terminus ==="
ssh terminus "cd ~/OptionChainBasedStrategy && bash scripts/start_terminus.sh --no-pull"

echo ""
echo "=== DONE -- deployed ${BRANCH}@${LOCAL_SHA} ==="
echo "Verify: ssh terminus 'pm2 logs terminus --lines 30'"
