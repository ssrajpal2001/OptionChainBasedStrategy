#!/usr/bin/env bash
# deploy_direct.sh -- sync the local working tree straight to the EC2 box over SSH and
# restart terminus, skipping the commit/push/git-pull round-trip entirely.
#
# Matches the workflow of the user's other app (E:\AlgoSoft\Option_Buying\algo): local git
# stays as version control (commit/push to GitHub separately, unaffected by this script),
# but DEPLOY is direct -- the server never runs `git fetch`/`git reset --hard` for a normal
# deploy anymore.
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

LOCAL_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo 'no-git')"
LOCAL_DIRTY=""
git diff --quiet 2>/dev/null || LOCAL_DIRTY=" (+uncommitted changes)"

echo "=== deploy_direct: syncing working tree (local HEAD=${LOCAL_SHA}${LOCAL_DIRTY}) ==="

# 1. Sync the working tree straight to the server. The exclude list keeps the server's own
#    runtime state (data/, logs/, .git/, .keys/) completely untouched -- this only ever
#    overwrites CODE.
tar --exclude-from=.rsync-exclude -cf - . | ssh terminus "mkdir -p ~/OptionChainBasedStrategy && tar -xf - -C ~/OptionChainBasedStrategy"

# 2. Restart terminus on the server using the code that was JUST synced -- no git pull,
#    the tar sync above IS the deploy.
echo "=== restarting terminus ==="
ssh terminus "cd ~/OptionChainBasedStrategy && bash scripts/start_terminus.sh --no-pull"

echo ""
echo "=== DONE -- deployed local HEAD=${LOCAL_SHA}${LOCAL_DIRTY} ==="
echo "Verify: ssh terminus 'pm2 logs terminus --lines 30'"
