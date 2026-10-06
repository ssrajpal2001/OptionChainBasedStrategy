#!/bin/bash
set -e

# ── fill these in ──────────────────────────────────────────────
ADMIN_USER="admin"
ADMIN_PASS="Dell@1330"
CLIENT_ID="ssrajpal2001"
BINDING_ID="SA5770"
BASE_URL="http://localhost:5000"
# ────────────────────────────────────────────────────────────────

echo "Logging in..."
TOKEN=$(curl -s -X POST "$BASE_URL/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"role\":\"admin\",\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

if [ -z "$TOKEN" ]; then
  echo "Login failed -- check ADMIN_USER/ADMIN_PASS."
  exit 1
fi

echo "Finding deploy_id for $CLIENT_ID/$BINDING_ID..."
DEPLOY_ID=$(curl -s "$BASE_URL/api/admin/oiorb/deployments" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for dep in d['deployments']:
    if dep['client_id'] == '$CLIENT_ID' and dep['binding_id'] == '$BINDING_ID':
        print(dep['deploy_id'])
        break
")

if [ -z "$DEPLOY_ID" ]; then
  echo "No OI-ORB deployment found for $CLIENT_ID/$BINDING_ID."
  exit 1
fi
echo "deploy_id = $DEPLOY_ID"

echo "Fetching current full config (so nothing else gets reset)..."
CURRENT_PARAMS=$(curl -s "$BASE_URL/api/admin/oiorb/deployments" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for dep in d['deployments']:
    if dep['deploy_id'] == '$DEPLOY_ID':
        print(json.dumps(dep['params']))
        break
")

echo "Bumping chain_watch_max_stocks: 2 -> 10 (keeping every other value unchanged)..."
NEW_PARAMS=$(python3 -c "
import json
p = json.loads('''$CURRENT_PARAMS''')
p['chain_watch_max_stocks'] = 10
print(json.dumps(p))
")

echo "Saving..."
curl -s -X POST "$BASE_URL/api/admin/oiorb/config/$DEPLOY_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$NEW_PARAMS"
echo
