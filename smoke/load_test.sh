#!/usr/bin/env bash
# Load test: 10 wrapped mock-agents passing messages around a ring.
#
# Pattern: agent_0 -> agent_1 -> agent_2 -> ... -> agent_9 -> agent_0
# Each agent receives a message, doesn't reply (mock-agent has no logic).
# We pump 100 messages from outside and measure delivery + DB consistency.
#
# Pass criteria:
#   - all 100 messages claimed + read in DB
#   - no daemon errors
#   - all 10 wrappers stay connected
set -euo pipefail
cd "$(dirname "$0")/.."

AB=$PWD/target/release/agentbus
MOCK=$PWD/smoke/target/release/mock-agent
N_AGENTS=${N_AGENTS:-10}
N_MESSAGES=${N_MESSAGES:-100}

echo "=== load test: $N_AGENTS agents, $N_MESSAGES messages ==="
echo "daemon status:"
$AB status > /dev/null 2>&1 || { echo "daemon not running, starting"; $AB start; sleep 1; }

# Start N wrappers
PIDS=()
for i in $(seq 0 $((N_AGENTS-1))); do
  rm -f /tmp/load-agent-$i.log
  $AB run --name "load-$i" --program test --transcript /tmp/load-agent-$i.log -- $MOCK > /tmp/load-runner-$i.log 2>&1 < /dev/null &
  PIDS+=($!)
done

# Give them time to register + connect
sleep 3

# Verify all are active
ACTIVE=$($AB status 2>&1 | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for a in d['agents'] if a['name'].startswith('load-') and a['state']=='active'))")
echo "wrappers active: $ACTIVE / $N_AGENTS"

# Register a sender
$AB register --name load-sender --program test > /dev/null

# Pump messages — fan out to random recipients
START=$(date +%s%N)
for i in $(seq 1 $N_MESSAGES); do
  TARGET="load-$((i % N_AGENTS))"
  $AB send --from load-sender --to "$TARGET" --msg-type request --thread-id "load-$i" "msg-$i" > /dev/null 2>&1 &
  # Throttle to avoid spawning too many subshells at once
  if (( i % 20 == 0 )); then wait; fi
done
wait
END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
echo "send loop: ${ELAPSED_MS}ms for $N_MESSAGES msgs ($((N_MESSAGES * 1000 / (ELAPSED_MS+1))) msg/s)"

# Wait for delivery to settle
sleep 4

# Count claimed/read in DB
echo ""
echo "=== DB consistency ==="
sqlite3 ~/.agentbus/bus.db <<SQL
.headers on
.mode column
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN read_at IS NOT NULL THEN 1 ELSE 0 END) AS read,
  SUM(CASE WHEN claimed_at IS NOT NULL AND read_at IS NULL THEN 1 ELSE 0 END) AS claimed_only,
  SUM(CASE WHEN claimed_at IS NULL THEN 1 ELSE 0 END) AS unclaimed
FROM messages
WHERE from_agent = 'load-sender' AND to_agent LIKE 'load-%';
SQL

# Count SUBMIT events per wrapper (proxy for delivered count)
echo ""
echo "=== per-wrapper delivery counts ==="
TOTAL_DELIVERED=0
for i in $(seq 0 $((N_AGENTS-1))); do
  C=$(grep -c "SUBMIT received" /tmp/load-agent-$i.log 2>/dev/null || echo 0)
  echo "  load-$i: $C"
  TOTAL_DELIVERED=$((TOTAL_DELIVERED + C))
done
echo "  total submits: $TOTAL_DELIVERED"

# Cleanup
echo ""
echo "=== cleanup ==="
for PID in "${PIDS[@]}"; do
  kill $PID 2>/dev/null || true
done
wait 2>/dev/null || true
sleep 1
echo "done"
