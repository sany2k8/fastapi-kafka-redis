#!/usr/bin/env bash
# End-to-end walkthrough of the four behaviours this project is built to show:
#   1. async happy path        2. cache-aside reads
#   3. client-side idempotency 4. retry -> DLQ
set -euo pipefail

API="${API:-http://localhost:8000}"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { printf '\033[36m%s\033[0m\n' "$*"; }
jqq()  { if command -v jq >/dev/null; then jq "$@"; else cat; fi; }

post_order() {
  curl -sS -X POST "$API/orders" -H 'content-type: application/json' "$@"
}

bold "0. Health"
curl -sS "$API/health" | jqq .

bold "1. Happy path — POST returns 202 immediately, Kafka does the work"
ID=$(post_order -d '{"customer_id":"alice","sku":"WIDGET-1","quantity":2,"amount_cents":4999}' \
     | jqq -r .id)
step "order $ID accepted"
step "status right after accept:"
curl -sS "$API/orders/$ID" | jqq '{status, source, attempts}'
sleep 3
step "status after the consumer ran:"
curl -sS "$API/orders/$ID" | jqq '{status, source, attempts, transitions: [.transitions[].to_status]}'

bold "2. Cache-aside — the read above was served from Redis"
step "drop the cache entry and read again (falls back to Postgres, then refills):"
docker compose exec -T redis redis-cli DEL "order:$ID" >/dev/null
curl -sS "$API/orders/$ID" | jqq '{status, source}'
curl -sS "$API/orders/$ID" | jqq '{status, source}'

bold "3. Idempotency-Key — a double-tapped submit creates ONE order"
KEY="demo-$(date +%s)"
A=$(post_order -H "Idempotency-Key: $KEY" \
    -d '{"customer_id":"bob","sku":"WIDGET-2","quantity":1,"amount_cents":1500}')
B=$(post_order -H "Idempotency-Key: $KEY" \
    -d '{"customer_id":"bob","sku":"WIDGET-2","quantity":1,"amount_cents":1500}')
step "first : $(echo "$A" | jqq -c '{id, duplicate}')"
step "second: $(echo "$B" | jqq -c '{id, duplicate}')"

bold "4. Transient failure — retries with backoff, then the DLQ"
FAIL=$(post_order \
  -d '{"customer_id":"carol","sku":"WIDGET-3","quantity":1,"amount_cents":999,"fail_mode":"transient"}' \
  | jqq -r .id)
step "order $FAIL will fail every attempt; watch it walk the retry ladder"
for _ in $(seq 1 12); do
  sleep 5
  S=$(curl -sS "$API/orders/$FAIL" | jqq -r .status)
  A=$(curl -sS "$API/orders/$FAIL" | jqq -r .attempts)
  step "  status=$S attempts=$A"
  # NB: `[ ... ] && break` would abort the whole script under `set -e`
  # on every iteration that is not yet terminal.
  if [ "$S" = "failed" ]; then break; fi
done
step "audit trail:"
curl -sS "$API/orders/$FAIL" | jqq '[.transitions[] | {to_status, note}]'

bold "5. Permanent failure — no retries, straight to the DLQ"
BAD=$(post_order -d '{"customer_id":"dave","sku":"BAD-404","quantity":1,"amount_cents":100}' \
      | jqq -r .id)
sleep 3
curl -sS "$API/orders/$BAD" | jqq '{status, attempts, failure_reason}'

bold "6. Dead letters (archived by the DLQ consumer)"
curl -sS "$API/dead-letters" | jqq '[.[] | {order_id, attempts, error}]'

bold "7. Pipeline counters (Redis)"
curl -sS "$API/stats" | jqq .
