#!/usr/bin/env bash
# =============================================================================
#  Wayang Trino Connector — Live Demo Script
#
#  Demonstrates:
#    Act 1 — Start Trino + Iceberg stack via Docker
#    Act 2 — Query Iceberg data directly through Trino CLI
#    Act 3 — Run the same queries via the Wayang API (optimizer + cost model)
#
#  Prerequisites:
#    - Docker running
#    - Run from the repo root: ./demo-trino.sh
# =============================================================================

set -euo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
# This script lives at the repo root and shells into trino-setup/ for docker
# compose and runs mvn from the repo root.
WAYANG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRINO_SETUP="$WAYANG_ROOT/trino-setup"

TRINO_CONTAINER="trino"
MAVEN_FLAGS="-Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true"

# ── Helpers ───────────────────────────────────────────────────────────────────
banner() {
  echo
  echo "╔══════════════════════════════════════════════════════╗"
  printf  "║  %-52s║\n" "$*"
  echo "╚══════════════════════════════════════════════════════╝"
  echo
}

step() {
  echo
  echo "  ──────────────────────────────────────────────────────"
  echo "  $*"
  echo "  ──────────────────────────────────────────────────────"
  echo
}

pause() {
  echo
  read -rp "  ▶  Press ENTER to continue..." _
  echo
}

run_wayang_demo() {
  mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
    -Dexec.mainClass="org.apache.wayang.genericjdbc.TrinoDemo" \
    ${MAVEN_FLAGS} -q 2>/dev/null || true
}

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 1 — Start Trino + Iceberg via Docker
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 1 — Start Trino + Iceberg via Docker"

step "1a. Starting the stack (Trino + Hive Metastore + MinIO)"
cd "$TRINO_SETUP"
docker compose up -d
echo

step "1b. Containers running"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" \
  | grep -E "NAMES|trino|minio|metastore|postgres"

step "1c. Waiting for Trino to be ready..."
MAX_WAIT=90
ELAPSED=0
until docker exec "$TRINO_CONTAINER" \
        trino --execute "SELECT 1" --output-format ALIGNED > /dev/null 2>&1; do
  if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "  ✗ Timed out waiting for Trino after ${MAX_WAIT}s"
    exit 1
  fi
  printf "  . waiting (%ds elapsed)\r" "$ELAPSED"
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done
echo "  ✓ Trino is ready at http://localhost:8080"

step "1d. Initialising Iceberg tables (create + seed data)"
docker exec -i "$TRINO_CONTAINER" trino < "$TRINO_SETUP/scripts/init.sql" 2>&1 \
  | grep -v "^WARNING\|jline\|org.jline" || true
echo "  ✓ iceberg.sales.orders seeded with 20 rows (4 regions, 5 products)"

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 2 — Query Iceberg directly through the Trino CLI
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 2 — Query Iceberg directly via Trino CLI"
echo "  (No Wayang yet — plain SQL sent straight to Trino)"

step "2a. Full table scan"
echo "  SQL: SELECT * FROM iceberg.sales.orders"
echo
docker exec "$TRINO_CONTAINER" \
  trino --execute "SELECT * FROM iceberg.sales.orders ORDER BY order_id" \
        --output-format ALIGNED 2>/dev/null

step "2b. Filter: region = 'AMER'"
echo "  SQL: SELECT * FROM iceberg.sales.orders WHERE region = 'AMER'"
echo
docker exec "$TRINO_CONTAINER" \
  trino --execute "SELECT * FROM iceberg.sales.orders WHERE region = 'AMER' ORDER BY order_id" \
        --output-format ALIGNED 2>/dev/null

step "2c. Projection: SELECT region, product, amount WHERE region = 'AMER'"
echo "  SQL: SELECT region, product, amount FROM iceberg.sales.orders WHERE region = 'AMER'"
echo
docker exec "$TRINO_CONTAINER" \
  trino --execute \
    "SELECT region, product, amount
     FROM iceberg.sales.orders
     WHERE region = 'AMER'
     ORDER BY order_id" \
  --output-format ALIGNED 2>/dev/null

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 3 — Same operators via Wayang API
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 3 — Wayang API: cost model + filter + projection"
echo "  Wayang rewrites logical operators to Trino-specific physical operators"
echo "  and generates SQL with WHERE + SELECT column pushdown."
cd "$WAYANG_ROOT"

step "Seg 3 — Cost model  |  Seg 4 — Filter  |  Seg 5 — Projection"
echo "  Running TrinoDemo.main() — all three operators demonstrated."
echo
run_wayang_demo

# ─────────────────────────────────────────────────────────────────────────────
banner "Demo complete"
echo "  Trino UI:  http://localhost:8080  (query history, plan, metrics)"
echo "  MinIO UI:  http://localhost:9001  (minioadmin / minioadmin)"
echo
echo "  To stop the stack:"
echo "    cd trino-setup && docker compose down"
echo
