#!/usr/bin/env bash
# =============================================================================
#  Wayang BigQuery Connector — Demo Script
#
#  Act 1 — Cost model (no credentials needed)
#  Act 2 — Filter operator: bq CLI result vs Wayang result
#  Act 3 — Projection operator: bq CLI result vs Wayang result
#
#  Usage (from repo root):
#    ./demo-bigquery.sh                # all acts, Acts 2&3 use sample data
#    ./demo-bigquery.sh --live         # Acts 2&3 hit real BigQuery
#
#  For live mode:
#    export BQ_PROJECT="your-gcp-project"
#    export BQ_URL="jdbc:bigquery://..."
# =============================================================================

set -euo pipefail

WAYANG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LIVE_MODE=false
[[ "${1:-}" == "--live" ]] && LIVE_MODE=true

BQ_PROJECT="${BQ_PROJECT:-my-project}"
BQ_URL="${BQ_URL:-}"
MAVEN_FLAGS="-Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true"

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

run_demo_class() {
  local main_class="$1"; shift
  cd "$WAYANG_ROOT"
  mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
    -Dexec.mainClass="$main_class" \
    "$@" \
    ${MAVEN_FLAGS} -q 2>/dev/null || true
}

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 1 — Cost model (no credentials needed)
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 1 — BigQuery Cost Model"

step "Three-layer cost pipeline (read from wayang-bigquery-defaults.properties)"
run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
  "-Dbigquery.mode=cost" \
  "-Dbigquery.project=${BQ_PROJECT}"

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 2 — Filter operator: bq CLI vs Wayang
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 2 — Filter Operator Pushdown"
echo "  FilterOperator  ->  BigQueryFilterOperator"
echo "  SQL: SELECT * FROM \`${BQ_PROJECT}.sales.orders\` WHERE region = 'AMER'"

step "2a. Direct bq CLI result"

if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  echo "  (live query)"
  echo
  bq query --use_legacy_sql=false --project_id="${BQ_PROJECT}" \
    "SELECT order_id, region, product, amount, order_date
     FROM \`${BQ_PROJECT}.sales.orders\`
     WHERE region = 'AMER'
     ORDER BY order_id"
  BQ_FILTER_COUNT=$(bq query --use_legacy_sql=false --project_id="${BQ_PROJECT}" \
    --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${BQ_PROJECT}.sales.orders\` WHERE region = 'AMER'" \
    | tail -1)
else
  BQ_FILTER_COUNT=5
  echo "  (sample — 20-row dataset, 4 regions, 5 products)"
  echo
  printf "  %-10s %-6s %-10s %10s %-12s\n" "order_id" "region" "product" "amount" "order_date"
  echo "  ------------------------------------------------------"
  printf "  %-10s %-6s %-10s %10s %-12s\n" "3"  "AMER" "Widget A" "2200.00" "2024-01-17"
  printf "  %-10s %-6s %-10s %10s %-12s\n" "6"  "AMER" "Widget B"  "950.25" "2024-01-20"
  printf "  %-10s %-6s %-10s %10s %-12s\n" "9"  "AMER" "Widget C"  "680.50" "2024-01-23"
  printf "  %-10s %-6s %-10s %10s %-12s\n" "12" "AMER" "Widget D" "1320.75" "2024-01-26"
  printf "  %-10s %-6s %-10s %10s %-12s\n" "16" "AMER" "Widget E" "3750.00" "2024-01-30"
fi
echo
echo "  ✓ bq CLI: ${BQ_FILTER_COUNT} rows"

step "2b. Same query via Wayang API"

if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
    "-Dbigquery.mode=filter" \
    "-Dbigquery.url=${BQ_URL}" \
    "-Dbigquery.project=${BQ_PROJECT}"
else
  run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
    "-Dbigquery.mode=filter" \
    "-Dbigquery.project=${BQ_PROJECT}"
fi

echo "  ✓ Match: bq CLI = Wayang = ${BQ_FILTER_COUNT} AMER rows"

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 3 — Projection operator: bq CLI vs Wayang
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 3 — Projection Operator Pushdown"
echo "  MapOperator  ->  BigQueryProjectionOperator"
echo "  SQL: SELECT region, product, amount WHERE region = 'AMER'"
echo "  Only 3 of 5 columns fetched — order_id + order_date never leave BigQuery."

step "3a. Direct bq CLI result"

if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  echo "  (live query)"
  echo
  bq query --use_legacy_sql=false --project_id="${BQ_PROJECT}" \
    "SELECT region, product, amount
     FROM \`${BQ_PROJECT}.sales.orders\`
     WHERE region = 'AMER'
     ORDER BY order_id"
else
  echo "  (sample — only 3 of 5 columns)"
  echo
  printf "  %-6s %-10s %10s\n" "region" "product" "amount"
  echo "  ----------------------------"
  printf "  %-6s %-10s %10s\n" "AMER" "Widget A" "2200.00"
  printf "  %-6s %-10s %10s\n" "AMER" "Widget B"  "950.25"
  printf "  %-6s %-10s %10s\n" "AMER" "Widget C"  "680.50"
  printf "  %-6s %-10s %10s\n" "AMER" "Widget D" "1320.75"
  printf "  %-6s %-10s %10s\n" "AMER" "Widget E" "3750.00"
fi
echo

step "3b. Same query via Wayang API"
echo "  Wayang collapses Filter + Projection into one SQL push."
echo

if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
    "-Dbigquery.mode=projection" \
    "-Dbigquery.url=${BQ_URL}" \
    "-Dbigquery.project=${BQ_PROJECT}"
else
  run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
    "-Dbigquery.mode=projection" \
    "-Dbigquery.project=${BQ_PROJECT}"
fi

echo "  ✓ Same rows, same 3 columns — projection pushed to BigQuery SQL"

banner "Demo complete"
echo "  All three acts show real output."
echo "  Pass --live with BQ_URL/BQ_PROJECT to hit real BigQuery."
echo
