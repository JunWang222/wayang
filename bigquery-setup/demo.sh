#!/usr/bin/env bash
# =============================================================================
#  Wayang BigQuery Connector — Live Demo Script
#
#  Demonstrates:
#    Act 1 — BigQuery connector structure (no cloud credentials needed)
#    Act 2 — Cost model: three-layer pipeline with BigQuery parameters
#    Act 3 — (Optional) Live query if BigQuery credentials are provided
#
#  Usage:
#    cd bigquery-setup && ./demo.sh
#    cd bigquery-setup && ./demo.sh --live   # also runs live query
#
#  For live mode, set env vars before running:
#    export BQ_URL="jdbc:bigquery://...;ProjectId=...;OAuthType=0;..."
#    export BQ_PROJECT="my-gcp-project"
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WAYANG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

LIVE_MODE=false
if [[ "${1:-}" == "--live" ]]; then
  LIVE_MODE=true
fi

BQ_URL="${BQ_URL:-}"
BQ_PROJECT="${BQ_PROJECT:-my-project}"
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

run_demo_class() {
  local main_class="$1"; shift
  local extra_props="${*:-}"
  mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
    -Dexec.mainClass="$main_class" \
    ${extra_props} \
    ${MAVEN_FLAGS} -q 2>/dev/null || true
}

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 1 — BigQuery connector structure
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 1 — BigQuery connector structure"
cd "$WAYANG_ROOT"

step "1a. Connector package layout"
echo "  BigQuery follows the same pattern as Trino — one sub-package,"
echo "  one CONFIG_NAME, everything resolves automatically."
echo
echo "  genericjdbc/"
echo "  ├── BigQuery.java               ← entry point: BigQuery.plugin()"
echo "  ├── BigQueryDemo.java           ← this demo"
echo "  ├── trino/                      ← Trino connector"
echo "  │   ├── TrinoPlatform.java      CONFIG_NAME = 'trino'"
echo "  │   └── ...                     wayang.trino.*"
echo "  └── bigquery/                   ← BigQuery connector"
echo "      ├── BigQueryPlatform.java   CONFIG_NAME = 'bigquery'"
echo "      └── ...                     wayang.bigquery.*"
echo

step "1b. Key difference from Trino: table identifier format"
echo "  Trino  :  catalog.schema.table"
echo "            e.g.  iceberg.sales.orders"
echo
echo "  BigQuery: \`project.dataset.table\`   (backtick-quoted)"
echo "            e.g.  \`my-project.sales.orders\`"
echo
echo "  The backtick quoting is handled by BigQueryTableSource automatically."

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 2 — Cost model (no credentials needed)
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 2 — BigQuery cost model + plan structure"

step "Running BigQueryDemo.main() — Seg 3 (cost model) + Seg 4 (plan)"
echo "  No BigQuery credentials required for this part."
echo

run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
  "-Dbigquery.project=${BQ_PROJECT}"

pause

# ═════════════════════════════════════════════════════════════════════════════
#  ACT 3 — Live query (optional, requires credentials)
# ═════════════════════════════════════════════════════════════════════════════
banner "ACT 3 — Live BigQuery query (optional)"

if [[ "$LIVE_MODE" == true && -n "$BQ_URL" ]]; then
  step "Running live query against BigQuery via Wayang API"
  echo "  Project : $BQ_PROJECT"
  echo "  Table   : \`${BQ_PROJECT}.sales.orders\`"
  echo
  run_demo_class "org.apache.wayang.genericjdbc.BigQueryDemo" \
    "-Dbigquery.url=${BQ_URL}" \
    "-Dbigquery.project=${BQ_PROJECT}"
else
  echo "  Live mode not active."
  echo
  echo "  To run a real query against BigQuery:"
  echo "    export BQ_PROJECT='your-gcp-project'"
  echo "    export BQ_URL='jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443"
  echo "                   ;ProjectId=your-project"
  echo "                   ;OAuthType=0"
  echo "                   ;OAuthServiceAcctEmail=sa@project.iam.gserviceaccount.com"
  echo "                   ;OAuthPvtKeyPath=/path/to/key.json'"
  echo "    ./demo.sh --live"
  echo
  echo "  Prerequisites: upload data to BigQuery first:"
  echo "    bq mk --dataset \${BQ_PROJECT}:sales"
  echo "    bq load --source_format=CSV \${BQ_PROJECT}:sales.orders data.csv schema.json"
fi

# ─────────────────────────────────────────────────────────────────────────────
banner "Demo complete"
echo "  BigQuery connector shows the same Wayang API as Trino."
echo "  The only user-visible differences are:"
echo "    1. BigQuery.plugin()  instead of  Trino.plugin()"
echo "    2. BigQueryTableSource('\`project.dataset.table\`', ...)"
echo "    3. JDBC URL with OAuth credentials in wayang.bigquery.jdbc.url"
echo
echo "  Cost parameters differ (α=5, β=2M vs Trino α=10, β=800k)"
echo "  so the optimizer correctly prefers BigQuery for large scans"
echo "  and Trino for smaller interactive queries."
echo
