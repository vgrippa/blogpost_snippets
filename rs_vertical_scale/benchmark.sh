#!/bin/bash
#
# MySQLSlap Benchmark Script for Multiple Queries
#
# Description:
# This script benchmarks a set of SQL queries using `mysqlslap`, running them in parallel
# and reporting Queries Per Second (QPS) for each query individually.
#
# Key Features:
# - Runs 10 query files concurrently with an evenly split total concurrency (default: 200).
# - Collects average execution time per query from `mysqlslap` output.
# - Calculates and displays QPS for each query based on actual timing.
# - Removes MySQL password warning lines from output files.
# - Summarizes total combined QPS across all queries.
#
# Configuration:
# - Update USER, PASS, HOST, PORT, and DB variables to match your MySQL instance.
# - Place SQL files (named q1.sql through q10.sql) in the same directory as this script.
#
# Output:
# - Stores raw output for each query in ./mysqlslap_results/
# - Displays formatted QPS results in the terminal
#
# Usage:
#   chmod +x benchmark.sh
#   ./benchmark.sh
#
# Dependencies:
# - mysqlslap (part of MySQL client tools)
#
# Note:
# If your mysqlslap output shows "Average number of queries per client: 1", then the 
# --iterations parameter may not be honored as expected depending on your MySQL version.
# You can manually verify iterations using the raw output files in ./mysqlslap_results/.
#

# Database connection details
USER="sysbench"
PASS="sysbench"
HOST="172.25.31.248"
PORT="3306"
DB="employees"

# Query files (q1.sql through q10.sql)
QUERIES=("q1.sql" "q2.sql" "q3.sql" "q4.sql" "q5.sql" "q6.sql" "q7.sql" "q8.sql" "q9.sql" "q10.sql")

# Total concurrency and iterations per query
TOTAL_CONCURRENCY=200
CONCURRENCY_PER_QUERY=$(( TOTAL_CONCURRENCY / ${#QUERIES[@]} ))
ITERATIONS=10

# Temporary directory to store outputs
TMP_DIR="./mysqlslap_results"
mkdir -p "$TMP_DIR"

echo "Running queries with $CONCURRENCY_PER_QUERY concurrency each..."

# Run each query in parallel
for QUERY_FILE in "${QUERIES[@]}"; do
  BASENAME=$(basename "$QUERY_FILE" .sql)
  OUTPUT_FILE="$TMP_DIR/${BASENAME}.out"

  mysqlslap -u "$USER" -p"$PASS" -h "$HOST" -P "$PORT" \
    --concurrency="$CONCURRENCY_PER_QUERY" \
    --iterations="$ITERATIONS" \
    --query="$(cat "$QUERY_FILE")" \
    --delimiter=";" \
    --create-schema="$DB" > "$OUTPUT_FILE" 2>&1 &

done

# Wait for all background jobs to complete
wait

# Remove warning lines from output files
sed -i '/Using a password on the command line interface can be insecure/d' "$TMP_DIR"/*.out

echo
echo "================== QPS Results =================="

# Extract and display QPS from each output
TOTAL_QPS=0
for OUTPUT in "$TMP_DIR"/*.out; do
  QUERY_NAME=$(basename "$OUTPUT" .out)
  AVG_TIME=$(grep -i 'Average number of seconds to run all queries' "$OUTPUT" | awk -F ':' '{print $2}' | xargs)

  if [[ -n "$AVG_TIME" ]]; then
    QPS=$(awk "BEGIN {printf \"%.2f\", ($CONCURRENCY_PER_QUERY * $ITERATIONS) / $AVG_TIME}")
    printf "%-10s : %10s QPS\n" "$QUERY_NAME" "$QPS"
    TOTAL_QPS=$(awk "BEGIN {print $TOTAL_QPS + $QPS}")
  else
    echo "$QUERY_NAME: QPS not available or query failed"
  fi
done

echo "-------------------------------------------------"
echo "Total estimated QPS (sum): $TOTAL_QPS"
echo "================================================="

# Optional: clean up
# rm -r "$TMP_DIR"
