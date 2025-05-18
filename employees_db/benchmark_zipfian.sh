#!/bin/bash
#
# MySQLSlap Benchmark Script with Zipfian Distribution
#
# Description:
# This script benchmarks a set of SQL queries using `mysqlslap`, running them in parallel
# and reporting Queries Per Second (QPS) for each query individually.
#
# Key Features:
# - Runs 15 query files concurrently with a Zipfian distribution for query execution.
# - Collects average execution time per query from `mysqlslap` output.
# - Calculates and displays QPS for each query based on actual timing.
# - Removes MySQL password warning lines from output files.
# - Summarizes total combined QPS across all queries.
#
# Configuration:
# - Update USER, PASS, HOST, PORT, and DB variables to match your MySQL instance.
# - Place SQL files (named q1.sql through q15.sql) in the same directory as this script.
#
# Output:
# - Stores raw output for each query in ./mysqlslap_results/
# - Displays formatted QPS results in the terminal
#
# Usage:
#   chmod +x benchmark_zipfian.sh
#   ./benchmark_zipfian.sh
#
# Dependencies:
# - mysqlslap (part of MySQL client tools)
# - Python for generating the Zipfian distribution
#

# Database connection details
USER="sysbench"
PASS="sysbench"
HOST="127.0.0.1"
PORT="22233"
DB="employees"

# Query files (q1.sql through q15.sql)
QUERIES=("q1.sql" "q2.sql" "q3.sql" "q4.sql" "q5.sql" "q6.sql" "q7.sql" "q8.sql" "q9.sql" "q10.sql" "q11.sql" "q12.sql" "q13.sql" "q14.sql" "q15.sql")

# Total iterations and concurrency settings
TOTAL_RUNS=3000
TOTAL_QUERIES=${#QUERIES[@]}

# Temporary directory to store outputs
TMP_DIR="./mysqlslap_results"
mkdir -p "$TMP_DIR"

# Generate Zipfian distribution using Python
DISTRIBUTION=$(python3 -c "import numpy as np; print(' '.join(map(str, np.random.zipf(1.5, $TOTAL_RUNS))))")

# Run queries based on Zipfian distribution
echo "Running queries with Zipfian distribution..."

for QUERY_INDEX in $DISTRIBUTION; do
  # Ensure index is within range
  if (( QUERY_INDEX <= TOTAL_QUERIES )); then
    QUERY_FILE="${QUERIES[$((QUERY_INDEX - 1))]}"
    BASENAME=$(basename "$QUERY_FILE" .sql)
    OUTPUT_FILE="$TMP_DIR/${BASENAME}.out"

    # Run mysqlslap with concurrency set to 1 to maintain Zipfian distribution
    mysqlslap -u "$USER" -p"$PASS" -h "$HOST" -P "$PORT" \
      --concurrency=1 \
      --iterations=1 \
      --query="$(cat "$QUERY_FILE")" \
      --delimiter=";" \
      --create-schema="$DB" >> "$OUTPUT_FILE" 2>&1 &
  fi

  # Throttle to avoid overwhelming the server
  sleep 0.1

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
    QPS=$(awk "BEGIN {printf \"%.2f\", (1 / $AVG_TIME)}")
    printf "%-10s : %10s QPS\n" "$QUERY_NAME" "$QPS"
    TOTAL_QPS=$(awk "BEGIN {print $TOTAL_QPS + $QPS}")
  else
    echo "$QUERY_NAME: QPS not available or query failed"
  fi
done

echo "-------------------------------------------------"
echo "Total estimated QPS (sum): $TOTAL_QPS"
echo "================================================="
