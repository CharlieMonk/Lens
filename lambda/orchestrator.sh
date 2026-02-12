#!/bin/bash
# eCFR Lambda Orchestrator
# Fetches all CFR titles for current and historical years
# Optimized for Lambda concurrency limit (default 10)

set -e

FUNCTION_NAME="${ECFR_FUNCTION_NAME:-ecfr-dev-fetcher}"
MAX_WORKERS="${ECFR_MAX_WORKERS:-10}"
YEARS="${ECFR_YEARS:-0 2025 2020 2015 2010 2005 2000}"  # 0 = current
RESULTS_DIR="${ECFR_RESULTS_DIR:-/tmp/ecfr_fetch_$(date +%Y%m%d_%H%M%S)}"

# Title categories (ordered by expected duration)
LARGE_TITLES="40 26 42 45 48"
MEDIUM_TITLES="7 12 14 17 21 29 49"
SMALL_TITLES="1 2 3 4 5 6 8 9 10 11 13 15 16 18 19 20 22 23 24 25 27 28 30 31 32 33 34 36 37 38 39 41 43 44 46 47 50"

# Build priority-ordered title list
TITLES="$LARGE_TITLES $MEDIUM_TITLES $SMALL_TITLES"

mkdir -p "$RESULTS_DIR"

echo "========================================================================"
echo "eCFR LAMBDA ORCHESTRATOR"
echo "========================================================================"
echo "Started: $(date)"
echo "Function: $FUNCTION_NAME"
echo "Workers: $MAX_WORKERS concurrent"
echo "Years: $YEARS"
echo "Results: $RESULTS_DIR"
echo "========================================================================"

GRAND_START=$(date +%s.%N)
TOTAL_SUCCESSFUL=0
TOTAL_FAILED=0
TOTAL_SECTIONS=0

# Worker function
invoke_title() {
    local title=$1
    local year=$2
    local output_dir=$3

    local OUTPUT_FILE="$output_dir/title-$title.json"
    local INVOKE_START=$(date +%s.%N)

    aws lambda invoke \
        --function-name "$FUNCTION_NAME" \
        --payload "{\"title\": $title, \"year\": $year}" \
        --cli-binary-format raw-in-base64-out \
        --cli-read-timeout 300 \
        "$OUTPUT_FILE" >/dev/null 2>&1

    local INVOKE_END=$(date +%s.%N)
    local INVOKE_TIME=$(echo "$INVOKE_END - $INVOKE_START" | bc)

    if [ -f "$OUTPUT_FILE" ]; then
        local success=$(jq -r '.success // "false"' "$OUTPUT_FILE" 2>/dev/null)
        local status=$(jq -r '.status // "unknown"' "$OUTPUT_FILE" 2>/dev/null)
        local sections=$(jq -r '.sections_count // .sections // 0' "$OUTPUT_FILE" 2>/dev/null)

        if [ "$success" = "true" ] || [ "$status" = "success" ] || [ "$status" = "empty" ]; then
            printf "    [OK] Title %2d: %5s sections (%.1fs)\n" "$title" "$sections" "$INVOKE_TIME"
            echo "$sections" >> "$output_dir/.sections_count"
        else
            printf "    [FAIL] Title %2d: %s\n" "$title" "$(jq -r '.errors[-1].message // "unknown"' "$OUTPUT_FILE" 2>/dev/null)"
            echo "1" >> "$output_dir/.failed_count"
        fi
    else
        printf "    [FAIL] Title %2d: No response\n" "$title"
        echo "1" >> "$output_dir/.failed_count"
    fi
}

# Process each year
for year in $YEARS; do
    year_dir="$RESULTS_DIR/year-$year"
    mkdir -p "$year_dir"

    if [ "$year" = "0" ]; then
        echo ""
        echo "Fetching CURRENT year..."
    else
        echo ""
        echo "Fetching year $year..."
    fi

    YEAR_START=$(date +%s.%N)

    # Reset counters
    > "$year_dir/.sections_count"
    > "$year_dir/.failed_count"

    # Use semaphore pattern for controlled parallelism
    RUNNING=0
    for title in $TITLES; do
        while [ $RUNNING -ge $MAX_WORKERS ]; do
            wait -n 2>/dev/null || true
            RUNNING=$((RUNNING - 1))
        done

        invoke_title $title $year "$year_dir" &
        RUNNING=$((RUNNING + 1))
    done

    wait

    YEAR_END=$(date +%s.%N)
    YEAR_TIME=$(echo "$YEAR_END - $YEAR_START" | bc)

    # Count results
    YEAR_SECTIONS=$(awk '{sum+=$1} END {print sum}' "$year_dir/.sections_count" 2>/dev/null || echo 0)
    YEAR_FAILED=$(wc -l < "$year_dir/.failed_count" 2>/dev/null || echo 0)
    YEAR_SUCCESS=$((49 - YEAR_FAILED))  # 49 titles (excluding reserved 35)

    echo "  Year summary: $YEAR_SUCCESS/49 titles, $YEAR_SECTIONS sections, ${YEAR_TIME}s"

    TOTAL_SECTIONS=$((TOTAL_SECTIONS + YEAR_SECTIONS))
    TOTAL_SUCCESSFUL=$((TOTAL_SUCCESSFUL + YEAR_SUCCESS))
    TOTAL_FAILED=$((TOTAL_FAILED + YEAR_FAILED))
done

GRAND_END=$(date +%s.%N)
GRAND_TIME=$(echo "$GRAND_END - $GRAND_START" | bc)

echo ""
echo "========================================================================"
echo "FINAL SUMMARY"
echo "========================================================================"
echo "Years processed: $(echo $YEARS | wc -w)"
echo "Total titles: $((TOTAL_SUCCESSFUL + TOTAL_FAILED))"
echo "Successful: $TOTAL_SUCCESSFUL"
echo "Failed: $TOTAL_FAILED"
echo "Total sections: $TOTAL_SECTIONS"
echo "Total time: ${GRAND_TIME}s ($(echo "scale=1; $GRAND_TIME / 60" | bc)m)"
echo ""
echo "Completed: $(date)"
echo "Results saved to: $RESULTS_DIR"
echo "========================================================================"

# Exit with error if any failures
[ "$TOTAL_FAILED" -gt 0 ] && exit 1 || exit 0
