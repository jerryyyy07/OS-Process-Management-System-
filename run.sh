#!/bin/bash
# ==================================================================
# run.sh - Orchestrator for the OS Pipeline
# ==================================================================

# Bash strictly exits on uninitialized variables
set -u

# Function: Display usage
usage() {
    echo "Usage: $0 -i <input_dir> -o <output_dir> -n <num_threads> -q <queue_size> [-c]"
    echo "  -i  Input directory containing CSV files"
    echo "  -o  Output directory for reports"
    echo "  -n  Number of worker threads for processor"
    echo "  -q  Queue size for bounded buffer"
    echo "  -c  Clean build before running"
    exit 1
}

# Function: Cleanup trap
cleanup() {
    echo "[run.sh] Trap triggered. Cleaning up background processes..."
    if [ -f dispatcher.pid ]; then
        DPID=$(cat dispatcher.pid)
        kill -TERM "$DPID" 2>/dev/null
        rm -f dispatcher.pid
    fi
}

# Function: Check dependencies
check_deps() {
    if ! command -v gcc &> /dev/null || ! command -v make &> /dev/null; then
        echo "Error: gcc and make are required."
        exit 1
    fi
}

# Register the trap for EXIT, INT, and TERM signals
trap cleanup EXIT INT TERM

# Parse arguments using getopts
INPUT_DIR=""
OUTPUT_DIR=""
THREADS=""
QUEUE_SIZE=""
CLEAN=0

while getopts "i:o:n:q:ch" opt; do
    case ${opt} in
        i ) INPUT_DIR=$OPTARG ;;
        o ) OUTPUT_DIR=$OPTARG ;;
        n ) THREADS=$OPTARG ;;
        q ) QUEUE_SIZE=$OPTARG ;;
        c ) CLEAN=1 ;;
        h ) usage ;;
        * ) usage ;;
    esac
done

if [ -z "$INPUT_DIR" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$THREADS" ] || [ -z "$QUEUE_SIZE" ]; then
    usage
fi

# Ensure input directory contains at least one CSV
if ! ls "$INPUT_DIR"/*.csv &> /dev/null; then
    echo "Error: No .csv files found in $INPUT_DIR"
    exit 1
fi

check_deps

# Clean if requested
if [ "$CLEAN" -eq 1 ]; then
    make clean 2>/dev/null || true
fi

# Build project
echo "Building project..."
if ! make; then
    echo "Error: make failed."
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

# Launch dispatcher in the background
FIFO_PATH="/tmp/retail_fifo_$$"
SHM_NAME="/retail_shm_$$"
SEM_NAME="/retail_sem_$$"

./dispatcher "$INPUT_DIR" "$OUTPUT_DIR" "$THREADS" "$QUEUE_SIZE" "$FIFO_PATH" "$SHM_NAME" "$SEM_NAME" &
DISPATCHER_PID=$!
echo "$DISPATCHER_PID" > dispatcher.pid

echo "Dispatcher launched with PID $DISPATCHER_PID. Waiting for completion..."
wait $DISPATCHER_PID
EXIT_STATUS=$?

rm -f dispatcher.pid

# Arithmetic expansion and output check
if [ -f "$OUTPUT_DIR/report.csv" ]; then
    RECORDS_PROC=$(tail -n +2 "$OUTPUT_DIR/report.csv" | awk -F',' '{sum+=$3} END {print sum}')
    if [ -z "$RECORDS_PROC" ]; then RECORDS_PROC=0; fi
    let TOTAL_PROC=$RECORDS_PROC+0
    echo "[run.sh] Pipeline finished with exit code $EXIT_STATUS. Processed $TOTAL_PROC records."
else
    echo "[run.sh] Pipeline finished with exit code $EXIT_STATUS. No report generated."
fi