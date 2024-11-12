#!/bin/bash

# Configuration
THRESHOLD=80
MEASUREMENTS_FILE="/tmp/cpu_measurements.txt"
MONITOR_INTERVAL=5  # seconds
PROCESS_NAME="cdk-erigon"
DETAILED_LOG="/tmp/cpu_detailed.log"

# Function to get CPU usage for all matching processes
get_process_cpu() {
    # Clear previous detailed log
    > "$DETAILED_LOG"

    # Get PIDs of cdk-erigon processes
    pids=$(pgrep -f "[c]dk-erigon")

    if [ -n "$pids" ]; then
        # Use top in batch mode for each PID to get current CPU usage
        for pid in $pids; do
            # Get process command
            if [[ "$OSTYPE" == "darwin"* ]]; then
                cmd=$(ps -p $pid -o command=)
                cpu=$(top -l 1 -pid $pid | tail -1 | awk '{print $3}')
            else
                cmd=$(ps -p $pid -o cmd=)
                cpu=$(top -b -n 1 -p $pid | tail -1 | awk '{print $9}')
            fi
            # Get current CPU usage
            echo "$pid $cpu $cmd" >> "$DETAILED_LOG"
        done
    fi

    # Sum total CPU usage
    total_cpu=$(awk '{sum += $2} END {printf "%.1f", sum}' "$DETAILED_LOG")

    # Return 0 if no process found
    if [ -z "$total_cpu" ]; then
        echo "0.0"
    else
        echo "$total_cpu"
    fi
}

# Function to show current process details
show_process_details() {
    if [ -s "$DETAILED_LOG" ]; then
        echo "Individual process details:"
        printf "%-10s %-8s %-s\n" "PID" "CPU%" "Command"
        echo "----------------------------------------"
        while read -r line; do
            pid=$(echo "$line" | awk '{print $1}')
            cpu=$(echo "$line" | awk '{print $2}')
            cmd=$(echo "$line" | cut -d' ' -f3-)
            printf "%-10s %-8.1f %-s\n" "$pid" "$cpu" "$cmd"
        done < "$DETAILED_LOG"
        echo "----------------------------------------"
    else
        echo "No $PROCESS_NAME processes found"
    fi
}

# Function to analyze CPU measurements
analyze_cpu() {
    if [ -f "$MEASUREMENTS_FILE" ]; then
        # Calculate statistics
        avg_cpu=$(awk '{ sum += $1 } END { print sum/NR }' "$MEASUREMENTS_FILE")
        avg_cpu_rounded=$(printf "%.1f" "$avg_cpu")
        max_cpu=$(awk 'BEGIN{max=0} {if($1>max) max=$1} END{print max}' "$MEASUREMENTS_FILE")
        measurement_count=$(wc -l < "$MEASUREMENTS_FILE")

        echo ""
        echo "=== CPU Usage Analysis for all $PROCESS_NAME processes ==="
        echo "Number of measurements: $measurement_count"
        echo "Average Combined CPU Usage: $avg_cpu_rounded%"
        echo "Peak Combined CPU Usage: $max_cpu%"
        echo "Threshold: $THRESHOLD%"

        # Get final process details for the report
        echo ""
        echo "Final process state:"
        show_process_details

        # Compare with threshold
        if [ "$(echo "$avg_cpu > $THRESHOLD" | bc -l)" -eq 1 ]; then
            echo ""
            echo "ERROR: Average CPU usage ($avg_cpu_rounded%) exceeded threshold of $THRESHOLD%"
            cleanup_and_exit 1
        else
            echo ""
            echo "SUCCESS: CPU usage ($avg_cpu_rounded%) is within threshold of $THRESHOLD%"
            cleanup_and_exit 0
        fi
    else
        echo "ERROR: No CPU measurements found at $MEASUREMENTS_FILE"
        cleanup_and_exit 1
    fi
}

# Function to clean up and exit
cleanup_and_exit() {
    exit_code=$1
    rm -f "$DETAILED_LOG"
    exit $exit_code
}

# Function to handle interruption
handle_interrupt() {
    echo ""
    echo "Monitoring interrupted. Analyzing collected data..."
    analyze_cpu
}

# Set up trap for various signals
trap handle_interrupt TERM INT

# Clear measurements file
> "$MEASUREMENTS_FILE"
> "$DETAILED_LOG"

echo "Starting CPU monitoring for all '$PROCESS_NAME' processes"
echo "Storing measurements in $MEASUREMENTS_FILE"
echo "Monitoring interval: ${MONITOR_INTERVAL}s"
echo "Press Ctrl+C to stop monitoring and see analysis"
echo ""

# Start monitoring loop
while true; do
    # Get CPU usage for all matching processes
    cpu_usage=$(get_process_cpu)
    echo "$cpu_usage" >> "$MEASUREMENTS_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Combined CPU Usage: $cpu_usage%"
    show_process_details
    echo ""
    sleep "$MONITOR_INTERVAL"
done