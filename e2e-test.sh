#!/bin/bash

# Test script for distributed workload scheduler
# Starts services, submits jobs, monitors until completion
# Usage: ./e2e-test.sh [NUM_CPU_JOBS] [NUM_GPU_JOBS] [NUM_CPU_WORKERS] [NUM_GPU_WORKERS]
#
# The scheduler is a 3-node Raft cluster; a follower responds 503 with a
# leader hint instead of redirecting (see scheduler_request below), so this
# script tries each known node in turn rather than assuming port 5001 is
# the leader.

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCHEDULER_URLS=("http://localhost:5001" "http://localhost:5002" "http://localhost:5003")

# CLI arguments with defaults
NUM_CPU_JOBS=${1:-3}
NUM_GPU_JOBS=${2:-1}
NUM_CPU_WORKERS=${3:-2}
NUM_GPU_WORKERS=${4:-2}
TOTAL_WORKERS=$((NUM_CPU_WORKERS + NUM_GPU_WORKERS))

SUBMITTED_JOBS=()

# Calls the scheduler cluster, trying each known node until one accepts the
# request. A follower answers 503 (with a leader hint we don't need here,
# since trying the next node converges on the leader anyway); prints the
# response body and returns non-zero if no node accepted it.
scheduler_request() {
    local method=$1
    local path=$2
    local data=$3
    local response http_code body

    for url in "${SCHEDULER_URLS[@]}"; do
        if [ -n "$data" ]; then
            response=$(curl -s -w '\n%{http_code}' -X "$method" "$url$path" \
                -H "Content-Type: application/json" -d "$data")
        else
            response=$(curl -s -w '\n%{http_code}' -X "$method" "$url$path")
        fi
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')
        if [[ "$http_code" =~ ^2 ]]; then
            echo "$body"
            return 0
        fi
    done
    echo "$body"
    return 1
}

echo "Distributed Workload Scheduler Test"
echo "===================================="
echo "Configuration:"
echo "  CPU Jobs: $NUM_CPU_JOBS"
echo "  GPU Jobs: $NUM_GPU_JOBS"
echo "  CPU Workers: $NUM_CPU_WORKERS"
echo "  GPU Workers: $NUM_GPU_WORKERS"
echo "  Total Workers: $TOTAL_WORKERS"

# Start Docker Compose
echo "Starting services..."

if docker compose ps | grep -q "Up"; then
    echo "Services already running"
    read -p "Restart? (y/N): " restart
    if [[ $restart =~ ^[Yy]$ ]]; then
        docker compose down
        docker compose up --build -d --scale cpu-executor=$NUM_CPU_WORKERS --scale gpu-executor=$NUM_GPU_WORKERS
    fi
else
    docker compose up --build -d --scale cpu-executor=$NUM_CPU_WORKERS --scale gpu-executor=$NUM_GPU_WORKERS
fi

echo "Services started"
echo ""

# Wait for services (and for a leader to be elected)
echo -n "Waiting for scheduler"
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if scheduler_request GET /health > /dev/null 2>&1; then
        echo " OK"
        break
    fi
    echo -n "."
    sleep 1
    ((attempt++))
done

if [ $attempt -eq $max_attempts ]; then
    echo " FAILED"
    echo "Scheduler did not start"
    docker compose logs scheduler-1 scheduler-2 scheduler-3 | tail -30
    exit 1
fi

# Discover executors
echo "Discovering resources..."

echo -n "Waiting for $TOTAL_WORKERS executor(s) to register"
max_wait=60
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    executors=$(scheduler_request GET /executors) || executors="[]"
    executor_count=$(echo "$executors" | jq length)

    # Check if we have all expected executors
    if [ "$executor_count" -eq "$TOTAL_WORKERS" ]; then
        echo " OK ($executor_count/$TOTAL_WORKERS)"
        break
    fi

    echo -n "."
    sleep 1
    ((elapsed++))
done
echo "Found $executor_count executor(s)"
echo ""

gpu_executors=$(echo "$executors" | jq -r '.[].gpu_type | select(. != null)' | sort | uniq)
gpu_count=$(echo "$executors" | jq '[.[].available_gpu_count] | add')

echo "Available resources:"

echo "$executors" | jq -c '.[]' | while read -r ex; do
    ip=$(echo "$ex" | jq -r .ip_address)
    cpu=$(echo "$ex" | jq -r .available_cpu_cores)
    mem=$(echo "$ex" | jq -r .available_memory_gb)
    gpus=$(echo "$ex" | jq -r .available_gpu_count)
    gpu_type=$(echo "$ex" | jq -r .gpu_type)

    if [ "$gpu_type" == "null" ] || [ -z "$gpu_type" ]; then
        echo "  $ip: ${cpu} CPU, ${mem}GB RAM"
    else
        echo "  $ip: ${cpu} CPU, ${mem}GB RAM, ${gpus}x ${gpu_type}"
    fi
done

echo ""

# Submit jobs
echo "Submitting jobs..."

submit_job() {
    local job_name=$1
    local job_data=$2

    response=$(scheduler_request POST /jobs "$job_data") || response="{}"
    job_id=$(echo "$response" | jq -r '.id')

    if [ -n "$job_id" ] && [ "$job_id" != "null" ]; then
        echo "  $job_name (${job_id:0:12})"
        SUBMITTED_JOBS+=("$job_id")
    else
        echo "  $job_name - failed to submit or parse response"
        echo "  $response"
    fi
}

# Submit CPU jobs
for ((i=1; i<=NUM_CPU_JOBS; i++)); do
    submit_job "CPU Job $i" '{
        "docker_image": "python:3.10",
        "command": ["python", "-c", "print(\"Processing data on CPU\")"],
        "cpu_cores": 2,
        "memory_gb": 4,
        "gpu_count": 0
    }'
done

# Submit GPU jobs
if [ ! -z "$gpu_executors" ] && [ $NUM_GPU_JOBS -gt 0 ]; then
    for ((i=1; i<=NUM_GPU_JOBS; i++)); do
        # Choose one of the available GPU types at random (not always the first)
        gpu_type=$(echo "$gpu_executors" | sort -R | head -1)
        submit_job "GPU Job $i ($gpu_type)" "{
            \"docker_image\": \"nvidia/cuda:11.8.0-base\",
            \"command\": [\"python\", \"-c\", \"print('Processing data on GPU')\"],
            \"cpu_cores\": 4,
            \"memory_gb\": 16,
            \"gpu_count\": 1,
            \"gpu_type\": \"$gpu_type\"
        }"
    done
else
    if [ $NUM_GPU_JOBS -gt 0 ]; then
        echo "  No GPU executors available, skipping $NUM_GPU_JOBS GPU job(s)"
    else
        echo "  NUM_GPU_JOBS=0, skipping GPU jobs"
    fi
fi

total_jobs=${#SUBMITTED_JOBS[@]}
echo ""
echo "Submitted $total_jobs jobs"

if [ $total_jobs -eq 0 ]; then
    echo "No jobs submitted"
    exit 1
fi

echo ""
echo "Monitoring jobs..."

max_wait=120
elapsed=0
last_status=""

while [ $elapsed -lt $max_wait ]; do
    completed=0
    running=0
    pending=0
    failed=0

    for job_id in "${SUBMITTED_JOBS[@]}"; do
        job=$(scheduler_request GET "/jobs/$job_id") || job="{}"
        status=$(echo "$job" | jq -r .status)

        case "$status" in
            "succeeded") ((completed++)) ;;
            "running") ((running++)) ;;
            "pending") ((pending++)) ;;
            "failed") ((failed++)) ;;
        esac
    done

    status_str="[done: $completed | running: $running | pending: $pending"
    if [ $failed -gt 0 ]; then
        status_str="$status_str | failed: $failed"
    fi
    status_str="$status_str] ${elapsed}s"

    if [ "$status_str" != "$last_status" ]; then
        echo -e "\r\033[K$status_str"
        last_status=$status_str
    fi

    if [ $((completed + failed)) -eq $total_jobs ]; then
        echo ""
        break
    fi

    sleep 2
    ((elapsed+=2))
done

echo ""
echo "Results:"

succeeded=0
failed_count=0

for job_id in "${SUBMITTED_JOBS[@]}"; do
    job=$(scheduler_request GET "/jobs/$job_id") || job="{}"
    status=$(echo "$job" | jq -r .status)
    cpu=$(echo "$job" | jq -r .cpu_cores)
    mem=$(echo "$job" | jq -r .memory_gb)
    gpu=$(echo "$job" | jq -r .gpu_count)

    short_id="${job_id:0:12}"

    case "$status" in
        "succeeded")
            echo "  $short_id: succeeded (${cpu}CPU, ${mem}GB, ${gpu}GPU)"
            ((succeeded++))
            ;;
        "failed")
            echo "  $short_id: FAILED (${cpu}CPU, ${mem}GB, ${gpu}GPU)"
            ((failed_count++))
            ;;
        "running")
            echo "  $short_id: running (${cpu}CPU, ${mem}GB, ${gpu}GPU)"
            ;;
        "pending")
            echo "  $short_id: pending (${cpu}CPU, ${mem}GB, ${gpu}GPU)"
            ;;
    esac
done

echo ""
echo "Summary"
echo "-------"
echo "Total:     $total_jobs"
echo "Succeeded: $succeeded"
echo "Failed:    $failed_count"
echo "Executors: $executor_count"
if [ ! -z "$gpu_count" ] && [ "$gpu_count" -gt 0 ]; then
    echo "GPUs:      $gpu_count"
fi
echo ""

if [ $succeeded -eq $total_jobs ]; then
    echo -e "${GREEN}All tests passed${NC}"
    exit_code=0
elif [ $failed_count -gt 0 ]; then
    echo -e "${RED}Some jobs failed${NC}"
    exit_code=1
else
    echo -e "${YELLOW}Jobs did not complete in time${NC}"
    exit_code=1
fi

echo ""
echo "Useful commands:"
echo "  docker compose logs -f"
echo "  curl http://localhost:5001/raft/status   # check which port is the leader"
echo "  docker compose down"
echo ""

exit $exit_code
