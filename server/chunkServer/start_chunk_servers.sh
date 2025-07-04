#!/bin/bash

# Read command-line arguments
NUM_SERVERS=$1
BASE_PORT=$2

# Validate inputs
if [[ -z "$NUM_SERVERS" || -z "$BASE_PORT" ]]; then
    echo "Usage: $0 <num_servers> <base_port>"
    exit 1
fi

# Start the specified number of chunk servers
for ((i=1; i<=NUM_SERVERS; i++)); do
    PORT=$((BASE_PORT + i))
    go run main.go -port="$PORT" -masterAddr="$BASE_PORT" &  
done

# Wait for all background processes to complete
wait
