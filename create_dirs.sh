#!/bin/bash

# Define the directories to be created
DIRS=(
    "./data/zookeeper"
    "./data/bookkeeper"
)

# Loop through the directories and create them if they don't exist
for dir in "${DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "Created: $dir"
    else
        echo "Already exists: $dir"
    fi
done
