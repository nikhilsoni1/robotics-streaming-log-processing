#!/bin/bash

# Find and delete all .mcap files in the current directory and subdirectories
find . -type f -name "*.mcap" -exec rm -v {} \;

echo "All .mcap files deleted."
