#!/bin/bash

# Check if a custom .env file is provided as an argument
ENV_FILE=".env"
if [ ! -z "$1" ]; then
  ENV_FILE="$1"
fi

# Check if the specified .env file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: $ENV_FILE file not found"
  exit 1
fi

# Export variables from the specified .env file
while IFS='=' read -r key value; do
  # Ignore comments and empty lines
  if [[ -z "$key" || "$key" =~ ^# ]]; then
    continue
  fi

  # Remove leading/trailing whitespace
  key=$(echo "$key" | xargs)
  value=$(echo "$value" | xargs)

  # Export the variable
  export "$key=$value"

  # Echo the key-value pair
  echo "Set: $key=$value"

done < "$ENV_FILE"

echo "Environment variables loaded from $ENV_FILE"
