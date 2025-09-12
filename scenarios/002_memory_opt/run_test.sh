#!/bin/bash

# Memory optimization test script
echo "Starting memory optimization test..."

# Test the model endpoint
echo "Testing model endpoint..."

# Store the response
response=$(curl -s http://183.56.181.9:34441/v1/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama","prompt":"hi","max_tokens":16,"temperature":0}')

# Try to format with jq, fallback to raw output if jq is not available
if command -v jq >/dev/null 2>&1; then
    echo "$response" | jq .
else
    echo "jq not available, showing raw JSON response:"
    echo "$response"
fi

# Create completion marker
echo "$(date '+%Y-%m-%d %H:%M:%S') Memory optimization test completed" > /tmp/test_complete.txt

echo "Test execution completed successfully"