#!/bin/bash
curl -s http://183.56.181.9:34441/v1/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama","prompt":"hi","max_tokens":16,"temperature":0}' | jq .

echo "$(date '+%Y-%m-%d %H:%M:%S') Test completed" > /tmp/test_complete.txt