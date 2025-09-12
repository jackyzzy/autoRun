#!/bin/bash

# 基线性能测试执行脚本
# 此脚本演示如何手动运行基准测试（playbook会自动处理）

set -e

echo "Starting baseline performance test..."

# 测试配置
BASE_URL="http://183.56.181.9:34451"
MODEL="/data/Qwen3-235B-A22B"
RESULTS_PATH="/home/zjwei/benchmark/results"

# 确保结果目录存在
mkdir -p "${RESULTS_PATH}"

# 运行AICP基准测试
docker run -it --rm \
  --network host \
  --ipc=host \
  --privileged=true \
  -v "${RESULTS_PATH}:/benchmark/data/results" \
  sangfor.com/aicp-benchmark:v0.0.6 \
  --base-url "${BASE_URL}" \
  --model "${MODEL}" \
  --tokenizer-path ./tokenizer \
  --dataset-path ShareGPT_V3_10000.json \
  --dataset-name sharegpt \
  --num-prompts 1000 \
  --max-concurrency 20 \
  --sharegpt-input-len 4096 \
  --sharegpt-output-len 1024 \
  --sharegpt-prompt-len-scale 0.1 \
  --enable-same-prompt \
  --metadata scenario=baseline_test arch=x86 gpu="NVIDIA H200" gpu_num=4 replicas=2 backend=sglang410 config_type=baseline

echo "Baseline performance test completed!"
echo "Results saved to: ${RESULTS_PATH}"

# 检查测试结果文件
if [ -d "${RESULTS_PATH}" ]; then
    echo "Result files:"
    find "${RESULTS_PATH}" -name "*.json" -o -name "*.csv" -o -name "*.log" | head -10
else
    echo "Warning: Results directory not found"
fi