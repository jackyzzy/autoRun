#!/bin/bash

# 场景测试执行脚本模板
# 复制此模板并根据具体场景需求进行修改

set -e

echo "Starting template scenario test..."

# 测试配置 - 根据场景需求修改
BASE_URL="http://10.112.0.201:18008"
MODEL="/data/your-model-path"

# 场景特定参数（可以从环境变量获取，也可以硬编码）
NUM_PROMPTS=1000
MAX_CONCURRENCY=20

# 使用系统提供的环境变量
echo "🚀 开始执行场景: $SCENARIO_NAME"
echo "📁 工作目录: $SCENARIO_PATH"
echo "💾 结果路径: $SCENARIO_RESULT_PATH"

# 确保结果目录存在
mkdir -p "${SCENARIO_RESULT_PATH}"

# 运行LLM性能基准测试
docker run -it --rm \
  --network host \
  --ipc=host \
  --privileged=true \
  -v "${SCENARIO_RESULT_PATH}:/benchmark/data/results" \
  jacky.com/benchmark:v0.0.6 \
  --base-url "${BASE_URL}" \
  --model "${MODEL}" \
  --tokenizer-path ./tokenizer \
  --dataset-path ShareGPT_V3_10000.json \
  --dataset-name sharegpt \
  --num-prompts "${NUM_PROMPTS}" \
  --max-concurrency "${MAX_CONCURRENCY}" \
  --sharegpt-input-len 4096 \
  --sharegpt-output-len 1024 \
  --sharegpt-prompt-len-scale 0.1 \
  --enable-same-prompt \
  --metadata scenario="${SCENARIO_NAME}" arch=x86 gpu="NVIDIA H200" gpu_num=4 replicas=2 backend=sglang410 config_type=template

echo "✅ Template scenario test completed!"
echo "💾 Results saved to: ${SCENARIO_RESULT_PATH}"

# 检查测试结果文件
if [ -d "${SCENARIO_RESULT_PATH}" ]; then
    echo "📋 Result files:"
    find "${SCENARIO_RESULT_PATH}" -name "*.json" -o -name "*.csv" -o -name "*.log" | head -10
else
    echo "⚠️  Warning: Results directory not found"
fi