# benchmark
Benchmark for LLM

## 安装
在项目根目录下，执行`python setup.py bdist_wheel`，生成wheel包；

执行安装 `pip install dist/benchmark-0.0.7-py3-none-any.whl`

## 使用示例

### Static Benchmark
```shell
# static
benchmark --model /data/Qwen3-235B-A22B-Instruct-2507-FP8 \
--base-url http://127.0.0.1:30007 \
--result-dir /workspace/result \
--result-dirname "defualt_params" \
--dataset-path /workspace/dataset/filtered.json \
--dataset-name filtered \
--max-concurrency 1,2 \
--input-len 1024 \
--output-len 1024 \
--goodput '{"mean_TTFT":10000, "mean_TPOT":100}' \
--stop-slo '{"mean_TTFT":15000, "mean_TPOT":150}' \
--metadata arch=x86 gpu="NVIDIA H20" gpu_num=4 \
  replicas=1 backend=sgalng410 other_params="default"
```

### Dynamic Benchmark
```shell
# dynamic
benchmark --model /data/Qwen3-235B-A22B-Instruct-2507-FP8 \
--base-url http://127.0.0.1:30007 \
--result-dir /workspace/result \
--result-dirname "defualt_params" \
--dataset-path /workspace/dataset/ShareGPT_V3_10000.json \
--dataset-name sharegpt \
--num-prompts 200 \
--max-concurrency 100,200 \
--tokenizer-path /workspace/dataset/tokenizer \
--dynamic-input-len 1024 \
--dynamic-output-len 1024 \
--dynamic-prompt-len-scale 0.1 \
--enable-same-prompt \
--dynamic-strategy "fast" \
--goodput '{"mean_TTFT":10000, "mean_TPOT":100}' \
--stop-slo '{"mean_TTFT":15000, "mean_TPOT":150}' \
--metadata arch=x86 gpu="NVIDIA H20" gpu_num=4 \
  replicas=1 backend=sgalng410 other_params="defualt_params"
```


### Dynamic Benchmark Auto Find Batch
```shell
# dynamic
benchmark --model /data/Qwen3-235B-A22B-Instruct-2507-FP8 \
--base-url http://127.0.0.1:30007 \
--result-dir /workspace/result \
--result-dirname "defualt_params" \
--dataset-path /workspace/dataset/ShareGPT_V3_10000.json \
--dataset-name sharegpt \
--num-prompts 200 \
--max-concurrency 20,200 \
--tokenizer-path /workspace/dataset/tokenizer \
--dynamic-input-len 1024 \
--dynamic-output-len 1024 \
--dynamic-prompt-len-scale 0.1 \
--enable-same-prompt \
--enable-auto-batch \
--sparse-step 30 \
--dense-step 10 \
--dynamic-strategy "fast" \
--goodput '{"mean_TTFT":10000, "mean_TPOT":100}' \
--stop-slo '{"mean_TTFT":15000, "mean_TPOT":150}' \
--metadata arch=x86 gpu="NVIDIA H20" gpu_num=4 \
  replicas=1 backend=sgalng410 other_params="defualt_params"
```

### Docker Run Benchmark
```shell
docker run -it --rm \
--network=host \
--ipc=host \
--privileged=true \
-v /home/zjwei/result:/workspace/result \
benchmark:v0.1 \
benchmark --model /data/Qwen3-235B-A22B-Instruct-2507-FP8 \
--base-url http://127.0.0.1:30007 \
--result-dir /workspace/result \
--result-dirname "defualt_params" \
--dataset-path /workspace/dataset/ShareGPT_V3_10000.json \
--dataset-name sharegpt \
--num-prompts 200 \
--max-concurrency 20,200 \
--tokenizer-path /workspace/dataset/tokenizer \
--dynamic-input-len 1024 \
--dynamic-output-len 1024 \
--dynamic-prompt-len-scale 0.1 \
--enable-same-prompt \
--enable-auto-batch \
--sparse-step 30 \
--dense-step 10 \
--dynamic-strategy "fast" \
--goodput '{"mean_TTFT":10000, "mean_TPOT":100}' \
--stop-slo '{"mean_TTFT":15000, "mean_TPOT":150}' \
--metadata arch=x86 gpu="NVIDIA H20" gpu_num=4 \
  replicas=1 backend=sgalng410 other_params="defualt_params"
```
