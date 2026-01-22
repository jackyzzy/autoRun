# -*- coding: utf-8 -*-
import argparse
import json


def parse_input_list(value):
    # 将逗号分隔的字符串分割为列表，并转换为整数
    return [int(num) for num in value.split(',')]


def parse_input_dict(value):
    try:
        # 将 JSON 字符串转换为字典
        return json.loads(value)
    except json.JSONDecodeError:
        # 解析失败时抛出友好的错误提示
        raise argparse.ArgumentTypeError(f"Invalid JSON format for --goodput: {value}")


def parse_request_rate(value):
    """解析请求速率参数，支持单个浮点数或逗号分隔的浮点数列表"""
    if ',' in value:
        # 处理逗号分隔的多个值
        parts = value.split(',')
        try:
            return [float(part.strip()) for part in parts]
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid float values: {value}")
    else:
        # 处理单个浮点数
        try:
            return float(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"Invalid float value: {value}")


def parse_args():
    __version__ = '0.0.7'
    """命令行参数解析"""
    parser = argparse.ArgumentParser('inference performance test tool.')
    parser.add_argument("--version", action='version', version=__version__)
    # request api options
    parser.add_argument("--base-url", type=str, required=True,
                        help="Server or API base url if not using http host and port."
                             " Such as http://127.0.0.1:8000",
                        )
    parser.add_argument('--endpoint', type=str,
                        help='Endpoint of the server or API. Such as /v1/chat/completion',
                        default='/v1/chat/completions'
                        )
    parser.add_argument('--api-key', type=str,
                        help='API-KEY to request LLM service.',
                        )
    parser.add_argument('--model', type=str, required=True,
                        help='Model name in request body, such as Yi1.5-34B-chat-FP16. '
                             'It means custom model name in AICP.',
                        )
    parser.add_argument('--response-mode', type=str, choices=['openai', 'kts', 'hw'],
                        help="Response mode for the server or API. Such as openai.",
                        default='openai')
    parser.add_argument("--tokenizer-path", type=str,
                        help="Tokenizer path for the model. Such as /path/to/tokenizer",
                        default=None)
    parser.add_argument('--max-concurrency', type=parse_input_list,
                        help='A list, Maximum number of concurrent requests. default is'
                             '1, 8, 16, 32, 64, 128, 200, 300, 400, 500, 600',
                        default=[1, 8, 16, 32, 64, 128, 200, 300, 400, 500, 600]
                        )
    # dataset options
    parser.add_argument('--dataset-path', type=str,
                        help='dataset path, such as /xx/xxx/filtered.json. '
                             'default is filtered.json',
                        default="/workspace/dataset/filtered.json"
                        )
    parser.add_argument('--dataset-name', type=str,
                        choices=['filtered', 'sharegpt', 'normal', 'custom'],
                        help="dataset name, "
                             "'filtered' indicates the static mode. In this mode, "
                             "you are able to add specific parameters within"
                             " the options for the *static* test mode."
                             "'sharegpt' represents the dynamic mode. For this mode, "
                             "you can incorporate specific parameters "
                             "in the options designated for the *dynamic* mode."
                             "default is filtered.",
                        default='filtered'
                        )
    parser.add_argument('--dataset-field', type=parse_input_dict,
                        help='Custom dataset field.'
                             'Used for customizing dataset reading'
                             '{"input_field": ["Question", "Complex_CoT"], "output_field": ["Response"]}',
                        default=None
                        )
    # test options
    parser.add_argument('--request-rate', type=parse_request_rate, default=float("inf"),
                        help='Request rate in requests per second.')
    parser.add_argument('--burstiness', type=float, default=1.0,
                        help='Burstiness factor for the Poisson process.')
    parser.add_argument('--seed', type=int, help='Random seed.',
                        default=2024)
    parser.add_argument('--logprobs', type=int, default=None,
                        help=("Number of logprobs-per-token to compute & return as part of "
                              "the request. If unspecified, then either (1) if beam search "
                              "is disabled, no logprobs are computed & a single dummy "
                              "logprob is returned for each token; or (2) if beam search "
                              "is enabled 1 logprob per token is computed"))
    parser.add_argument('--best-of', type=int,
                        help='Number of best of samples to request.',
                        default=1)
    parser.add_argument('--disable-ignore-eos', action='store_true',
                        help='Whether to ignore EOS token.',
                        )
    parser.add_argument('--profile', action='store_true',
                        help='Whether to profile the server.')
    parser.add_argument("--disable-warm", action="store_true",
                        help="do not warm up the server.")
    parser.add_argument("--shuffle", action='store_true',
                        help="Set to shuffle the dataset.")
    # statistic options
    parser.add_argument('--percentile-metrics', type=str,
                        help='Metrics to calculate percentiles for. '
                             'default is \'ttft,tpot,itl,e2el,throughput\'',
                        default='ttft,tpot,itl,e2el,throughput'
                        )
    parser.add_argument('--metric-percentiles', type=str,
                        help='Percentiles to calculate for the specified metrics. default is \'90,95,99\'',
                        default='90,95,99'
                        )
    parser.add_argument('--goodput', type=parse_input_dict,
                        help="Specify service level objectives for goodput as \"KEY:VALUE\" "
                             "pairs, where the key is a metric name, and the value is in "
                             "milliseconds. Multiple \"KEY:VALUE\" pairs can be provided, "
                             "separated by spaces. Allowed request level metric names are "
                             "\"ttft\", \"tpot\", \"e2el\", \"throughput\". "
                             "For more context on the definition of "
                             "goodput, refer to DistServe paper: https://arxiv.org/pdf/2401.09670 "
                             "and the blog: https://hao-ai-lab.github.io/blogs/distserve",
                        default={"TTFT_P90": 10000, "TPOT_P90": 100}
                        )
    # static mode test params
    parser.add_argument('--input-len', type=parse_input_list,
                        help='Input tokens length. The positions of the input token count and'
                             ' output token count correspond to each other, '
                             'such as 128,2048,4096,7168',
                        default=[128, 2048, 4096, 7168],  # 新场景
                        )
    parser.add_argument('--output-len', type=parse_input_list,
                        help='Output tokens length. such as 1024,2048,1024,1024',
                        default=[1024, 2048, 1024, 1024],
                        )
    parser.add_argument('--stop-slo', type=parse_input_dict,
                        help='Whether to stop the benchmark '
                             'if the SLO has been achieved.'
                             'default is {"TTFT_P90": 15000, "TPOT_P90": 150}',
                        default={"TTFT_P90": 15000, "TPOT_P90": 150}
                        )
    parser.add_argument("--disable-acc", action="store_true",
                        help="Whether to enable accuracy test. "
                             "Precisely test to the concurrency near SLO.",
                        )
    parser.add_argument("--stop-strategy", type=str, choices=["slo", "acc", "all"],
                        default="slo",
                        help="Stop strategy. default is 'slo'. "
                             "slo: Traverse max currency until reaching slo. And --stop-slo must be set. "
                             "acc: Test out the maximum concurrent requests under SLO and then stop. And --enable-acc must be set. "
                             "all: Traverse all --max-concurrency."
                             "method to find the accurate concurrency that achieves slo")
    parser.add_argument("--acc-col", type=str,
                        default="TTFT_P99",
                        help="Reference column names for precise testing. Default is 'mean_TTFT'.")
    # dynamic test mode options
    parser.add_argument("--dynamic-output-len",
                        type=int,
                        default=None,
                        help="Output length for each request. Overrides the output length "
                             "from the ShareGPT dataset.")
    parser.add_argument('--dynamic-input-len', type=int, default=None,
                        help="Input length for each request. Overrides the input length "
                             "from the ShareGPT dataset.")
    parser.add_argument('--dynamic-prompt-len-scale', type=float, default=0,
                        help="Prompt length scale for each request.")
    parser.add_argument('--enable-same-prompt', action='store_true',
                        help="Enable same prompt.")
    parser.add_argument('--num-prompts', type=parse_input_list,
                        help='Number of prompts to generate. '
                             '--num-prompts can input multiple values separated by spaces, '
                             'with a length consistent with max currency, '
                             'and each element corresponds one-to-one.',
                        default=[1000]
                        )
    parser.add_argument('--disable-pbar', action='store_true',
                        help='Whether to open progress bar.',
                        )
    parser.add_argument('--enable-auto-batch', action='store_true',
                        help='Whether to enable auto find best batch.',
                        )
    parser.add_argument('--sparse-step', type=int,
                        default=30,
                        help='Sparsity sampling step size parameter, takes effect when auto-batch mode is enabled.',
                        )
    parser.add_argument('--dense-step', type=int,
                        default=10,
                        help='Dense sampling step size parameter, takes effect when auto-batch mode is enabled.',
                        )
    parser.add_argument('--dynamic-strategy', type=str,
                        choices=["fast", "exhaustive"], default="fast",
                        help='Dynamic test mode strategy. default is fast.',
                        )
    # result save options
    # parser = parser.add_argument_group("save options")
    parser.add_argument("--metadata",
                        metavar="KEY=VALUE",
                        nargs="*",
                        help="Key-value pairs (e.g, --metadata  case_id=perf_test "
                             "arch=x86 gpu=\"NVIDIA 4090\" gpu_num=8 replicas=1 "
                             "backend=sllm other_params={}) "
                             "for metadata of this run to be saved in the result JSON file "
                             "for record keeping purposes.",
                        )
    parser.add_argument("--case-id", type=str,
                        default=None, help="Case ID for the benchmark run.")
    parser.add_argument("--disable-save-result",
                        action="store_true",
                        help="Specify to save benchmark results to db.",
                        )
    parser.add_argument("--result-dir",
                        type=str,
                        help="Specify directory to save benchmark json results."
                             "If not specified, results are saved in the current directory.",
                        default='/workspace/result')
    parser.add_argument("--result-filename",
                        type=str,
                        help="Specify the filename to save benchmark json results.",
                        default='model_performance')
    parser.add_argument("--result-dirname",
                        type=str,
                        help="Specify the directory name to save benchmark excel results.",
                        default='defualt_params')
    parser.add_argument("--disable-export-to-excel", action="store_true",
                        help="Specify to export benchmark results to excel.")
    parser.add_argument("--export-col", nargs="*",
                        help="Specify the columns to export to excel.",
                        default=[])

    # logging options
    parser.add_argument("--log-level", type=str,
                        default="info", help="Log level.")

    run_args = parser.parse_args()
    run_args.enable_acc = not run_args.disable_acc
    run_args.open_pbar = not run_args.disable_pbar
    run_args.save_result = not run_args.disable_save_result
    run_args.export_to_excel = not run_args.disable_export_to_excel
    run_args.ignore_eos = not run_args.disable_ignore_eos

    return run_args


if __name__ == "__main__":
    args = parse_args()
    print(type(args.max_concurrency), args.max_concurrency)
    print(type(args.goodput), args.goodput)
    print(type(args.request_rate), args.request_rate)
