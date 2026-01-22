from dataclasses import dataclass, field
from typing import List, Tuple, Optional


@dataclass
class Scene:
    """
    1. short in short out; 2. short in long out; 3. long in short out; 4. long in long out
    """
    siso = (64, 128)
    silo = (64, 128)
    liso = (1024, 128)
    lilo = (2048, 2048)


@dataclass
class SQLTable:
    summary_title = [
        # metadata
        "case_id", "arch", "gpu", "model", "gpu_num", "replicas", "question_label",
        # args
        "batch", "request_rate", "num_prompts",
        # dataset
        "mean_input_tokens", "mean_output_tokens", "total_prompt_tokens", "total_completion_tokens",
        # metrics
        ## ttft
        "mean_TTFT", "median_TTFT", "std_TTFT", "TTFT_P90", "TTFT_P95", "TTFT_P99",
        ## throughput
        "mean_output_throughput", "output_throughput", "total_throughput",
        ## request per seconds
        "request_throughput",
        ## prefilling, decoding
        "mean_prefill_throughput",
        "median_prefill_throughput", "std_prefill_throughput", "P90_prefill_throughput", "P95_prefill_throughput",
        "P99_prefill_throughput",
        "mean_decode_throughput",
        "median_decode_throughput", "std_decode_throughput", "P90_decode_throughput", "P95_decode_throughput",
        "P99_decode_throughput",
        ## goodput
        "request_goodput", "goodput_percentage",
        "mean_goodput_ttft", "mean_goodput_tpot", "mean_goodput_e2el", "mean_goodput_throughput",
        ## tpot
        "mean_TPOT", "median_TPOT", "std_TPOT", "TPOT_P90", "TPOT_P95", "TPOT_P99",
        ## itl
        "mean_ITL", "median_ITL", "std_ITL", "ITL_P90", "ITL_P95", "ITL_P99",
        ## e2el
        "mean_E2EL", "median_E2EL", "std_E2EL", "E2EL_P90", "E2EL_P95", "E2EL_P99",
        'completed',
        # other metadata
        ## inference framework of backend
        'backend',
        'other_params',
        'created_at',
        'prefill_throughput_per_gpu',
        'decode_throughput_per_gpu'
    ]

    sql_summary_title = [
        "key INTEGER PRIMARY KEY",
        "case_id TEXT",
        "arch TEXT",
        "gpu TEXT",
        "model TEXT",
        "gpu_num INT",
        "replicas INT",
        "question_label TEXT",
        "batch INT",
        "request_rate FLOAT",
        "num_prompts INT",
        "mean_input_tokens FLOAT",
        "mean_output_tokens FLOAT",
        "total_prompt_tokens INT",
        "total_completion_tokens INT",
        "mean_TTFT FLOAT",
        "median_TTFT FLOAT",
        "std_TTFT FLOAT",
        "TTFT_P90 FLOAT",
        "TTFT_P95 FLOAT",
        "TTFT_P99 FLOAT",
        "mean_output_throughput FLOAT",
        "output_throughput FLOAT",
        "total_token_throughput FLOAT",
        "request_throughput FLOAT",
        "request_goodput FLOAT",
        "mean_prefill_throughput FLOAT",
        "median_prefill_throughput FLOAT",
        "std_prefill_throughput FLOAT",
        "P90_prefill_throughput FLOAT",
        "P95_prefill_throughput FLOAT",
        "P99_prefill_throughput FLOAT",
        "mean_decode_throughput FLOAT",
        "median_decode_throughput FLOAT",
        "std_decode_throughput FLOAT",
        "P90_decode_throughput FLOAT",
        "P95_decode_throughput FLOAT",
        "P99_decode_throughput FLOAT",
        "goodput_percentage FLOAT",
        "mean_goodput_ttft FLOAT",
        "mean_goodput_tpot FLOAT",
        "mean_goodput_e2el FLOAT",
        "mean_goodput_throughput FLOAT",
        "mean_TPOT FLOAT",
        "median_TPOT FLOAT",
        "std_TPOT FLOAT",
        "TPOT_P90 FLOAT",
        "TPOT_P95 FLOAT",
        "TPOT_P99 FLOAT",
        "mean_ITL FLOAT",
        "median_ITL FLOAT",
        "std_ITL FLOAT",
        "ITL_P90 FLOAT",
        "ITL_P95 FLOAT",
        "ITL_P99 FLOAT",
        "mean_E2EL FLOAT",
        "median_E2EL FLOAT",
        "std_E2EL FLOAT",
        "E2EL_P90 FLOAT",
        "E2EL_P95 FLOAT",
        "E2EL_P99 FLOAT",
        "completed INT",
        "backend TEXT",
        "other_params TEXT DEFAULT ''",
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "prefill_throughput_per_gpu FLOAT",
        "decode_throughput_per_gpu FLOAT"
    ]


@dataclass
class BenchmarkMetrics:
    completed: int
    mean_input: int
    mean_output: int
    total_input: int
    total_output: int
    request_throughput: float
    request_goodput: float
    goodput_percentage: float
    output_throughput: float
    mean_output_throughput: float
    total_token_throughput: float
    mean_goodput_ttft: float
    mean_goodput_tpot: float
    mean_goodput_e2el: float
    mean_goodput_throughput: float
    mean_ttft_ms: float
    median_ttft_ms: float
    std_ttft_ms: float
    percentiles_ttft_ms: List[Tuple[float, float]]
    mean_tpot_ms: float
    median_tpot_ms: float
    std_tpot_ms: float
    percentiles_tpot_ms: List[Tuple[float, float]]
    mean_itl_ms: float  # ITL stands for inter-token latency. Also, can be called TBT stand for token-to-token latency.
    median_itl_ms: float
    std_itl_ms: float
    percentiles_itl_ms: List[Tuple[float, float]]
    # E2EL stands for end-to-end latency per request.
    # It is the time taken on the client side from sending
    # a request to receiving a complete response.
    mean_e2el_ms: float
    median_e2el_ms: float
    std_e2el_ms: float
    percentiles_e2el_ms: List[Tuple[float, float]]

    mean_prefilling_throughput: float  # Prefilling throughput is the number of tokens generated per second during the prefilling stage.
    median_prefilling_throughput: float
    std_prefilling_throughput: float
    percentiles_prefilling_throughput: List[Tuple[float, float]]

    mean_decoding_throughput: float  # Decoding throughput is the number of tokens generated per second during the decoding stage.
    median_decoding_throughput: float
    std_decoding_throughput: float
    percentiles_decoding_throughput: List[Tuple[float, float]]


@dataclass
class RequestFuncInput:
    prompt: str
    api_url: str
    prompt_len: int
    output_len: int
    model: str
    best_of: int = 1
    logprobs: Optional[int] = None  # 每个输出token返回的对数概率数量，None为不返回
    multi_modal_content: Optional[dict] = None  # 多模态
    ignore_eos: bool = False
    temperature: float = 0.0
    stream: bool = True


@dataclass
class RequestFuncOutput:
    generated_text: str = ""
    success: bool = False
    latency: float = 0.0  # Total time cost, the unit is seconds
    ttft: float = 0.0  # Time to first token, the unit is seconds
    itl: List[float] = field(
        default_factory=list)  # List of inter-token latencies, the unit is seconds
    prompt_len: int = 0
    completion_len: int = 0
    error: str = ""

