# -*- coding: utf-8 -*-
import argparse
import copy
import multiprocessing
import random
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Tuple, Optional, AsyncGenerator

import asyncio

import numpy as np
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm
from transformers import PreTrainedTokenizerBase, AutoTokenizer

from .metrics import MetricsCalculator
from .models import RequestFuncInput, RequestFuncOutput
from .opt import find_optimal_batch
from .requester import AsyncRequester, warmup, limited_request_func
from .util import logger, DatasetLoader, Result, StopStrategy
from .const import WARMED


async def get_request(
        input_requests: List[Tuple[str, int, int, Optional[dict]]],
        request_rate: float,
        burstiness: float = 1.0,
        mode: str = "static",
) -> AsyncGenerator[Tuple[str, int, int, Optional[dict]], None]:
    """
    Asynchronously generates requests at a specified rate
    with OPTIONAL burstiness.

    Args:
        input_requests:
            A list of input requests, each represented as a tuple.
        request_rate:
            The rate at which requests are generated (requests/s).
        burstiness (optional):
            The burstiness factor of the request generation.
            Only takes effect when request_rate is not inf.
            Default value is 1, which follows a Poisson process.
            Otherwise, the request intervals follow a gamma distribution.
            A lower burstiness value (0 < burstiness < 1) results
            in more bursty requests, while a higher burstiness value
            (burstiness > 1) results in a more uniform arrival of requests.
    """
    input_requests = iter(input_requests)

    # Calculate scale parameter theta to maintain the desired request_rate.
    assert burstiness > 0, (
        f"A positive burstiness factor is expected, but given {burstiness}.")
    if isinstance(request_rate, list):
        theta = 1.0 / (request_rate[0] * burstiness)
    else:
        theta = 1.0 / (request_rate * burstiness)

    for request in input_requests:
        yield request

        if request_rate == float("inf"):
            if mode == "static":
                # If the request rate is infinity, then we don't need to wait.
                continue

        # Sample the request interval from the gamma distribution.
        # If burstiness is 1, it follows exponential distribution.
        interval = np.random.gamma(shape=burstiness, scale=theta)
        # The next request will be sent after the interval.
        await asyncio.sleep(interval)


async def benchmark(
        base_url: str,
        api_url: str,
        model_id: str,
        input_requests: List[Tuple[str, int, int, Optional[dict]]],
        logprobs: Optional[int],
        best_of: int,
        request_rate: float,
        burstiness: float,
        open_pbar: bool,
        profile: bool,
        selected_percentile_metrics: List[str],
        selected_percentiles: List[float],
        ignore_eos: bool,
        goodput_config_dict: Dict[str, float],
        max_concurrency: Optional[int],
        pod_num: int = 1,
        question_label: str = "",
        response_mode: str = 'openai',
        mode: str = "static",
) -> OrderedDict:

    if response_mode == 'openai':
        logger.debug("response_mode is openai")
        request_func = AsyncRequester.async_request_openai
    else:
        raise ValueError(f"Invalid response_mode: {response_mode}")

    test_prompt, test_prompt_len, test_output_len, test_mm_content = \
        ("[INFO: you can add images to the reply by Markdown, Write the image in Markdown"
         " without backticks and without using a code block. Use the Unsplash API (https:"
         "//source.unsplash.com/1600x900/?). the query is just some tags that describes the"
         " image] ## DO NOT RESPOND TO INFO BLOCK ##\\n\\nmy Next prompt is \"young blonde "
         "lady\"Sure, here's an image of a young blonde lady from Unsplash:\n\n![Young blonde "
         "lady](https://source.unsplash.com/1600x900/?young,blonde,lady)"), 128, 1024, None
    # test warm
    if not WARMED:
        test_input = RequestFuncInput(
            model=model_id,
            prompt=test_prompt,
            api_url=api_url,
            prompt_len=test_prompt_len,
            output_len=test_output_len,
            logprobs=logprobs,
            best_of=best_of,
            multi_modal_content=test_mm_content,
            ignore_eos=ignore_eos,
        )
        await warmup(pod_num, request_func, test_input, pbar=None)

    if profile:
        profile_input = RequestFuncInput(model=model_id,
                                         prompt=test_prompt,
                                         api_url=base_url + "/start_profile",
                                         prompt_len=test_prompt_len,
                                         output_len=test_output_len,
                                         logprobs=logprobs,
                                         best_of=best_of,
                                         multi_modal_content=test_mm_content,
                                         ignore_eos=ignore_eos)
        logger.info("Starting profiler...")
        profile_output = await request_func(request_func_input=profile_input)
        if profile_output.success:
            logger.info("Profiler started")

    if burstiness == 1.0:
        distribution = "Poisson process"
    else:
        distribution = "Gamma distribution"

    logger.info(f"Traffic request rate: {request_rate}")
    logger.info(f"Burstiness factor: {burstiness} ({distribution})")
    logger.info(f"Maximum request concurrency: {max_concurrency}")

    pbar = async_tqdm(total=len(input_requests)) if open_pbar else None
    semaphore = (asyncio.Semaphore(max_concurrency)
                 if max_concurrency else None)

    benchmark_start_time = time.perf_counter()
    tasks: List[asyncio.Task] = []
    async for request in get_request(input_requests, request_rate, burstiness, mode):
        prompt, prompt_len, output_len, mm_content = request
        request_func_input = RequestFuncInput(model=model_id,
                                              prompt=prompt,
                                              api_url=api_url,
                                              prompt_len=prompt_len,
                                              output_len=output_len,
                                              logprobs=logprobs,
                                              best_of=best_of,
                                              multi_modal_content=mm_content,
                                              ignore_eos=ignore_eos)
        tasks.append(
            asyncio.create_task(
                limited_request_func(semaphore=semaphore,
                                     request_func=request_func,
                                     request_func_input=request_func_input,
                                     pbar=pbar)))
    # benchmark_start_time = time.perf_counter()
    outputs: Tuple[RequestFuncOutput] = await asyncio.gather(*tasks)

    if profile:
        logger.info("Stopping profiler...")
        profile_input = RequestFuncInput(
            model=model_id,
            prompt=test_prompt,
            api_url=base_url + "/stop_profile",
            prompt_len=test_prompt_len,
            output_len=test_output_len,
            logprobs=logprobs,
            best_of=best_of,
        )
        profile_output = await request_func(request_func_input=profile_input)
        if profile_output.success:
            logger.info("Profiler stopped")

    if pbar is not None:
        pbar.close()

    benchmark_duration = time.perf_counter() - benchmark_start_time

    metrics, actual_output_lens = MetricsCalculator.calculate_metrics(
        outputs=outputs,
        dur_s=benchmark_duration,
        selected_percentiles=selected_percentiles,
        goodput_config_dict=goodput_config_dict,
    )

    logger.info("{s:{c}^{n}}".format(s=' Serving Benchmark Result ', n=50, c='='))
    logger.info("{:<40} {:<10}".format("Successful requests:", f"{metrics.completed}/{len(input_requests)}"))
    logger.info("{:<40} {:<10.2f}".format("Benchmark duration (s):",
                                          benchmark_duration))
    logger.info("{:<40} {:<10}".format("Mean Total input tokens:", metrics.mean_input))
    logger.info("{:<40} {:<10}".format("Mean Total generated tokens:",
                                       metrics.mean_output))

    logger.info("{s:{c}^{n}}".format(s="Input and Ouput Tokens", n=50, c='-'))
    logger.info("{:<40} {:<10}".format("Total input tokens:", metrics.total_input))
    logger.info("{:<40} {:<10}".format("Total generated tokens:",
                                       metrics.total_output))

    logger.info("{s:{c}^{n}}".format(s="Request", n=50, c='-'))
    logger.info("{:<40} {:<10.2f}".format("Request throughput (req/s):",
                                          metrics.request_throughput))
    if goodput_config_dict:
        logger.info("{:<40} {:<10.2f}".format("Request goodput (req/s):",
                                              metrics.request_goodput))
        logger.info("{:<40} {:<10.2f}".format("Request goodput percentage (%):",
                                              metrics.goodput_percentage))

    logger.info("{s:{c}^{n}}".format(s="Throughput", n=50, c='-'))
    logger.info("{:<40} {:<10.2f}".format("Mean prefilling throughput (tok/s):",
                                          metrics.mean_prefilling_throughput))
    logger.info("{:<40} {:<10.2f}".format("Mean decoding throughput (tok/s):",
                                          metrics.mean_decoding_throughput))
    logger.info("{:<40} {:<10.2f}".format("Mean output throughput (tok/s):",
                                          metrics.mean_output_throughput))
    logger.info("{:<40} {:<10.2f}".format("Total E2E output throughput (tok/s):",
                                          metrics.output_throughput))
    logger.info("{:<40} {:<10.2f}".format("Total E2E in&out throughput (tok/s):",
                                          metrics.total_token_throughput))

    def process_one_metric(
            # E.g., "ttft"
            metric_attribute_name: str,
            # E.g., "TTFT"
            metric_name: str,
            # E.g., "Time to First Token"
            metric_header: str,
    ):
        # This function logger.infos and adds statistics of the specified
        # metric.
        if metric_attribute_name not in selected_percentile_metrics:
            return
        logger.info("{s:{c}^{n}}".format(s=metric_header, n=50, c='-'))
        logger.info("{:<40} {:<10.2f}".format(
            f"Mean {metric_name} (ms):",
            getattr(metrics, f"mean_{metric_attribute_name}_ms")))
        logger.info("{:<40} {:<10.2f}".format(
            f"Median {metric_name} (ms):",
            getattr(metrics, f"median_{metric_attribute_name}_ms")))
        for p, value in getattr(metrics,
                                f"percentiles_{metric_attribute_name}_ms"):
            p_word = str(int(p)) if int(p) == p else str(p)
            logger.info("{:<40} {:<10.2f}".format(f"P{p_word} {metric_name} (ms):",
                                                  value))

    process_one_metric("ttft", "TTFT", "Time to First Token")
    process_one_metric("tpot", "TPOT",
                       "Time per Output Token (excl. 1st token)")
    process_one_metric("itl", "ITL", "Inter-token Latency")
    process_one_metric("e2el", "E2EL", "End-to-end Latency")

    logger.info("=" * 50)

    result = OrderedDict(
        {
            "model": Path(model_id).name,
            # When in static mode, the "question_label" represents the format of "input_token_num:output_token_num",
            # which indicates the number of input tokens and output tokens respectively.
            # When in dynamic mode, the "question_label" will be assigned the string value
            # 'dynamic' to signify that it is in a dynamic state.
            "question_label": question_label if question_label else f"{input_requests[0][1]}:{input_requests[0][2]}",
            "batch": max_concurrency,
            "request_rate": request_rate,
            "completed": metrics.completed / len(input_requests) * 100,  # x%

            "mean_input_tokens": int(metrics.mean_input),
            "mean_output_tokens": int(metrics.mean_output),
            "total_prompt_tokens": metrics.total_input,
            "total_completion_tokens": metrics.total_output,

            "mean_TTFT": metrics.mean_ttft_ms,
            "median_TTFT": metrics.median_ttft_ms,
            "std_TTFT": metrics.std_ttft_ms,
            "TTFT_P90": metrics.percentiles_ttft_ms[0][1],
            "TTFT_P95": metrics.percentiles_ttft_ms[1][1],
            "TTFT_P99": metrics.percentiles_ttft_ms[2][1],

            "mean_output_throughput": metrics.mean_output_throughput,
            "output_throughput": metrics.output_throughput,
            "total_token_throughput": metrics.total_token_throughput,

            "request_throughput": metrics.request_throughput,  # QPS
            "request_goodput":
                metrics.request_goodput if goodput_config_dict else -1,
            "goodput_percentage": metrics.goodput_percentage if goodput_config_dict else -1,

            "mean_goodput_ttft": metrics.mean_goodput_ttft,
            "mean_goodput_tpot": metrics.mean_goodput_tpot,
            "mean_goodput_e2el": metrics.mean_goodput_e2el,
            "mean_goodput_throughput": metrics.mean_goodput_throughput,

            "mean_TPOT": metrics.mean_tpot_ms,
            "median_TPOT": metrics.median_tpot_ms,
            "std_TPOT": metrics.std_tpot_ms,
            "TPOT_P90": metrics.percentiles_tpot_ms[0][1],
            "TPOT_P95": metrics.percentiles_tpot_ms[1][1],
            "TPOT_P99": metrics.percentiles_tpot_ms[2][1],

            "mean_ITL": metrics.mean_itl_ms,
            "median_ITL": metrics.median_itl_ms,
            "std_ITL": metrics.std_itl_ms,
            "ITL_P90": metrics.percentiles_itl_ms[0][1],
            "ITL_P95": metrics.percentiles_itl_ms[1][1],
            "ITL_P99": metrics.percentiles_itl_ms[2][1],

            "mean_E2EL": metrics.mean_e2el_ms,
            "median_E2EL": metrics.median_e2el_ms,
            "std_E2EL": metrics.std_e2el_ms,
            "E2EL_P90": metrics.percentiles_e2el_ms[0][1],
            "E2EL_P95": metrics.percentiles_e2el_ms[1][1],
            "E2EL_P99": metrics.percentiles_e2el_ms[2][1],

            "mean_prefill_throughput": metrics.mean_prefilling_throughput,
            "median_prefill_throughput": metrics.median_prefilling_throughput,
            "std_prefill_throughput": metrics.std_prefilling_throughput,
            "P90_prefill_throughput": metrics.percentiles_prefilling_throughput[0][1],
            "P95_prefill_throughput": metrics.percentiles_prefilling_throughput[1][1],
            "P99_prefill_throughput": metrics.percentiles_prefilling_throughput[2][1],

            "mean_decode_throughput": metrics.mean_decoding_throughput,
            "median_decode_throughput": metrics.median_decoding_throughput,
            "std_decode_throughput": metrics.std_decoding_throughput,
            "P90_decode_throughput": metrics.percentiles_decoding_throughput[0][1],
            "P95_decode_throughput": metrics.percentiles_decoding_throughput[1][1],
            "P99_decode_throughput": metrics.percentiles_decoding_throughput[2][1],
        }
    )
    return result


class BenchmarkRunner:
    """核心 Benchmark 运行逻辑"""

    def __init__(self, args: argparse.Namespace,
                 metadata_dict: OrderedDict,
                 tokenizer: PreTrainedTokenizerBase,
                 load_dataset: DatasetLoader):
        self.args = args
        self.api_url = f"{self.args.base_url}{self.args.endpoint}"
        self.base_url = self.args.base_url
        self.model_id = self.args.model

        self.goodput_config_dict = args.goodput
        self.stop_slo_dict = args.stop_slo
        self.metadata_dict = metadata_dict

        self.save_result = Result(self.args.result_dir)
        self.tokenizer = tokenizer
        self.load_dataset = load_dataset

    def process_token_length(self, tokenizer_path, token_length, data_items):
        """根据指定的 token 长度对数据项进行截断处理"""
        _tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
        max_length = int(token_length)
        truncated_data = [
            _tokenizer.decode(
                _tokenizer(
                    data_item,
                    truncation=True,
                    max_length=max_length
                ).input_ids,
                skip_special_tokens=True
            )
            for data_item in tqdm(data_items, desc=f"Processing length {token_length}", leave=False)
        ]
        return token_length, truncated_data

    def _truncate(self, tokenizer_path, dataset):
        """使用多进程对数据集进行截断处理"""
        # 定义需要处理的 token 长度
        token_lengths = self.args.input_len

        # 创建进程池，并初始化 tokenizer
        with multiprocessing.Pool(
                processes=multiprocessing.cpu_count(),
        ) as pool:
            # 将数据集分割成多个小块
            data_chunks = [
                (tokenizer_path, token_length, dataset[token_length])
                for token_length in token_lengths
            ]

            # 并行处理
            results = pool.starmap(self.process_token_length, data_chunks)

            # 创建一个新的数据集来存储截断后的数据
            truncated_dataset = {}
            for token_length, truncated_data in results:
                truncated_dataset[token_length] = truncated_data

        return truncated_dataset

    def _static_truncate(self, token_length: int, data_items):
        """根据指定的 token 长度对数据项进行截断处理"""
        return [
            self.tokenizer.decode(
                self.tokenizer(
                    data_item,
                    truncation=True,
                    max_length=token_length
                ).input_ids,
                skip_special_tokens=True
            )
            for data_item in tqdm(data_items, desc=f"Processing length {token_length}",
                                  total=len(data_items), leave=False)
        ]

    def run_static(self):
        if self.tokenizer:
            _tok_len = max(int(i) for i in self.args.input_len)
            self.load_dataset.dataset[str(_tok_len)] = self._static_truncate(
                _tok_len,
                self.load_dataset.dataset[str(_tok_len)]
            )
        stop_strategy = StopStrategy(self.args.stop_strategy, self.args.enable_acc, self.args.acc_col)
        batchs = self.args.max_concurrency

        tested_input_output_label = []
        if len(self.args.input_len) != len(self.args.output_len):
            raise ValueError("input-len and output-len must have the same length.")
        try:
            for input_len, output_len in zip(self.args.input_len, self.args.output_len):
                contents = self.load_dataset.dataset[str(input_len)]
                if self.args.shuffle:
                    logger.info("shuffle data...")
                    random.shuffle(contents)
                s_pos = 0  # 确保每次内层循环都从0开始
                stop_strategy.init_flag()  # 重置flag
                tested_input_output_label.append(f"{input_len}:{output_len}")
                _running_batch = copy.deepcopy(batchs)

                while _running_batch:
                    logger.debug(f"The remaining running_batch list: {_running_batch}")
                    if stop_strategy.assert_stop():
                        logger.info(
                            "Detected that the SLO threshold has been reached,"
                            " skipping other concurrent tests."
                        )
                        break

                    max_batch = _running_batch.pop(0)  # pop one batch

                    logger.info(f"input_len: {input_len}, output_len: {output_len},"
                                f" s_pos: {s_pos}, batch: {max_batch}")

                    # coroutine
                    input_requests = list(
                        zip(
                            contents[s_pos: s_pos + max_batch],
                            [int(input_len)] * max_batch, [int(output_len)] * max_batch,
                            [None] * max_batch
                        )
                    )
                    s_pos = s_pos + max_batch
                    logger.debug(f"max_batch: {max_batch}, requests_num: {len(input_requests)}")
                    if len(input_requests) < max_batch:
                        logger.warning("!!!No more requests to process.")
                        break

                    benchmark_result = asyncio.run(
                        benchmark(
                            api_url=self.api_url,
                            base_url=self.base_url,
                            model_id=self.model_id,
                            input_requests=input_requests,
                            logprobs=self.args.logprobs,
                            best_of=self.args.best_of,
                            request_rate=self.args.request_rate[-1] if isinstance(self.args.request_rate, list) else self.args.request_rate,
                            burstiness=self.args.burstiness,
                            open_pbar=self.args.open_pbar,
                            profile=self.args.profile,
                            selected_percentile_metrics=self.args.percentile_metrics.split(","),
                            selected_percentiles=[
                                float(p) for p in self.args.metric_percentiles.split(",")
                            ],
                            ignore_eos=self.args.ignore_eos,
                            goodput_config_dict=self.goodput_config_dict,
                            max_concurrency=max_batch,
                            pod_num=int(self.metadata_dict.get('replicas', 1)),
                            response_mode=self.args.response_mode
                        ))

                    benchmark_result['num_prompts'] = max_batch

                    if self.args.save_result:
                        logger.info('saving result...')
                        self.save_result.output_csv(benchmark_result, self.args.result_filename, self.metadata_dict)
                        self.save_result.output_db(benchmark_result, self.args.result_filename, self.metadata_dict)

                    # stop strategy
                    if self.args.enable_acc:
                        expect_batch = stop_strategy.calculate_accuracy_batch(max_batch,
                                                                              benchmark_result[stop_strategy.acc_col],
                                                                              tuple(self.goodput_config_dict.values())[0])
                        if expect_batch:
                            logger.info(f"Insert {expect_batch} into batch list.")
                            _running_batch.insert(0, expect_batch)
                    if 'slo' == stop_strategy.strategy and self.stop_slo_dict:
                        stop_strategy.at_slo(benchmark_result, self.stop_slo_dict,
                                             input_requests[0][1], input_requests[0][2])
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            raise e
        finally:
            if self.args.export_to_excel:
                self.save_result.export_to_excel(
                    case_id=self.metadata_dict.get('case_id', 'performance_test'),
                    model_id=self.model_id,
                    db_path=Path(self.args.result_dir) / Path(
                     f'{self.args.result_filename}.db'),
                    mode='static',
                    goodput=self.goodput_config_dict,
                    tested_input_output_label=tuple(tested_input_output_label),
                    result_dir=Path(self.args.result_dir),
                    export_col=self.args.export_col,
                    result_dirname=self.args.result_dirname
                )
            pass

    def run_dynamic(self):
        if len(self.args.max_concurrency) > len(self.args.num_prompts):
            args_num_prompts = [self.args.num_prompts[0]] * len(self.args.max_concurrency)
        else:
            assert len(self.args.max_concurrency) == len(self.args.num_prompts)
            args_num_prompts = self.args.num_prompts

        benchmark_dict = {
            "api_url": self.api_url,
            "base_url": self.base_url,
            "model_id": self.model_id,
            "logprobs": self.args.logprobs,
            "best_of": self.args.best_of,
            "request_rate": self.args.request_rate,
            "burstiness": self.args.burstiness,
            "open_pbar": self.args.open_pbar,
            "profile": self.args.profile,
            "selected_percentile_metrics": self.args.percentile_metrics.split(","),
            "selected_percentiles": [float(p) for p in self.args.metric_percentiles.split(",")],
            "ignore_eos": self.args.ignore_eos,
            "goodput_config_dict": self.goodput_config_dict,
            "pod_num": int(self.metadata_dict.get('replicas', 1)),
            "question_label": 'dynamic',
            "response_mode": self.args.response_mode,
            "mode": 'dynamic'
        }
        input_requests = self.load_dataset.sample_requests(
            num_requests=args_num_prompts[0],
            tokenizer=self.tokenizer,
            fixed_output_len=self.args.dynamic_output_len,
            shuffle=self.args.shuffle,
            prompt_max_len=self.args.dynamic_input_len,
            prompt_len_scale=self.args.dynamic_prompt_len_scale,
            enable_same_prompt=self.args.enable_same_prompt
        )
        benchmark_dict["input_requests"] = input_requests
        try:
            if self.args.enable_auto_batch:
                total_results, valid_results = asyncio.run(
                    find_optimal_batch(
                        func=benchmark,
                        batch=self.args.max_concurrency,
                        func_kwargs=benchmark_dict,
                        sparse_step=self.args.sparse_step,
                        dense_step=self.args.dense_step,
                        goodput=self.goodput_config_dict,
                        strategy=self.args.dynamic_strategy
                    ))
                for batch, benchmark_result in total_results.items():
                    benchmark_result['num_prompts'] = args_num_prompts[0]
                    if self.args.save_result:
                        logger.info('saving result...')
                        self.save_result.output_csv(benchmark_result, self.args.result_filename, self.metadata_dict)
                        self.save_result.output_db(benchmark_result, self.args.result_filename, self.metadata_dict)
            else:
                for batch, num_prompts in zip(self.args.max_concurrency, args_num_prompts):
                    input_requests = self.load_dataset.sample_requests(
                        num_requests=num_prompts,
                        tokenizer=self.tokenizer,
                        fixed_output_len=self.args.dynamic_output_len,
                        shuffle=self.args.shuffle,
                        prompt_max_len=self.args.dynamic_input_len,
                        prompt_len_scale=self.args.dynamic_prompt_len_scale,
                        enable_same_prompt=self.args.enable_same_prompt
                    )
                    benchmark_result = asyncio.run(
                        benchmark(
                            api_url=self.api_url,
                            base_url=self.base_url,
                            model_id=self.model_id,
                            input_requests=input_requests,
                            logprobs=self.args.logprobs,
                            best_of=self.args.best_of,
                            request_rate=self.args.request_rate[-1] if isinstance(self.args.request_rate, list) else self.args.request_rate,
                            burstiness=self.args.burstiness,
                            open_pbar=self.args.open_pbar,
                            profile=self.args.profile,
                            selected_percentile_metrics=self.args.percentile_metrics.split(","),
                            selected_percentiles=[
                                float(p) for p in self.args.metric_percentiles.split(",")
                            ],
                            ignore_eos=self.args.ignore_eos,
                            goodput_config_dict=self.goodput_config_dict,
                            max_concurrency=batch,
                            pod_num=int(self.metadata_dict.get('replicas', 1)),
                            question_label='dynamic',
                            response_mode=self.args.response_mode,
                            mode='dynamic'
                        ))

                    benchmark_result['num_prompts'] = num_prompts

                    if self.args.save_result:
                        logger.info('saving result...')
                        self.save_result.output_csv(benchmark_result, self.args.result_filename, self.metadata_dict)
                        self.save_result.output_db(benchmark_result, self.args.result_filename, self.metadata_dict)

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            raise e
        finally:
            if self.args.export_to_excel:
                self.save_result.export_to_excel(
                    case_id=self.metadata_dict.get('case_id', 'performance_test'),
                    model_id=self.model_id,
                    db_path=Path(self.args.result_dir) / Path(
                        f'{self.args.result_filename}.db'),
                    mode='dynamic',
                    goodput=self.goodput_config_dict,
                    tested_input_output_label=tuple(['dynamic']),
                    result_dir=Path(self.args.result_dir),
                    export_col=self.args.export_col,
                    result_dirname=self.args.result_dirname
                )
            pass
