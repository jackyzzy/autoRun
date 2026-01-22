# -*- coding: utf-8 -*-
import asyncio
import json
import os
import sys
import traceback

import aiohttp
import time
from typing import Dict, Any, List
from tqdm.asyncio import tqdm as async_tqdm

from .const import AIOHTTP_TIMEOUT
from .models import RequestFuncInput, RequestFuncOutput
from .util import logger


def remove_prefix(s, prefix):
    # python3.9 才有removeprefix方法
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


async def limited_request_func(semaphore, request_func, request_func_input, pbar):
    if semaphore is None:
        return await request_func(request_func_input=request_func_input,
                                  pbar=pbar)
    async with semaphore:
        return await request_func(request_func_input=request_func_input,
                                  pbar=pbar)


async def warmup(pod_num, request_func, request_func_input, pbar):
    global WARMED
    pod_num = pod_num * 4 if pod_num > 1 else pod_num
    # semaphore = asyncio.Semaphore(pod_num)
    tasks: List[asyncio.Task] = []
    for _ in range(pod_num):
        tasks.append(
            asyncio.create_task(
                limited_request_func(semaphore=None,
                                     request_func=request_func,
                                     request_func_input=request_func_input,
                                     pbar=pbar)))
    logger.info(f"Starting initial {pod_num} prompt to warm-up, running...")
    test_outputs = await asyncio.gather(*tasks)
    logger.debug(f"test_outputs: {test_outputs}")
    for test_output in test_outputs:
        if not test_output.success:
            raise ValueError(
                "Initial test run failed - Please make sure benchmark arguments "
                f"are correctly specified.")  # Error: {test_output.error}
        else:
            WARMED = True
            logger.info("Initial test run completed. Starting main benchmark run...")


class AsyncRequester:
    """封装 aiohttp 的异步请求工具"""

    @staticmethod
    async def async_request_openai(
            request_func_input: RequestFuncInput,
            pbar: async_tqdm = None,
    ):
        api_url = request_func_input.api_url
        # 异步请求
        connector = aiohttp.TCPConnector(ssl=False) if 'https' in api_url else None
        async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT, connector=connector) as session:
            payload = {
                "model": request_func_input.model,
                "temperature": request_func_input.temperature,
                "best_of": request_func_input.best_of,
                "max_tokens": request_func_input.output_len,
                # "logprobs": request_func_input.logprobs,
                "stream": request_func_input.stream,
                "ignore_eos": request_func_input.ignore_eos,
                "stream_options": {"include_usage": True},
                "messages": [
                    {
                        "role": "user",
                        "content": request_func_input.prompt
                    }
                ]
            }
            headers = {
                "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
                "Content-Type": "application/json"
            }

            output = RequestFuncOutput()
            output.prompt_len = request_func_input.prompt_len

            generated_text = ""
            st = time.perf_counter()
            most_recent_timestamp = st
            try:
                async with session.post(url=api_url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        async for chunk_bytes in response.content:
                            if not chunk_bytes or chunk_bytes in {b'\r', b'\n'} or b': ping - ' in chunk_bytes:
                                continue
                            if b"local_rate_limited" in chunk_bytes:
                                logger.info(f"{chunk_bytes}, local rate limited, break")
                                break

                            chunk = remove_prefix(chunk_bytes.decode('utf-8'), "data:").strip()
                            logger.debug(f"{chunk}")
                            if chunk == "[DONE]":
                                output.latency = time.perf_counter() - st
                                output.success = True
                            else:
                                timestamp = time.perf_counter()

                                data = json.loads(chunk)
                                # response error
                                if data.get("status", {}).get("code") == 500:
                                    logger.error(f"Request failed with status code {response.status}: {chunk}")
                                    output.error = f"{response.reason} {chunk}".strip()
                                    output.success = False
                                    break

                                choices = data.get("choices", [])
                                if not choices and data.get("usage"):
                                    output.prompt_len = data["usage"]["prompt_tokens"]
                                    # output.completion_len = data["usage"]["completion_tokens"] if data["usage"]["completion_tokens"] != 0 else request_func_input.output_len
                                    continue

                                delta = choices[0].get("delta", {})
                                if "content" in delta:
                                    if output.ttft == 0.0:
                                        logger.debug("set ttft...")
                                        output.ttft = time.perf_counter() - st
                                    else:
                                        output.itl.append(timestamp - most_recent_timestamp)
                                    output.generated_text += delta["content"] if delta["content"] else ""
                                    output.completion_len += 1

                                most_recent_timestamp = timestamp
                    else:
                        error_message = ''.join([chunk.decode('utf-8').strip() async for chunk in response.content])
                        output.error = f"{response.reason} {error_message}".strip()
                        logger.error(f"Request failed with status code {response.status}: {output.error}")
                        output.success = False
            except Exception as e:
                output.success = False
                output.error = "".join(traceback.format_exception(*sys.exc_info()))
                logger.error(f"Request failed with error: {output.error}")

        if pbar:
            pbar.update(1)

        return output
