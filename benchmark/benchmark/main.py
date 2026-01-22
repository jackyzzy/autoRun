# -*- coding: utf-8 -*-
import argparse
import json
import os
import random
import time
import uuid
from collections import OrderedDict

import numpy as np
from transformers import AutoTokenizer

from .cli import parse_args
from .runner import BenchmarkRunner
from .util import logger, DatasetLoader


def check_metadata_args(metadata):
    metadata_dict = OrderedDict()
    if metadata:
        for item in metadata:
            key, value = item.split('=')
            metadata_dict[key] = value

    if 'case_id' not in metadata_dict:
        metadata_dict['case_id'] = uuid.uuid1()
    return metadata_dict


def main():
    args = parse_args()
    # logger = setup_logging(args.model, args.log_level)
    if args.api_key:
        os.environ["OPENAI_API_KEY"] = args.api_key
    stime = time.time()

    logger.info(args)
    random.seed(args.seed)
    np.random.seed(args.seed)
    global WARMED
    WARMED = args.disable_warm
    metadata_dict = check_metadata_args(args.metadata)
    if args.case_id:
        metadata_dict['case_id'] = args.case_id

    # 解析自定义数据集，dataset_field参数为字典
    dataset_field = args.dataset_field
    if dataset_field is not None:
        try:
            dataset_field = json.loads(dataset_field)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse dataset_field as JSON: {dataset_field}")
            dataset_field = None
    load_dataset = DatasetLoader(args.dataset_path, args.dataset_name, dataset_field)
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer_path, trust_remote_code=True) \
        if args.tokenizer_path else None

    runner = BenchmarkRunner(args, tokenizer=tokenizer, load_dataset=load_dataset, metadata_dict=metadata_dict)

    if args.dataset_name == "filtered":
        if isinstance(args.max_concurrency, int):
            args.max_concurrency = [args.max_concurrency]
        runner.run_static()
    elif args.dataset_name == 'sharegpt':
        if not tokenizer:
            raise ValueError("Please set --tokenizer-path.")
        runner.run_dynamic()
    else:
        raise argparse.ArgumentTypeError("Only Support filtered now.")

    logger.info(f"✅ Benchmark complete, test cost: {time.time() - stime}s")


if __name__ == "__main__":
    main()
