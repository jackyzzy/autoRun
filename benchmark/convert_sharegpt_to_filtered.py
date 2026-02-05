#!/usr/bin/env python3
"""将 ShareGPT 数据集转换为 filtered 格式，用于静态测试"""

import argparse
import json
from transformers import AutoTokenizer
from tqdm import tqdm

# 要生成的 input_len 分组
target_lengths = [128, 512, 1024, 2048, 4096]
samples_per_length = 500  # 每个长度采样数量


def parse_args():
    parser = argparse.ArgumentParser(description="将 ShareGPT 数据集转换为 filtered 格式，用于静态测试")
    parser.add_argument("--sharegpt-path", type=str,
                        default="/data/dataset/ShareGPT_V3/ShareGPT_V3_unfiltered_cleaned_split1.json",
                        help="ShareGPT 数据集路径")
    parser.add_argument("--output-path", type=str,
                        default="/data/dataset/filtered/filtered_from_sharegpt1.json",
                        help="输出文件路径")
    parser.add_argument("--tokenizer-path", type=str,
                        default="/data/model/qwen2.5-32",
                        help="Tokenizer 路径")
    return parser.parse_args()


def main():
    args = parse_args()

    # 加载 tokenizer
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer_path, trust_remote_code=True)
    
    # 加载 ShareGPT 数据
    with open(args.sharegpt_path, 'r', encoding='utf-8') as f:
        sharegpt_data = json.load(f)
    
    print(f"Loaded {len(sharegpt_data)} conversations from ShareGPT")
    
    # 提取所有 human 消息
    all_prompts = []
    for conv in sharegpt_data:
        if 'conversations' in conv:
            for turn in conv['conversations']:
                if turn.get('from') == 'human':
                    all_prompts.append(turn.get('value', ''))
    
    print(f"Extracted {len(all_prompts)} prompts")
    
    # 按 token 长度分组
    filtered_data = {str(length): [] for length in target_lengths}
    
    for prompt in tqdm(all_prompts, desc="Processing prompts"):
        if not prompt.strip():
            continue
        
        token_ids = tokenizer.encode(prompt)
        token_len = len(token_ids)
        
        # 找到最接近的目标长度
        for target_len in target_lengths:
            # 允许 ±20% 的误差范围
            if target_len * 0.8 <= token_len <= target_len * 1.2:
                if len(filtered_data[str(target_len)]) < samples_per_length:
                    filtered_data[str(target_len)].append(prompt)
                break
    
    # 打印统计
    print("\n=== 统计结果 ===")
    for length in target_lengths:
        count = len(filtered_data[str(length)])
        print(f"  {length}: {count} samples")
    
    # 保存
    with open(args.output_path, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nSaved to {args.output_path}")

if __name__ == "__main__":
    main()