from collections import OrderedDict
from typing import Dict, List
import numpy as np
from .util import logger

async def find_optimal_batch(
    func,
    batch: List[int],
    func_kwargs: Dict = {},
    sparse_step: int = 20,
    dense_step: int = 10,
    goodput: Dict[str, int] = {"TTFT_P90": 10000, "TPOT_P90": 100},
    strategy: str = "fast"
):
    """
    寻找最优的batch参数
    
    参数:
        func: 评估函数，输入batch大小，输出包含吞吐量等指标的字典
        batch: 候选的batch大小列表
        func_kwargs: 传递给评估函数的额外参数
        sparse_step: 稀疏采样的步长，默认为50
        dense_step: 密集采样的步长，默认为20
        goodput: 期望的吞吐量指标，默认为{"TTFT_P99": 10000, "TPOT_P99": 100}
        strategy: 搜索策略，"fast"为二分法快速搜索，"exhaustive"为穷举搜索
    
    返回:
        最佳batch值, 对应的结果字典, 算法执行步骤列表
    """
    # 参数验证
    if not batch:
        logger.error("错误：batch列表不能为空")
        return None, None
    if sparse_step <= 0 or dense_step <= 0:
        logger.error("错误：采样步长必须为正数")
        return None, None
    if strategy not in ["fast", "exhaustive"]:
        logger.error("错误：策略必须为'fast'或'exhaustive'")
        return None, None

    request_rate_benchmark = False
    if isinstance(func_kwargs["request_rate"], list):
        request_rate_benchmark = True
    num_promps = len(func_kwargs["input_requests"])
    # 检查结果是否满足所有约束条件
    def check_conditions(result: Dict[str, float]) -> bool:
        for key, value in goodput.items():
            if key not in result:
                return False  # 如果结果中没有该指标，则不满足条件
            if result[key] > value:
                return False
        return True
    # 存储所有满足约束条件的结果
    valid_results: Dict[int, Dict[str, float]] = {}
    total_results: Dict[int, Dict[str, float]] = {}
    total_evaluations = 0
    if request_rate_benchmark:
        min_batch = min(func_kwargs["request_rate"])
        max_batch = max(func_kwargs["request_rate"])
    else:
        min_batch = min(batch)
        max_batch = max(batch)

    if strategy == "fast":
        left, right = min_batch, max_batch
        logger.info(f"1. 初始化搜索范围: [{left}, {right}]")
        # 二分搜索主循环
        while right - left > sparse_step:
            # 计算两个中间点
            if request_rate_benchmark:
                mid = round((left + right) / 2, 1)
            else:
                mid = (left + right) // 2
            logger.info(f"  当前搜索范围: [{left}, {right}], 当前评估{"request-rate" if request_rate_benchmark else "batch"}: {mid}")
            # 评估中间点
            if mid not in total_results:
                if request_rate_benchmark:
                    func_kwargs["request_rate"] = mid
                    func_kwargs["max_concurrency"] = num_promps
                else:
                    func_kwargs["max_concurrency"] = mid
                result = await func(**func_kwargs)
                total_results[mid] = result
                total_evaluations += 1
                if check_conditions(result):
                    valid_results[mid] = result
                    left = mid
                else:
                    right = mid
            else:
                index_result = total_results[mid]
                if check_conditions(index_result):
                    valid_results[mid] = index_result
                    left = mid
                else:
                    right = mid
        # 在最终范围内进行密集采样
        if request_rate_benchmark:
            dense_samples = [b for b in np.arange(left, right + 0.1 * dense_step, dense_step)]
        else:
            dense_samples = [b for b in range(left, right + 1, dense_step)]
        dense_samples = list(set(dense_samples) - set(valid_results.keys()))
        dense_samples.sort()
        logger.info(f"2. 在范围[{left}, {right}]内进行密集采样: {dense_samples}")
        
        for b in dense_samples:
            logger.info(f"  当前评估{"request-rate" if request_rate_benchmark else "batch"}: {b}")
            if request_rate_benchmark:
                func_kwargs["request_rate"] = b
                func_kwargs["max_concurrency"] = num_promps
            else:
                func_kwargs["max_concurrency"] = b
            result = await func(**func_kwargs)
            total_results[b] = result
            total_evaluations += 1
            if check_conditions(result):
                valid_results[b] = result

    else:
        best_batch = 0
        # 第一步：稀疏采样，了解大致趋势
        if request_rate_benchmark:
            sparse_samples = [b for b in np.arange(min_batch, max_batch + 0.1 * sparse_step, sparse_step)]
        else:
            sparse_samples = [b for b in range(min_batch, max_batch + 1, sparse_step)]
        sparse_samples = list(set(sparse_samples))
        sparse_samples.sort()
        logger.info(f"1. 稀疏采样点: {sparse_samples} (共{len(sparse_samples)}个点)")
        
        for batch in sparse_samples:
            logger.info(f"  当前评估batch: {batch}, 当前最高batch：{best_batch}")
            if request_rate_benchmark:
                func_kwargs["request_rate"] = batch
                func_kwargs["max_concurrency"] = num_promps
            else:
                func_kwargs["max_concurrency"] = batch
            result = await func(**func_kwargs)
            total_results[batch] = result
            total_evaluations += 1
            # 检查是否满足约束条件
            if check_conditions(result):
                valid_results[batch] = result
                if batch > best_batch:
                    best_batch = batch
    
        if not valid_results:
            logger.info(f"未找到满足约束条件的batch参数，共评估了{total_evaluations}个点")
            return total_results, valid_results
        # 第二步：分析稀疏采样结果，确定潜在最优区域, 进行密集采样
        if request_rate_benchmark:
            dense_samples = [b for b in np.arange(best_batch, best_batch + sparse_step + 0.1 * dense_step, dense_step)]
        else:
            dense_samples = [b for b in range(best_batch, best_batch + sparse_step + 1, dense_step)]
        dense_samples = list(set(dense_samples) - set(valid_results.keys())) 
        dense_samples.sort() # 排除已评估的点
        logger.info(f"2. 密集采样点: {dense_samples} (共{len(dense_samples)}个点)")
        
        for b in dense_samples:
            logger.info(f"  当前评估batch: {b}")
            if request_rate_benchmark:
                func_kwargs["request_rate"] = b
                func_kwargs["max_concurrency"] = num_promps
            else:
                func_kwargs["max_concurrency"] = b
            result = await func(**func_kwargs)
            total_results[b] = result
            total_evaluations += 1
            if check_conditions(result):
                valid_results[b] = result

    # 处理无有效结果的情况
    if not valid_results:
        logger.error(f"未找到满足约束条件的batch参数，共评估了{total_evaluations}个点")
        return total_results, valid_results

    # OrderedDict(sorted(valid_results.items()))
    return OrderedDict(sorted(total_results.items())), OrderedDict(sorted(valid_results.items()))
