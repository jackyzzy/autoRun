"""
结果收集相关的数据模型
定义结果收集过程中使用的数据结构
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class CollectionMode(Enum):
    """结果收集模式"""
    BASIC = "basic"                    # 仅测试artifacts
    STANDARD = "standard"              # artifacts + 服务日志
    COMPREHENSIVE = "comprehensive"    # 全部类型


@dataclass
class CollectionTask:
    """收集任务"""
    task_id: str
    scenario_name: str
    node_names: List[str]
    remote_paths: List[str]
    local_base_path: str
    mode: CollectionMode = CollectionMode.STANDARD
    status: str = "pending"  # pending, running, completed, failed
    collected_files: List[str] = field(default_factory=list)
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'task_id': self.task_id,
            'scenario_name': self.scenario_name,
            'node_names': self.node_names,
            'remote_paths': self.remote_paths,
            'local_base_path': self.local_base_path,
            'mode': self.mode.value,
            'status': self.status,
            'collected_files': self.collected_files,
            'error_message': self.error_message
        }


@dataclass
class CollectionSummary:
    """收集过程摘要"""
    collection_time: str
    mode: str
    test_artifacts: Dict[str, Any] = field(default_factory=dict)
    node_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    total_files_collected: int = 0
    total_size_mb: float = 0.0
    collection_errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'collection_time': self.collection_time,
            'mode': self.mode,
            'test_artifacts': self.test_artifacts,
            'node_results': self.node_results,
            'total_files_collected': self.total_files_collected,
            'total_size_mb': self.total_size_mb,
            'collection_errors': self.collection_errors
        }


@dataclass
class ResultSummary:
    """结果摘要"""
    scenario_name: str
    timestamp: str
    total_nodes: int
    successful_nodes: int
    failed_nodes: int
    
    # 性能指标汇总
    avg_throughput: float = 0.0
    max_throughput: float = 0.0
    min_throughput: float = 0.0
    avg_latency: float = 0.0
    total_requests: int = 0
    total_successful_requests: int = 0
    overall_success_rate: float = 0.0
    
    # 文件统计
    total_result_files: int = 0
    total_size_mb: float = 0.0
    
    # 详细结果
    node_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'scenario_name': self.scenario_name,
            'timestamp': self.timestamp,
            'total_nodes': self.total_nodes,
            'successful_nodes': self.successful_nodes,
            'failed_nodes': self.failed_nodes,
            'performance_metrics': {
                'avg_throughput': self.avg_throughput,
                'max_throughput': self.max_throughput,
                'min_throughput': self.min_throughput,
                'avg_latency': self.avg_latency,
                'total_requests': self.total_requests,
                'total_successful_requests': self.total_successful_requests,
                'overall_success_rate': self.overall_success_rate
            },
            'file_statistics': {
                'total_result_files': self.total_result_files,
                'total_size_mb': self.total_size_mb
            },
            'node_results': self.node_results
        }


@dataclass
class TestSuiteResultSummary:
    """测试套件结果汇总"""
    suite_name: str = "Full Test Suite"
    timestamp: str = ""
    total_scenarios: int = 0
    successful_scenarios: int = 0
    failed_scenarios: int = 0

    # 汇总的性能指标
    overall_avg_throughput: float = 0.0
    overall_max_throughput: float = 0.0
    overall_success_rate: float = 0.0
    total_execution_time: float = 0.0

    # 文件统计汇总
    total_result_files: int = 0
    total_size_mb: float = 0.0

    # 各场景摘要
    scenario_summaries: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # 健康检查和执行摘要
    health_report: Dict[str, Any] = field(default_factory=dict)
    execution_summary: Dict[str, Any] = field(default_factory=dict)

    @property
    def overall_success_rate_pct(self) -> float:
        """总体成功率百分比"""
        if self.total_scenarios == 0:
            return 0.0
        return (self.successful_scenarios / self.total_scenarios) * 100.0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'suite_name': self.suite_name,
            'timestamp': self.timestamp,
            'summary': {
                'total_scenarios': self.total_scenarios,
                'successful_scenarios': self.successful_scenarios,
                'failed_scenarios': self.failed_scenarios,
                'overall_success_rate': self.overall_success_rate_pct,
                'total_execution_time': self.total_execution_time
            },
            'performance_metrics': {
                'overall_avg_throughput': self.overall_avg_throughput,
                'overall_max_throughput': self.overall_max_throughput,
                'overall_success_rate': self.overall_success_rate
            },
            'file_statistics': {
                'total_result_files': self.total_result_files,
                'total_size_mb': self.total_size_mb
            },
            'scenario_summaries': self.scenario_summaries,
            'health_report': self.health_report,
            'execution_summary': self.execution_summary
        }