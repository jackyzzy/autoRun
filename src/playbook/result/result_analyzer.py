"""
结果分析器
负责处理测试结果的数据分析和指标计算
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

from ..benchmark_runner import TestResult
from ..scenario_manager import Scenario
from ..scenario_runner import ScenarioResult
from ..test_script_executor import TestExecutionResult
from .result_models import CollectionSummary, ResultSummary


class ResultAnalyzer:
    """结果数据分析器"""
    
    def __init__(self):
        self.logger = logging.getLogger("playbook.result_analyzer")
    
    def generate_result_summary_from_collection(self, scenario_name: str, timestamp: str,
                                               scenario_result: ScenarioResult,
                                               collection_summary: CollectionSummary) -> ResultSummary:
        """基于新的收集结果生成ResultSummary"""
        summary = ResultSummary(
            scenario_name=scenario_name,
            timestamp=timestamp,
            total_nodes=len(collection_summary.node_results),
            successful_nodes=1 if scenario_result.is_success else 0,
            failed_nodes=0 if scenario_result.is_success else 1
        )

        # 从 scenario_result 中获取性能指标
        test_metrics = scenario_result.metrics.get('test_execution', {})

        # 填充性能指标（如果有）
        if 'metrics' in test_metrics and test_metrics['metrics']:
            metrics = test_metrics['metrics']
            summary.avg_throughput = metrics.get('throughput', 0.0)
            summary.max_throughput = metrics.get('max_throughput', 0.0)
            summary.min_throughput = metrics.get('min_throughput', 0.0)
            summary.avg_latency = metrics.get('latency_mean', 0.0)
            summary.total_requests = metrics.get('total_requests', 0)
            summary.total_successful_requests = metrics.get('successful_requests', 0)
            if summary.total_requests > 0:
                summary.overall_success_rate = (summary.total_successful_requests / summary.total_requests * 100)

        # 文件统计从 collection_summary 获取
        summary.total_result_files = collection_summary.total_files_collected
        summary.total_size_mb = collection_summary.total_size_mb

        # 节点结果从 collection_summary 转换
        for node_name, node_info in collection_summary.node_results.items():
            summary.node_results[node_name] = {
                'status': 'completed' if scenario_result.is_success else 'failed',
                'services_collected': node_info.get('services_collected', []),
                'system_logs_collected': node_info.get('system_logs_collected', False),
                'collection_errors': node_info.get('collection_errors', [])
            }

        return summary

    def generate_result_summary_legacy(self, scenario_name: str, timestamp: str,
                                       test_results: Dict[str, TestResult],
                                       collected_files: List[str]) -> ResultSummary:
        """生成结果摘要 - 旧版本，用于向后兼容"""
        summary = ResultSummary(
            scenario_name=scenario_name,
            timestamp=timestamp,
            total_nodes=len(test_results),
            successful_nodes=0,
            failed_nodes=0
        )

        # 统计性能指标
        throughputs = []
        latencies = []
        total_requests = 0
        total_successful = 0

        for node_name, result in test_results.items():
            # 节点状态统计
            if result.status.value == "completed":
                summary.successful_nodes += 1
            else:
                summary.failed_nodes += 1

            # 性能指标统计
            if result.throughput > 0:
                throughputs.append(result.throughput)
            if result.latency_mean > 0:
                latencies.append(result.latency_mean)

            total_requests += result.total_requests
            total_successful += result.successful_requests

            # 保存节点详细结果
            summary.node_results[node_name] = {
                'status': result.status.value,
                'throughput': result.throughput,
                'latency_mean': result.latency_mean,
                'total_requests': result.total_requests,
                'successful_requests': result.successful_requests,
                'success_rate': result.success_rate,
                'duration': result.duration,
                'error_message': result.error_message
            }

        # 计算汇总指标
        if throughputs:
            summary.avg_throughput = sum(throughputs) / len(throughputs)
            summary.max_throughput = max(throughputs)
            summary.min_throughput = min(throughputs)

        if latencies:
            summary.avg_latency = sum(latencies) / len(latencies)

        summary.total_requests = total_requests
        summary.total_successful_requests = total_successful
        summary.overall_success_rate = (total_successful / total_requests * 100) if total_requests > 0 else 0

        # 文件统计
        summary.total_result_files = len(collected_files)

        total_size = 0
        for file_path in collected_files:
            try:
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
            except:
                pass

        summary.total_size_mb = total_size / (1024 * 1024)

        return summary
    
    def analyze_performance_trends(self, summary: ResultSummary) -> Dict[str, Any]:
        """分析性能趋势"""
        analysis = {
            'performance_overview': {
                'avg_throughput': summary.avg_throughput,
                'throughput_range': {
                    'min': summary.min_throughput,
                    'max': summary.max_throughput,
                    'variance': summary.max_throughput - summary.min_throughput if summary.max_throughput > 0 else 0
                },
                'avg_latency': summary.avg_latency,
                'success_rate': summary.overall_success_rate
            },
            'node_performance': {}
        }
        
        # 分析各节点性能
        throughputs = []
        latencies = []
        success_rates = []
        
        for node_name, result in summary.node_results.items():
            if 'throughput' in result:
                throughputs.append(result['throughput'])
            if 'latency_mean' in result:
                latencies.append(result['latency_mean'])
            if 'success_rate' in result:
                success_rates.append(result['success_rate'])
            
            analysis['node_performance'][node_name] = {
                'status': result.get('status', 'unknown'),
                'throughput': result.get('throughput', 0),
                'latency': result.get('latency_mean', 0),
                'success_rate': result.get('success_rate', 0)
            }
        
        # 计算统计指标
        if throughputs:
            analysis['statistics'] = {
                'throughput': {
                    'mean': sum(throughputs) / len(throughputs),
                    'median': sorted(throughputs)[len(throughputs) // 2],
                    'std_dev': self._calculate_std_dev(throughputs)
                }
            }
            
            if latencies:
                analysis['statistics']['latency'] = {
                    'mean': sum(latencies) / len(latencies),
                    'median': sorted(latencies)[len(latencies) // 2],
                    'std_dev': self._calculate_std_dev(latencies)
                }
        
        return analysis
    
    def analyze_error_patterns(self, collection_summary: CollectionSummary) -> Dict[str, Any]:
        """分析错误模式"""
        error_analysis = {
            'total_errors': len(collection_summary.collection_errors),
            'error_categories': {},
            'node_error_distribution': {},
            'common_error_patterns': []
        }
        
        # 分析错误分布
        for node_name, node_info in collection_summary.node_results.items():
            node_errors = node_info.get('collection_errors', [])
            error_analysis['node_error_distribution'][node_name] = len(node_errors)
        
        # 分析错误类型
        all_errors = collection_summary.collection_errors.copy()
        for node_info in collection_summary.node_results.values():
            all_errors.extend(node_info.get('collection_errors', []))
        
        # 简单的错误分类
        error_keywords = {
            'connection': ['connection', 'timeout', 'network'],
            'permission': ['permission', 'access', 'denied'],
            'file': ['file', 'directory', 'path'],
            'service': ['service', 'container', 'docker']
        }
        
        for error_msg in all_errors:
            error_lower = error_msg.lower()
            for category, keywords in error_keywords.items():
                if any(keyword in error_lower for keyword in keywords):
                    if category not in error_analysis['error_categories']:
                        error_analysis['error_categories'][category] = 0
                    error_analysis['error_categories'][category] += 1
                    break
        
        return error_analysis
    
    def compare_with_historical_data(self, summary: ResultSummary, 
                                   historical_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """与历史数据比较"""
        if not historical_results:
            return {'comparison': 'no_historical_data'}
        
        comparison = {
            'current_performance': {
                'throughput': summary.avg_throughput,
                'latency': summary.avg_latency,
                'success_rate': summary.overall_success_rate
            },
            'historical_average': {},
            'performance_trend': {}
        }
        
        # 计算历史平均值
        historical_throughputs = []
        historical_latencies = []
        historical_success_rates = []
        
        for historical in historical_results:
            perf_metrics = historical.get('performance_metrics', {})
            historical_throughputs.append(perf_metrics.get('avg_throughput', 0))
            historical_latencies.append(perf_metrics.get('avg_latency', 0))
            historical_success_rates.append(perf_metrics.get('overall_success_rate', 0))
        
        if historical_throughputs:
            avg_throughput = sum(historical_throughputs) / len(historical_throughputs)
            comparison['historical_average']['throughput'] = avg_throughput
            comparison['performance_trend']['throughput'] = {
                'change': summary.avg_throughput - avg_throughput,
                'change_percent': ((summary.avg_throughput - avg_throughput) / avg_throughput * 100) if avg_throughput > 0 else 0
            }
        
        if historical_latencies:
            avg_latency = sum(historical_latencies) / len(historical_latencies)
            comparison['historical_average']['latency'] = avg_latency
            comparison['performance_trend']['latency'] = {
                'change': summary.avg_latency - avg_latency,
                'change_percent': ((summary.avg_latency - avg_latency) / avg_latency * 100) if avg_latency > 0 else 0
            }
        
        return comparison
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """计算标准差"""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def save_metadata(self, scenario: Scenario, scenario_result: ScenarioResult,
                      collection_summary: CollectionSummary, result_dir: Path):
        """保存分析元数据"""
        metadata_dir = result_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)

        # 1. 保存场景元数据
        scenario_metadata = {
            'name': scenario.name,
            'description': scenario.description,
            'directory': scenario.directory,
            'metadata': scenario.metadata.to_dict() if scenario.metadata else None
        }
        with open(metadata_dir / "scenario_metadata.json", 'w', encoding='utf-8') as f:
            json.dump(scenario_metadata, f, indent=2, ensure_ascii=False)

        # 2. 保存服务部署映射
        if scenario.metadata and scenario.metadata.services:
            service_deployment = {
                'services': {
                    service.name: {
                        'nodes': service.nodes,
                        'compose_file': service.compose_file,
                        'depends_on': service.depends_on
                    }
                    for service in scenario.metadata.services
                }
            }
            with open(metadata_dir / "service_deployment.json", 'w', encoding='utf-8') as f:
                json.dump(service_deployment, f, indent=2, ensure_ascii=False)

        # 3. 保存测试执行结果
        test_execution_metadata = {
            'scenario_result': scenario_result.to_dict(),
            'test_metrics': scenario_result.metrics.get('test_execution', {})
        }
        with open(metadata_dir / "test_execution.json", 'w', encoding='utf-8') as f:
            json.dump(test_execution_metadata, f, indent=2, ensure_ascii=False)

        # 4. 保存收集摘要
        with open(metadata_dir / "collection_summary.json", 'w', encoding='utf-8') as f:
            json.dump(collection_summary.to_dict(), f, indent=2, ensure_ascii=False)

        self.logger.debug("Metadata saved successfully")

    def save_test_metadata(self, result_dir: Path, test_results: Dict[str, TestResult]):
        """保存测试元数据"""
        metadata = {
            'collection_time': datetime.now().isoformat(),
            'test_results': {
                node_name: result.to_dict() 
                for node_name, result in test_results.items()
            }
        }
        
        metadata_file = result_dir / "test_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        self.logger.debug(f"Saved test metadata to {metadata_file}")