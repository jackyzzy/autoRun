"""
重构后的结果收集器
整合各个专门化组件，提供统一的结果收集接口
"""

import os
import json
import logging
import shutil
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

from ..node_manager import NodeManager
from ..benchmark_runner import TestResult
from ..scenario_manager import Scenario
from ..scenario_runner import ScenarioResult
from ..test_script_executor import TestExecutionResult

from .result_models import CollectionMode, CollectionTask, CollectionSummary, ResultSummary
from .result_transporter import ResultTransporter
from .result_analyzer import ResultAnalyzer
from .result_reporter import ResultReporter


class ResultCollector:
    """重构后的结果收集器 - 协调各个专门化组件"""
    
    def __init__(self, node_manager: NodeManager, results_base_dir: str = "results"):
        self.logger = logging.getLogger("playbook.result_collector")
        self.node_manager = node_manager
        self.results_base_dir = Path(results_base_dir)
        self.results_base_dir.mkdir(parents=True, exist_ok=True)
        
        # 收集任务
        self.active_tasks: Dict[str, CollectionTask] = {}
        
        # 初始化专门化组件
        self.transporter = ResultTransporter(node_manager)
        self.analyzer = ResultAnalyzer()
        self.reporter = ResultReporter()
        
        self.logger.info(f"ResultCollector initialized with components architecture")
    
    def collect_scenario_results(self, scenario: Scenario, scenario_result: ScenarioResult,
                               test_execution_result: TestExecutionResult = None,
                               mode: CollectionMode = CollectionMode.STANDARD) -> ResultSummary:
        """
        收集场景测试结果 - 重构版本
        
        Args:
            scenario: 场景对象，包含服务-节点映射信息
            scenario_result: 场景执行结果
            test_execution_result: 测试执行结果，包含已收集的artifacts
            mode: 收集模式
            
        Returns:
            ResultSummary: 结果摘要
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scenario_name = scenario.name

        self.logger.info(f"Collecting results for scenario {scenario_name} with mode {mode.value}")

        # 创建标准化的结果目录结构
        scenario_result_dir = self._create_result_directory_structure(timestamp, scenario_name)

        # 创建收集任务
        task_id = f"collect_{scenario_name}_{timestamp}"

        # 获取参与的节点列表
        participating_nodes = self._get_participating_nodes(scenario)

        task = CollectionTask(
            task_id=task_id,
            scenario_name=scenario_name,
            node_names=participating_nodes,
            remote_paths=[],  # 不再使用过时的remote_paths
            local_base_path=str(scenario_result_dir),
            mode=mode
        )

        self.active_tasks[task_id] = task

        try:
            task.status = "running"

            # 初始化收集摘要
            collection_summary = CollectionSummary(
                collection_time=datetime.now().isoformat(),
                mode=mode.value
            )

            # 1. 收集测试artifacts（如果有）
            if test_execution_result and test_execution_result.artifacts:
                self.logger.info(f"Collecting {len(test_execution_result.artifacts)} test artifacts")
                artifacts_summary = self.transporter.collect_artifacts_from_nodes(
                    test_execution_result.artifacts, participating_nodes, scenario_result_dir
                )
                
                # 更新收集摘要
                collection_summary.total_files_collected += artifacts_summary.total_files_collected
                collection_summary.total_size_mb += artifacts_summary.total_size_mb
                collection_summary.test_artifacts = {
                    'count': artifacts_summary.total_files_collected,
                    'total_size_mb': artifacts_summary.total_size_mb,
                    'source': 'test_execution'
                }
                
                # 合并节点结果
                for node_name, node_info in artifacts_summary.node_results.items():
                    if node_name not in collection_summary.node_results:
                        collection_summary.node_results[node_name] = {}
                    collection_summary.node_results[node_name].update(node_info)

            # 2. 根据模式收集额外信息
            if mode in [CollectionMode.STANDARD, CollectionMode.COMPREHENSIVE]:
                # 收集服务日志
                self.transporter.collect_service_logs(participating_nodes, scenario_result_dir, collection_summary)

            if mode == CollectionMode.COMPREHENSIVE:
                # 收集系统日志
                self.transporter.collect_system_logs(participating_nodes, scenario_result_dir, collection_summary)

            # 3. 保存元数据
            self.analyzer.save_metadata(scenario, scenario_result, collection_summary, scenario_result_dir)

            # 4. 生成结果摘要
            summary = self.analyzer.generate_result_summary_from_collection(
                scenario_name, timestamp, scenario_result, collection_summary
            )

            # 5. 保存摘要
            self.reporter.save_result_summary(scenario_result_dir, summary)

            # 6. 生成报告
            self.reporter.generate_reports_v2(scenario_result_dir, summary, collection_summary)

            task.status = "completed"
            task.collected_files = [str(f) for f in scenario_result_dir.rglob('*') if f.is_file()]

            self.logger.info(f"Result collection completed for scenario {scenario_name}: "
                           f"{collection_summary.total_files_collected} files, "
                           f"{collection_summary.total_size_mb:.2f} MB")

            return summary

        except Exception as e:
            task.status = "failed"
            task.error_message = str(e)
            self.logger.error(f"Result collection failed for scenario {scenario_name}: {e}")
            raise

        finally:
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]

    def _create_result_directory_structure(self, timestamp: str, scenario_name: str) -> Path:
        """创建标准化的结果目录结构"""
        # 创建基础目录：results/timestamp/scenario_name/
        base_dir = self.results_base_dir / timestamp / scenario_name
        base_dir.mkdir(parents=True, exist_ok=True)

        # 创建子目录
        (base_dir / "artifacts").mkdir(exist_ok=True)
        (base_dir / "logs").mkdir(exist_ok=True)
        (base_dir / "metadata").mkdir(exist_ok=True)
        (base_dir / "reports").mkdir(exist_ok=True)

        return base_dir

    def _get_participating_nodes(self, scenario: Scenario) -> List[str]:
        """获取参与场景的节点列表"""
        participating_nodes = set()

        if scenario.metadata and scenario.metadata.services:
            for service in scenario.metadata.services:
                participating_nodes.update(service.nodes)

        # 如果没有明确的服务-节点映射，使用所有启用的节点
        if not participating_nodes:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            participating_nodes = {node.name for node in nodes}

        return list(participating_nodes)

    # ==========================
    # 向后兼容接口
    # ==========================

    def collect_scenario_results_legacy(self, scenario_name: str, test_results: Dict[str, TestResult],
                                       node_names: List[str] = None) -> ResultSummary:
        """
        收集场景测试结果 - 旧版本接口，用于向后兼容
        
        Args:
            scenario_name: 场景名称
            test_results: 测试结果字典
            node_names: 节点名称列表
            
        Returns:
            ResultSummary: 结果摘要
        """
        if node_names is None:
            node_names = list(test_results.keys())

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.logger.warning(f"Using legacy collect_scenario_results for {scenario_name}. "
                           "Consider upgrading to the new interface.")

        # 创建结果目录
        scenario_result_dir = self.results_base_dir / timestamp / scenario_name
        scenario_result_dir.mkdir(parents=True, exist_ok=True)

        # 创建收集任务
        task_id = f"collect_legacy_{scenario_name}_{timestamp}"

        task = CollectionTask(
            task_id=task_id,
            scenario_name=scenario_name,
            node_names=node_names,
            remote_paths=[],
            local_base_path=str(scenario_result_dir),
            mode=CollectionMode.BASIC
        )

        self.active_tasks[task_id] = task

        try:
            task.status = "running"

            # 收集文件（使用旧方法）
            collected_files = self.transporter.collect_files_from_nodes_legacy(task)
            task.collected_files = collected_files

            # 保存测试结果元数据
            self.analyzer.save_test_metadata(scenario_result_dir, test_results)

            # 生成结果摘要
            summary = self.analyzer.generate_result_summary_legacy(scenario_name, timestamp, test_results, collected_files)

            # 保存摘要
            self.reporter.save_result_summary(scenario_result_dir, summary)

            # 生成报告
            self.reporter.generate_markdown_report_legacy(scenario_result_dir, summary)

            task.status = "completed"
            self.logger.info(f"Legacy result collection completed for scenario {scenario_name}")

            return summary

        except Exception as e:
            task.status = "failed"
            task.error_message = str(e)
            self.logger.error(f"Legacy result collection failed for scenario {scenario_name}: {e}")
            raise

        finally:
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]

    # ==========================
    # 管理功能
    # ==========================

    def cleanup_old_results(self, days_to_keep: int = 30):
        """清理旧的结果文件"""
        cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
        
        cleaned_dirs = []
        cleaned_size = 0
        
        for item in self.results_base_dir.iterdir():
            if item.is_dir():
                try:
                    # 检查目录修改时间
                    if item.stat().st_mtime < cutoff_time:
                        # 计算目录大小
                        dir_size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                        
                        # 删除目录
                        shutil.rmtree(item)
                        
                        cleaned_dirs.append(str(item))
                        cleaned_size += dir_size
                        
                except Exception as e:
                    self.logger.error(f"Failed to clean up directory {item}: {e}")
        
        if cleaned_dirs:
            self.logger.info(f"Cleaned up {len(cleaned_dirs)} old result directories, "
                           f"freed {cleaned_size / (1024*1024):.2f} MB")
        
        return cleaned_dirs
    
    def get_collection_status(self) -> Dict[str, Any]:
        """获取收集状态"""
        return {
            'active_tasks': len(self.active_tasks),
            'tasks': {task_id: task.to_dict() for task_id, task in self.active_tasks.items()},
            'results_base_dir': str(self.results_base_dir),
            'total_result_directories': len([d for d in self.results_base_dir.iterdir() if d.is_dir()]),
            'components': {
                'transporter': 'ResultTransporter',
                'analyzer': 'ResultAnalyzer', 
                'reporter': 'ResultReporter'
            }
        }
    
    def list_result_directories(self) -> List[Dict[str, Any]]:
        """列出结果目录"""
        directories = []
        
        for item in self.results_base_dir.iterdir():
            if item.is_dir():
                try:
                    # 获取目录统计信息
                    file_count = sum(1 for _ in item.rglob('*') if _.is_file())
                    total_size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                    modified_time = datetime.fromtimestamp(item.stat().st_mtime)
                    
                    # 查找摘要文件
                    summary_file = item / "result_summary.json"
                    summary_data = {}
                    if summary_file.exists():
                        try:
                            with open(summary_file, 'r', encoding='utf-8') as f:
                                summary_data = json.load(f)
                        except:
                            pass
                    
                    directories.append({
                        'name': item.name,
                        'path': str(item),
                        'modified_time': modified_time.isoformat(),
                        'file_count': file_count,
                        'total_size_mb': total_size / (1024 * 1024),
                        'has_summary': summary_file.exists(),
                        'summary': summary_data
                    })
                    
                except Exception as e:
                    self.logger.error(f"Failed to get info for directory {item}: {e}")
        
        # 按修改时间排序（最新的在前）
        directories.sort(key=lambda x: x['modified_time'], reverse=True)
        return directories

    def archive_results(self, result_dir: str, archive_name: str = None) -> str:
        """归档结果目录"""
        return self.reporter.archive_results(result_dir, archive_name)