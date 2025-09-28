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

from .result_models import CollectionMode, CollectionTask, CollectionSummary, ResultSummary, TestSuiteResultSummary
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
                               mode: CollectionMode = CollectionMode.STANDARD,
                               custom_result_dir: Optional[Path] = None) -> ResultSummary:
        """
        收集场景测试结果 - 重构版本

        Args:
            scenario: 场景对象，包含服务-节点映射信息
            scenario_result: 场景执行结果
            test_execution_result: 测试执行结果，包含已收集的artifacts
            mode: 收集模式
            custom_result_dir: 自定义结果目录，如果提供则直接使用

        Returns:
            ResultSummary: 结果摘要
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scenario_name = scenario.name

        self.logger.info(f"Collecting results for scenario {scenario_name} with mode {mode.value}")

        # 确定结果目录
        if custom_result_dir:
            # 使用自定义目录
            scenario_result_dir = custom_result_dir
            # 确保子目录存在
            (scenario_result_dir / "artifacts").mkdir(exist_ok=True)
            (scenario_result_dir / "logs").mkdir(exist_ok=True)
            (scenario_result_dir / "metadata").mkdir(exist_ok=True)
            (scenario_result_dir / "summary").mkdir(exist_ok=True)
            self.logger.info(f"Using custom result directory: {scenario_result_dir}")
        else:
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

                # 获取实际的测试执行节点
                test_execution_node = scenario.metadata.test_execution.node if scenario.metadata and scenario.metadata.test_execution else "local"
                artifacts_dir = scenario_result_dir / "artifacts"

                artifacts_summary = self.transporter.collect_artifacts_from_test_node(
                    test_execution_result.artifacts, test_execution_node, artifacts_dir
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
                self.transporter.collect_service_logs(participating_nodes, scenario_result_dir, collection_summary, scenario)

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
        (base_dir / "summary").mkdir(exist_ok=True)

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

    def generate_test_suite_summary(self, scenario_results: Dict[str, Any],
                                   execution_summary: dict,
                                   health_report: dict,
                                   suite_result_dir: Optional[Path] = None) -> TestSuiteResultSummary:
        """
        生成测试套件汇总报告 - 基于已收集的场景结果进行汇总

        Args:
            scenario_results: 各场景的执行结果字典 {scenario_name: ScenarioResult}
            execution_summary: 执行摘要信息
            health_report: 健康检查报告
            suite_result_dir: 可选的测试套件结果目录，如果提供则在其下创建汇总

        Returns:
            TestSuiteResultSummary: 测试套件结果汇总
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.logger.info("Generating test suite summary from scenario results")

        # 确定测试套件级别的结果目录
        if suite_result_dir:
            # 使用提供的目录，在其下创建test_suite_summary子目录
            summary_dir = suite_result_dir / "test_suite_summary"
            summary_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Using provided suite result directory: {summary_dir}")
        else:
            # 创建新的测试套件级别的结果目录（向后兼容）
            summary_dir = self.results_base_dir / timestamp / "test_suite_summary"
            summary_dir.mkdir(parents=True, exist_ok=True)

        # 初始化汇总对象
        suite_summary = TestSuiteResultSummary(
            timestamp=timestamp,
            health_report=health_report,
            execution_summary=execution_summary
        )

        # 汇总各场景的结果
        total_files = 0
        total_size = 0.0
        total_execution_time = 0.0

        for scenario_name, scenario_result in scenario_results.items():
            suite_summary.total_scenarios += 1

            # 统计成功失败场景数
            if scenario_result.is_success:
                suite_summary.successful_scenarios += 1
            else:
                suite_summary.failed_scenarios += 1

            # 汇总执行时间
            if scenario_result.duration:
                total_execution_time += scenario_result.duration.total_seconds()

            # 从scenario_result.metrics中提取结果收集信息
            scenario_summary = {}
            if hasattr(scenario_result, 'metrics') and 'result_collection' in scenario_result.metrics:
                collection_info = scenario_result.metrics['result_collection']
                total_files += collection_info.get('total_files', 0)
                total_size += collection_info.get('total_size_mb', 0.0)

                scenario_summary = {
                    'status': scenario_result.status.value if hasattr(scenario_result.status, 'value') else str(scenario_result.status),
                    'duration_seconds': scenario_result.duration.total_seconds() if scenario_result.duration else 0,
                    'result_files': collection_info.get('total_files', 0),
                    'result_size_mb': collection_info.get('total_size_mb', 0.0),
                    'successful_nodes': collection_info.get('successful_nodes', 0),
                    'failed_nodes': collection_info.get('failed_nodes', 0),
                    'error_message': scenario_result.error_message if hasattr(scenario_result, 'error_message') else ""
                }
            else:
                # 如果没有结果收集信息，创建基本摘要
                scenario_summary = {
                    'status': scenario_result.status.value if hasattr(scenario_result.status, 'value') else str(scenario_result.status),
                    'duration_seconds': scenario_result.duration.total_seconds() if scenario_result.duration else 0,
                    'error_message': scenario_result.error_message if hasattr(scenario_result, 'error_message') else ""
                }

            suite_summary.scenario_summaries[scenario_name] = scenario_summary

        # 设置汇总统计
        suite_summary.total_result_files = total_files
        suite_summary.total_size_mb = total_size
        suite_summary.total_execution_time = total_execution_time

        # 计算总体成功率
        if suite_summary.total_scenarios > 0:
            suite_summary.overall_success_rate = (suite_summary.successful_scenarios / suite_summary.total_scenarios) * 100.0

        # 保存测试套件汇总
        summary_file = summary_dir / "test_suite_summary.json"
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(suite_summary.to_dict(), f, indent=2, ensure_ascii=False)
            self.logger.info(f"Test suite summary saved to {summary_file}")
        except Exception as e:
            self.logger.error(f"Failed to save test suite summary: {e}")

        # 生成测试套件报告
        try:
            self.reporter.generate_suite_report(summary_dir, suite_summary)
            self.reporter.generate_html_report(summary_dir, suite_summary)
        except Exception as e:
            self.logger.error(f"Failed to generate test suite report: {e}")

        self.logger.info(f"Test suite summary generated: {suite_summary.total_scenarios} scenarios, "
                        f"{suite_summary.successful_scenarios} successful, "
                        f"{suite_summary.overall_success_rate:.1f}% success rate")

        return suite_summary

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