"""
结果收集器
负责收集、汇总和分析测试结果数据
"""

import os
import json
import logging
import tarfile
import shutil
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import yaml

from .node_manager import NodeManager
from .benchmark_runner import TestResult
from .scenario_manager import Scenario
from .scenario_runner import ScenarioResult
from .test_script_executor import TestExecutionResult


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


class ResultCollector:
    """结果收集器"""
    
    def __init__(self, node_manager: NodeManager, results_base_dir: str = "results"):
        self.logger = logging.getLogger("playbook.result_collector")
        self.node_manager = node_manager
        self.results_base_dir = Path(results_base_dir)
        self.results_base_dir.mkdir(parents=True, exist_ok=True)
        
        # 收集任务
        self.active_tasks: Dict[str, CollectionTask] = {}
        
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

            # 创建收集摘要对象
            collection_summary = CollectionSummary(
                collection_time=datetime.now().isoformat(),
                mode=mode.value
            )

            # 1. 收集测试结果artifacts
            if mode in [CollectionMode.BASIC, CollectionMode.STANDARD, CollectionMode.COMPREHENSIVE]:
                self._collect_test_artifacts(scenario_result_dir, test_execution_result, collection_summary)

            # 2. 收集服务日志
            if mode in [CollectionMode.STANDARD, CollectionMode.COMPREHENSIVE]:
                self._collect_service_logs(scenario, scenario_result_dir, collection_summary)

            # 3. 收集系统日志
            if mode == CollectionMode.COMPREHENSIVE:
                self._collect_system_logs(participating_nodes, scenario_result_dir, collection_summary)

            # 4. 保存元数据
            self._save_metadata(scenario, scenario_result, collection_summary, scenario_result_dir)

            # 5. 生成结果摘要（兼容旧接口）
            summary = self._generate_result_summary_from_collection(
                scenario_name, timestamp, scenario_result, collection_summary
            )

            # 6. 保存摘要和生成报告
            self._save_result_summary(scenario_result_dir, summary)
            self._generate_reports_v2(scenario_result_dir, summary, collection_summary)

            task.status = "completed"
            self.logger.info(f"Result collection completed for scenario {scenario_name}")

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
        scenario_result_dir = self.results_base_dir / timestamp / scenario_name

        # 创建主目录
        scenario_result_dir.mkdir(parents=True, exist_ok=True)

        # 创建子目录
        (scenario_result_dir / "test_artifacts").mkdir(exist_ok=True)
        (scenario_result_dir / "nodes").mkdir(exist_ok=True)
        (scenario_result_dir / "metadata").mkdir(exist_ok=True)

        self.logger.debug(f"Created result directory structure: {scenario_result_dir}")
        return scenario_result_dir

    def _get_participating_nodes(self, scenario: Scenario) -> List[str]:
        """获取参与场景的所有节点列表"""
        nodes = set()

        # 从服务部署中获取节点
        if scenario.metadata and scenario.metadata.services:
            for service in scenario.metadata.services:
                nodes.update(service.nodes)

        # 添加测试执行节点（如果是远程执行）
        if scenario.metadata and scenario.metadata.test_execution:
            test_node = scenario.metadata.test_execution.node
            if test_node and test_node != "local":
                nodes.add(test_node)

        return list(nodes)

    def _collect_test_artifacts(self, result_dir: Path, test_execution_result: TestExecutionResult,
                              collection_summary: CollectionSummary):
        """收集测试结果artifacts"""
        artifacts_dir = result_dir / "test_artifacts"
        collected_count = 0
        total_size = 0

        if test_execution_result and test_execution_result.artifacts:
            self.logger.info(f"Collecting {len(test_execution_result.artifacts)} test artifacts")

            for artifact_path in test_execution_result.artifacts:
                try:
                    source_path = Path(artifact_path)
                    if source_path.exists():
                        # 计算目标路径，保持相对路径结构
                        if source_path.is_absolute():
                            # 对于绝对路径，使用文件名
                            target_path = artifacts_dir / source_path.name
                        else:
                            # 对于相对路径，保持目录结构
                            target_path = artifacts_dir / source_path

                        # 确保目标目录存在
                        target_path.parent.mkdir(parents=True, exist_ok=True)

                        # 复制文件
                        if source_path.is_file():
                            shutil.copy2(source_path, target_path)
                            collected_count += 1
                            total_size += source_path.stat().st_size
                            self.logger.debug(f"Copied artifact: {source_path} -> {target_path}")
                        elif source_path.is_dir():
                            shutil.copytree(source_path, target_path, dirs_exist_ok=True)
                            # 计算目录大小
                            for file_path in target_path.rglob('*'):
                                if file_path.is_file():
                                    collected_count += 1
                                    total_size += file_path.stat().st_size
                            self.logger.debug(f"Copied artifact directory: {source_path} -> {target_path}")
                    else:
                        self.logger.warning(f"Artifact not found: {artifact_path}")
                        collection_summary.collection_errors.append(f"Artifact not found: {artifact_path}")

                except Exception as e:
                    error_msg = f"Failed to collect artifact {artifact_path}: {e}"
                    self.logger.error(error_msg)
                    collection_summary.collection_errors.append(error_msg)

        # 更新收集摘要
        collection_summary.test_artifacts = {
            'count': collected_count,
            'total_size_mb': total_size / (1024 * 1024),
            'source': 'test_execution_result'
        }
        collection_summary.total_files_collected += collected_count
        collection_summary.total_size_mb += total_size / (1024 * 1024)

        self.logger.info(f"Collected {collected_count} test artifacts ({total_size / (1024 * 1024):.2f} MB)")
    
    def _collect_service_logs(self, scenario: Scenario, result_dir: Path, collection_summary: CollectionSummary):
        """收集服务日志"""
        if not scenario.metadata or not scenario.metadata.services:
            self.logger.info("No services configured, skipping service log collection")
            return

        nodes_dir = result_dir / "nodes"

        for service in scenario.metadata.services:
            for node_name in service.nodes:
                try:
                    # 为每个节点创建目录
                    node_dir = nodes_dir / node_name / "services"
                    node_dir.mkdir(parents=True, exist_ok=True)

                    # 收集容器日志
                    log_cmd = f"docker logs {service.name} --tail 100 --timestamps"
                    results = self.node_manager.execute_command(log_cmd, [node_name], timeout=30)

                    node_result = results.get(node_name)
                    if node_result and node_result[0] == 0:
                        log_content = node_result[1]

                        # 保存日志文件
                        log_file = node_dir / f"{service.name}.log"
                        with open(log_file, 'w', encoding='utf-8') as f:
                            f.write(f"# Container logs for {service.name} on {node_name}\n")
                            f.write(f"# Collected at: {datetime.now().isoformat()}\n\n")
                            f.write(log_content)

                        # 更新统计
                        log_size = log_file.stat().st_size
                        collection_summary.total_files_collected += 1
                        collection_summary.total_size_mb += log_size / (1024 * 1024)

                        # 更新节点结果
                        if node_name not in collection_summary.node_results:
                            collection_summary.node_results[node_name] = {
                                'services_collected': [],
                                'system_logs_collected': False,
                                'collection_errors': []
                            }
                        collection_summary.node_results[node_name]['services_collected'].append(service.name)

                        self.logger.debug(f"Collected service log: {service.name}@{node_name}")
                    else:
                        error_msg = f"Failed to collect logs for {service.name}@{node_name}"
                        if node_result:
                            error_msg += f": {node_result[2]}"
                        self.logger.warning(error_msg)
                        collection_summary.collection_errors.append(error_msg)

                except Exception as e:
                    error_msg = f"Error collecting logs for {service.name}@{node_name}: {e}"
                    self.logger.error(error_msg)
                    collection_summary.collection_errors.append(error_msg)

        self.logger.info(f"Service log collection completed for {len(scenario.metadata.services)} services")

    def _collect_system_logs(self, node_names: List[str], result_dir: Path, collection_summary: CollectionSummary):
        """收集系统日志"""
        nodes_dir = result_dir / "nodes"

        system_commands = {
            'docker.log': 'sudo journalctl -u docker --no-pager --lines=50',
            'system.log': 'sudo journalctl --no-pager --lines=50 --since="1 hour ago"'
        }

        for node_name in node_names:
            try:
                node_dir = nodes_dir / node_name / "system"
                node_dir.mkdir(parents=True, exist_ok=True)

                for log_name, command in system_commands.items():
                    try:
                        results = self.node_manager.execute_command(command, [node_name], timeout=30)
                        node_result = results.get(node_name)

                        if node_result and node_result[0] == 0:
                            log_file = node_dir / log_name
                            with open(log_file, 'w', encoding='utf-8') as f:
                                f.write(f"# System log: {log_name} from {node_name}\n")
                                f.write(f"# Command: {command}\n")
                                f.write(f"# Collected at: {datetime.now().isoformat()}\n\n")
                                f.write(node_result[1])

                            # 更新统计
                            log_size = log_file.stat().st_size
                            collection_summary.total_files_collected += 1
                            collection_summary.total_size_mb += log_size / (1024 * 1024)

                    except Exception as e:
                        error_msg = f"Failed to collect {log_name} from {node_name}: {e}"
                        self.logger.warning(error_msg)
                        collection_summary.collection_errors.append(error_msg)

                # 标记该节点的系统日志已收集
                if node_name not in collection_summary.node_results:
                    collection_summary.node_results[node_name] = {
                        'services_collected': [],
                        'system_logs_collected': False,
                        'collection_errors': []
                    }
                collection_summary.node_results[node_name]['system_logs_collected'] = True

            except Exception as e:
                error_msg = f"Error collecting system logs from {node_name}: {e}"
                self.logger.error(error_msg)
                collection_summary.collection_errors.append(error_msg)

        self.logger.info(f"System log collection completed for {len(node_names)} nodes")

    def _save_metadata(self, scenario: Scenario, scenario_result: ScenarioResult,
                      collection_summary: CollectionSummary, result_dir: Path):
        """保存元数据"""
        metadata_dir = result_dir / "metadata"

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

    def _collect_files_from_nodes_legacy(self, task: CollectionTask) -> List[str]:
        """从节点收集文件 - 废弃的方法，保留用于向后兼容"""
        collected_files = []
        
        for node_name in task.node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                self.logger.error(f"Node not found: {node_name}")
                continue
            
            # 为每个节点创建子目录
            node_result_dir = Path(task.local_base_path) / node_name
            node_result_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                self.logger.warning(f"Legacy collection method called for {node_name}, using empty result")
                node_files = []
                collected_files.extend(node_files)
                
                self.logger.info(f"Collected {len(node_files)} files from node {node_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to collect files from node {node_name}: {e}")
        
        return collected_files
    
    def _collect_files_from_single_node(self, node, remote_path: str, local_dir: str) -> List[str]:
        """从单个节点收集文件"""
        collected_files = []
        
        client = node.get_ssh_client()
        
        with client.connection_context():
            # 检查远程路径是否存在
            if not client.directory_exists(remote_path):
                self.logger.warning(f"Remote path does not exist on {node.name}: {remote_path}")
                return []
            
            # 列出远程文件
            list_cmd = f"find {remote_path} -type f -name '*.json' -o -name '*.csv' -o -name '*.log' -o -name '*.txt' | head -100"
            exit_code, stdout, stderr = client.execute_command(list_cmd, timeout=60, check_exit_code=False)
            
            if exit_code != 0:
                self.logger.warning(f"Failed to list files on {node.name}: {stderr}")
                return []
            
            remote_files = [f.strip() for f in stdout.strip().split('\n') if f.strip()]
            
            # 下载每个文件
            for remote_file in remote_files:
                try:
                    # 计算本地文件路径
                    relative_path = os.path.relpath(remote_file, remote_path)
                    local_file = Path(local_dir) / relative_path
                    local_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 下载文件
                    success = client.download_file(remote_file, str(local_file))
                    if success:
                        collected_files.append(str(local_file))
                        self.logger.debug(f"Downloaded: {remote_file} -> {local_file}")
                    else:
                        self.logger.warning(f"Failed to download: {remote_file}")
                        
                except Exception as e:
                    self.logger.error(f"Error downloading {remote_file}: {e}")
        
        return collected_files
    
    def _save_test_metadata(self, result_dir: Path, test_results: Dict[str, TestResult]):
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
    
    def _generate_result_summary_from_collection(self, scenario_name: str, timestamp: str,
                                               scenario_result: ScenarioResult,
                                               collection_summary: CollectionSummary) -> ResultSummary:
        """基于新的收集结果生成ResultSummary（保持向后兼容）"""
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

    def _generate_result_summary_legacy(self, scenario_name: str, timestamp: str,
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
    
    def _save_result_summary(self, result_dir: Path, summary: ResultSummary):
        """保存结果摘要"""
        summary_file = result_dir / "result_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary.to_dict(), f, indent=2, ensure_ascii=False)
        
        # 同时保存YAML格式
        yaml_file = result_dir / "result_summary.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(summary.to_dict(), f, default_flow_style=False, allow_unicode=True)
        
        self.logger.debug(f"Saved result summary to {summary_file}")
    
    def _generate_reports_v2(self, result_dir: Path, summary: ResultSummary,
                           collection_summary: CollectionSummary):
        """生成报告 - 新版本"""
        try:
            # 生成Markdown报告
            self._generate_markdown_report_v2(result_dir, summary, collection_summary)

            # 生成收集统计报告
            self._generate_collection_report(result_dir, collection_summary)

        except Exception as e:
            self.logger.warning(f"Failed to generate some reports: {e}")

    def _generate_markdown_report_v2(self, result_dir: Path, summary: ResultSummary,
                                    collection_summary: CollectionSummary):
        """生成Markdown报告 - 新版本"""
        try:
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)

            report_content = f"""# 测试结果报告 - {summary.scenario_name}

## 基本信息
- **场景名称**: {summary.scenario_name}
- **测试时间**: {summary.timestamp}
- **收集模式**: {collection_summary.mode}
- **总节点数**: {summary.total_nodes}
- **成功节点数**: {summary.successful_nodes}
- **失败节点数**: {summary.failed_nodes}

## 测试结果收集
- **测试artifacts**: {collection_summary.test_artifacts.get('count', 0)} 个文件
- **artifacts大小**: {safe_format(collection_summary.test_artifacts.get('total_size_mb', 0), '.2f')} MB
- **总文件数**: {collection_summary.total_files_collected}
- **总大小**: {safe_format(collection_summary.total_size_mb, '.2f')} MB

## 分布式结果收集

"""

            # 每个节点的收集结果
            if collection_summary.node_results:
                report_content += "### 节点收集结果\n\n"
                report_content += "| 节点名称 | 服务日志 | 系统日志 | 错误数 |\n"
                report_content += "|----------|----------|----------|----------|\n"

                for node_name, node_info in collection_summary.node_results.items():
                    services = ', '.join(node_info.get('services_collected', []))
                    system_logs = '✓' if node_info.get('system_logs_collected', False) else '✗'
                    error_count = len(node_info.get('collection_errors', []))
                    report_content += f"| {node_name} | {services} | {system_logs} | {error_count} |\n"

            # 性能指标（如果有）
            if summary.total_requests > 0:
                report_content += f"""
## 性能指标
- **平均吞吐量**: {safe_format(summary.avg_throughput, '.2f')} tokens/s
- **最大吞吐量**: {safe_format(summary.max_throughput, '.2f')} tokens/s
- **最小吞吐量**: {safe_format(summary.min_throughput, '.2f')} tokens/s
- **平均延迟**: {safe_format(summary.avg_latency, '.3f')} s
- **总请求数**: {summary.total_requests}
- **成功请求数**: {summary.total_successful_requests}
- **总体成功率**: {safe_format(summary.overall_success_rate, '.2f')}%
"""

            # 错误信息
            if collection_summary.collection_errors:
                report_content += "\n## 收集错误\n\n"
                for error in collection_summary.collection_errors:
                    report_content += f"- {error}\n"

            markdown_file = result_dir / "test_report.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.debug(f"Generated Markdown report: {markdown_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate Markdown report: {e}")

    def _generate_collection_report(self, result_dir: Path, collection_summary: CollectionSummary):
        """生成收集统计报告"""
        try:
            report_content = f"""收集时间: {collection_summary.collection_time}
收集模式: {collection_summary.mode}
总文件数: {collection_summary.total_files_collected}
总大小: {collection_summary.total_size_mb:.2f} MB
错误数: {len(collection_summary.collection_errors)}

测试artifacts:
- 数量: {collection_summary.test_artifacts.get('count', 0)}
- 大小: {collection_summary.test_artifacts.get('total_size_mb', 0):.2f} MB
- 来源: {collection_summary.test_artifacts.get('source', 'unknown')}

节点结果:
"""

            for node_name, node_info in collection_summary.node_results.items():
                report_content += f"\n{node_name}:"
                report_content += f"\n  服务日志: {', '.join(node_info.get('services_collected', []))}"
                report_content += f"\n  系统日志: {'Yes' if node_info.get('system_logs_collected', False) else 'No'}"
                if node_info.get('collection_errors'):
                    report_content += f"\n  错误: {'; '.join(node_info['collection_errors'])}"

            if collection_summary.collection_errors:
                report_content += "\n\n所有错误:\n"
                for error in collection_summary.collection_errors:
                    report_content += f"- {error}\n"

            collection_file = result_dir / "collection_report.txt"
            with open(collection_file, 'w', encoding='utf-8') as f:
                f.write(report_content)

            self.logger.debug(f"Generated collection report: {collection_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate collection report: {e}")

    def _generate_reports_legacy(self, result_dir: Path, summary: ResultSummary,
                                test_results: Dict[str, TestResult]):
        """生成报告 - 旧版本，用于向后兼容"""
        try:
            # 生成CSV格式的性能报告
            self._generate_performance_csv_legacy(result_dir, test_results)

            # 生成Markdown报告
            self._generate_markdown_report(result_dir, summary)

            # 生成HTML报告（如果可能）
            self._generate_html_report(result_dir, summary)

        except Exception as e:
            self.logger.warning(f"Failed to generate some reports: {e}")
    
    def _generate_performance_csv_legacy(self, result_dir: Path, test_results: Dict[str, TestResult]):
        """生成性能数据CSV"""
        try:
            data = []
            for node_name, result in test_results.items():
                data.append({
                    'node_name': node_name,
                    'status': result.status.value,
                    'throughput': result.throughput,
                    'latency_mean': result.latency_mean,
                    'latency_p50': result.latency_p50,
                    'latency_p95': result.latency_p95,
                    'latency_p99': result.latency_p99,
                    'total_requests': result.total_requests,
                    'successful_requests': result.successful_requests,
                    'failed_requests': result.failed_requests,
                    'success_rate': result.success_rate,
                    'total_tokens': result.total_tokens,
                    'duration_seconds': result.duration,
                    'start_time': result.start_time.isoformat() if result.start_time else '',
                    'end_time': result.end_time.isoformat() if result.end_time else ''
                })
            
            df = pd.DataFrame(data)
            csv_file = result_dir / "performance_results.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8')
            
            self.logger.debug(f"Generated performance CSV: {csv_file}")
            
        except ImportError:
            self.logger.warning("pandas not available, skipping CSV generation")
        except Exception as e:
            self.logger.error(f"Failed to generate performance CSV: {e}")
    
    def _generate_markdown_report(self, result_dir: Path, summary: ResultSummary):
        """生成Markdown报告"""
        try:
            # 安全格式化数值，处理 None 值
            def safe_format(value, format_spec=''):
                if value is None:
                    return 'N/A'
                if format_spec:
                    return f"{value:{format_spec}}"
                return str(value)
            
            report_content = f"""# 测试结果报告 - {summary.scenario_name}

## 基本信息
- **场景名称**: {summary.scenario_name}
- **测试时间**: {summary.timestamp}
- **总节点数**: {summary.total_nodes}
- **成功节点数**: {summary.successful_nodes}
- **失败节点数**: {summary.failed_nodes}

## 性能指标汇总
- **平均吞吐量**: {safe_format(summary.avg_throughput, '.2f')} tokens/s
- **最大吞吐量**: {safe_format(summary.max_throughput, '.2f')} tokens/s
- **最小吞吐量**: {safe_format(summary.min_throughput, '.2f')} tokens/s
- **平均延迟**: {safe_format(summary.avg_latency, '.3f')} s
- **总请求数**: {summary.total_requests}
- **成功请求数**: {summary.total_successful_requests}
- **总体成功率**: {safe_format(summary.overall_success_rate, '.2f')}%

## 文件统计
- **结果文件数**: {summary.total_result_files}
- **总大小**: {safe_format(summary.total_size_mb, '.2f')} MB

## 各节点详细结果

| 节点名称 | 状态 | 吞吐量 | 平均延迟 | 成功率 | 执行时长 |
|---------|------|--------|----------|--------|----------|
"""
            
            for node_name, result in summary.node_results.items():
                status_icon = "✅" if result['status'] == 'completed' else "❌"
                throughput = safe_format(result['throughput'], '.2f')
                latency_mean = safe_format(result['latency_mean'], '.3f')
                success_rate = safe_format(result['success_rate'], '.1f')
                duration = safe_format(result['duration'], '.1f')
                report_content += f"| {node_name} | {status_icon} {result['status']} | {throughput} | {latency_mean} | {success_rate}% | {duration}s |\n"
            
            markdown_file = result_dir / "test_report.md"
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.debug(f"Generated Markdown report: {markdown_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to generate Markdown report: {e}")
    
    def _generate_html_report(self, result_dir: Path, summary: ResultSummary):
        """生成HTML报告"""
        # 这里可以使用模板引擎生成更丰富的HTML报告
        # 暂时生成简单的HTML版本
        pass
    
    def archive_results(self, result_dir: str, archive_name: str = None) -> str:
        """
        归档结果目录
        
        Args:
            result_dir: 结果目录路径
            archive_name: 归档文件名
            
        Returns:
            归档文件路径
        """
        result_path = Path(result_dir)
        if not result_path.exists():
            raise ValueError(f"Result directory does not exist: {result_dir}")
        
        if not archive_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{result_path.name}_{timestamp}.tar.gz"
        
        archive_path = result_path.parent / archive_name
        
        self.logger.info(f"Archiving results: {result_dir} -> {archive_path}")
        
        with tarfile.open(archive_path, 'w:gz') as tar:
            tar.add(result_path, arcname=result_path.name)
        
        self.logger.info(f"Results archived to: {archive_path}")
        return str(archive_path)

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
            collected_files = self._collect_files_from_nodes_legacy(task)
            task.collected_files = collected_files

            # 保存测试结果元数据
            self._save_test_metadata_legacy(scenario_result_dir, test_results)

            # 生成结果摘要
            summary = self._generate_result_summary_legacy(scenario_name, timestamp, test_results, collected_files)

            # 保存摘要
            self._save_result_summary(scenario_result_dir, summary)

            # 生成报告
            self._generate_reports_legacy(scenario_result_dir, summary, test_results)

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

    def _save_test_metadata_legacy(self, result_dir: Path, test_results: Dict[str, TestResult]):
        """保存测试元数据 - 旧版本"""
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

        self.logger.debug(f"Saved legacy test metadata to {metadata_file}")

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
            'total_result_directories': len([d for d in self.results_base_dir.iterdir() if d.is_dir()])
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