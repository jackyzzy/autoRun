"""
场景执行器
负责按顺序执行测试场景，管理场景生命周期
"""

import os
import time
import logging
import asyncio
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
from datetime import datetime, timedelta
from enum import Enum
import json

from .scenario_manager import ScenarioManager, Scenario
from .node_manager import NodeManager
from .docker_compose_manager import DockerComposeManager
from .dependency_resolver import ServiceDependencyResolver
from .health_check_manager import HealthCheckManager
from .test_script_executor import TestScriptExecutor
from .scenario_resource_manager import ScenarioResourceManager
from .concurrent_deployer import (
    ConcurrentServiceDeployer, ConcurrentDeploymentLogger, ConcurrentRetryStrategy
)
from ..utils.logger import setup_scenario_logger, LogCapture


class ScenarioStatus(Enum):
    """场景状态枚举"""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class ScenarioResult:
    """场景执行结果"""
    
    def __init__(self, scenario_name: str):
        self.scenario_name = scenario_name
        self.status = ScenarioStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: Optional[timedelta] = None
        self.error_message: str = ""
        self.logs: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self.artifacts: List[str] = []  # 生成的文件路径
        
    @property
    def is_success(self) -> bool:
        return self.status == ScenarioStatus.COMPLETED
        
    @property
    def is_failure(self) -> bool:
        return self.status == ScenarioStatus.FAILED
        
    def start(self):
        """开始执行"""
        self.status = ScenarioStatus.RUNNING
        self.start_time = datetime.now()
        
    def complete(self):
        """完成执行"""
        self.status = ScenarioStatus.COMPLETED
        self.end_time = datetime.now()
        if self.start_time:
            self.duration = self.end_time - self.start_time
            
    def fail(self, error_message: str):
        """执行失败"""
        self.status = ScenarioStatus.FAILED
        self.error_message = error_message
        self.end_time = datetime.now()
        if self.start_time:
            self.duration = self.end_time - self.start_time
            
    def skip(self, reason: str = ""):
        """跳过执行"""
        self.status = ScenarioStatus.SKIPPED
        self.error_message = reason
        
    def cancel(self):
        """取消执行"""
        self.status = ScenarioStatus.CANCELLED
        self.end_time = datetime.now()
        if self.start_time:
            self.duration = self.end_time - self.start_time
            
    def to_dict(self) -> Dict[str, Any]:
        return {
            'scenario_name': self.scenario_name,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration.total_seconds() if self.duration else None,
            'error_message': self.error_message,
            'metrics': self.metrics,
            'artifacts': self.artifacts,
            'log_count': len(self.logs)
        }


class ScenarioRunner:
    """场景执行器"""
    
    def __init__(self, scenario_manager: ScenarioManager, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.scenario_runner")
        self.scenario_manager = scenario_manager
        self.node_manager = node_manager
        
        # 新组件初始化
        self.docker_manager = DockerComposeManager(node_manager)
        self.dependency_resolver = ServiceDependencyResolver(self.docker_manager)
        self.health_checker = HealthCheckManager(node_manager)
        self.test_executor = TestScriptExecutor(node_manager)

        # 并发部署组件
        self.concurrent_deployer = ConcurrentServiceDeployer(
            self.docker_manager, self.dependency_resolver
        )
        # 资源管理组件
        self.resource_manager = ScenarioResourceManager(node_manager)

        # 执行状态
        self.is_running = False
        self.current_scenario: Optional[str] = None
        self.results: Dict[str, ScenarioResult] = {}
        self.cancelled = False
        
        # 配置
        self.retry_count = 1
        self.wait_between_scenarios = 60
        self.continue_on_failure = False
        
        # 加载场景间配置
        inter_config = self.scenario_manager.inter_scenario_config
        self.retry_count = inter_config.get('retry_count', self.retry_count)
        self.wait_between_scenarios = inter_config.get('wait_between_scenarios', self.wait_between_scenarios)
        self.continue_on_failure = inter_config.get('continue_on_failure', self.continue_on_failure)
        
        # 回调函数
        self.on_scenario_start: Optional[Callable[[str], None]] = None
        self.on_scenario_complete: Optional[Callable[[str, ScenarioResult], None]] = None
        self.on_all_complete: Optional[Callable[[Dict[str, ScenarioResult]], None]] = None
    
    def run_all_scenarios(self) -> Dict[str, ScenarioResult]:
        """执行所有场景"""
        if self.is_running:
            raise RuntimeError("Scenario runner is already running")
        
        self.is_running = True
        self.cancelled = False
        self.results.clear()
        
        try:
            execution_order = self.scenario_manager.get_execution_order()
            self.logger.info(f"Starting execution of {len(execution_order)} scenarios")
            
            for i, scenario_name in enumerate(execution_order):
                if self.cancelled:
                    self.logger.info("Execution cancelled by user")
                    break
                
                self.logger.info(f"Executing scenario {i+1}/{len(execution_order)}: {scenario_name}")
                result = self.run_scenario(scenario_name)

                # 🧹 完全清理scenario后的所有资源，确保scenarios间完全隔离
                self.logger.info(f"Cleaning up resources after scenario: {scenario_name}")
                cleanup_success = self.resource_manager.cleanup_scenario_resources(scenario_name)
                if not cleanup_success:
                    self.logger.warning(f"Resource cleanup failed for scenario {scenario_name}, but continuing execution")

                if result.is_failure and not self.continue_on_failure:
                    self.logger.error(f"Scenario {scenario_name} failed, stopping execution")
                    # 标记剩余场景为跳过
                    for remaining_scenario in execution_order[i+1:]:
                        skip_result = ScenarioResult(remaining_scenario)
                        skip_result.skip("Previous scenario failed")
                        self.results[remaining_scenario] = skip_result
                    break
                
                # 场景间等待
                if i < len(execution_order) - 1 and self.wait_between_scenarios > 0:
                    self.logger.info(f"Waiting {self.wait_between_scenarios} seconds before next scenario\n\n")
                    time.sleep(self.wait_between_scenarios)
            
            self.logger.info("All scenarios execution completed")
            
            # 执行完成回调
            if self.on_all_complete:
                self.on_all_complete(self.results)
            
        finally:
            self.is_running = False
            self.current_scenario = None

            # 🧹 最终资源清理：确保所有资源都被彻底清理
            self.logger.info("Performing final resource cleanup after all scenarios")
            try:
                self.resource_manager.force_clean_environment()
                cleanup_stats = self.resource_manager.get_cleanup_stats()
                self.logger.info(f"📊 Final cleanup stats: {cleanup_stats}")
            except Exception as e:
                self.logger.error(f"Final resource cleanup failed: {e}")
        
        return self.results
    
    def run_scenario(self, scenario_name: str) -> ScenarioResult:
        """执行单个场景"""
        scenario = self.scenario_manager.get_scenario(scenario_name)
        if not scenario:
            result = ScenarioResult(scenario_name)
            result.fail(f"Scenario not found: {scenario_name}")
            self.results[scenario_name] = result
            return result
        
        if not scenario.enabled:
            result = ScenarioResult(scenario_name)
            result.skip("Scenario is disabled")
            self.results[scenario_name] = result
            return result
        
        # 验证场景
        if not scenario.is_valid:
            result = ScenarioResult(scenario_name)
            result.fail("Scenario validation failed: missing required files")
            self.results[scenario_name] = result
            return result
        
        self.current_scenario = scenario_name
        result = ScenarioResult(scenario_name)
        self.results[scenario_name] = result
        
        # 设置场景专用日志记录器
        scenario_logger = setup_scenario_logger(scenario_name)
        
        # 开始执行回调
        if self.on_scenario_start:
            self.on_scenario_start(scenario_name)
        
        # 执行场景（支持重试）
        for attempt in range(self.retry_count + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"Retrying scenario {scenario_name} (attempt {attempt + 1})")
                    scenario_logger.info(f"Retry attempt {attempt + 1}")
                
                result.start()
                scenario_logger.info(f"Starting scenario execution: {scenario.description}")
                
                # 执行场景步骤
                self._execute_scenario_steps(scenario, result, scenario_logger)
                
                result.complete()
                scenario_logger.info("Scenario completed successfully")
                self.logger.info(f"Scenario {scenario_name} completed successfully")
                break
                
            except Exception as e:
                error_msg = f"Scenario execution failed: {str(e)}"
                scenario_logger.error(error_msg)
                
                if attempt < self.retry_count:
                    self.logger.warning(f"Scenario {scenario_name} failed, will retry: {error_msg}")
                    time.sleep(30)  # 重试前等待
                    continue
                else:
                    result.fail(error_msg)
                    self.logger.error(f"Scenario {scenario_name} failed after {self.retry_count + 1} attempts")
                    break
        
        # 完成回调
        if self.on_scenario_complete:
            self.on_scenario_complete(scenario_name, result)
        
        return result
    
    def _execute_scenario_steps(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """执行场景步骤（分布式部署流程）"""
        scenario_path = Path(scenario.directory)

        # 验证必须配置
        if not scenario.metadata or not scenario.metadata.services:
            raise RuntimeError(f"Scenario {scenario.name} missing required services configuration")

        logger.info("Using distributed deployment configuration")

        logger.info("\n\nStep 1: Validating deployment configuration")
        self._validate_deployment_config(scenario, result, logger)

        logger.info("\n\nStep 2: Building service dependency graph")
        self._build_service_dependencies(scenario, result, logger)

        logger.info("\n\nStep 3: Deploying services in dependency order")
        self._deploy_services_with_dependencies(scenario, result, logger)

        logger.info("\n\nStep 4: Running comprehensive health checks")
        self._run_comprehensive_health_checks(scenario, result, logger)

        logger.info("\n\nStep 5: Executing test scripts")
        self._execute_test_scripts(scenario, result, logger)

        logger.info("\n\nStep 6: Collecting distributed results")
        self._collect_distributed_results(scenario, result, logger)

        logger.info("\n\nStep 7: Stopping distributed services")
        self._stop_distributed_services(scenario, result, logger)

        logger.info("\n\nStep 8: Cleaning up distributed environment")
        self._cleanup_distributed_environment(scenario, result, logger)
    
    def _prepare_scenario_environment(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """准备场景环境"""
        logger.info("Preparing scenario environment")
        
        # 检查节点连接性
        nodes = self.node_manager.get_nodes(enabled_only=True)
        if not nodes:
            raise RuntimeError("No enabled nodes available")
        
        node_names = [node.name for node in nodes]
        connectivity = self.node_manager.test_connectivity(node_names, timeout=30)
        
        failed_nodes = [name for name, connected in connectivity.items() if not connected]
        if failed_nodes:
            raise RuntimeError(f"Failed to connect to nodes: {failed_nodes}")
        
        logger.info(f"All {len(nodes)} nodes are connected and ready")
        
        # 上传场景配置文件到节点（如果需要）
        config_file = scenario.get_test_config_path()
        if config_file:
            self.node_manager.upload_file(
                config_file,
                f"/tmp/test_config_{scenario.name}.json",
                node_names
            )
            logger.info("Uploaded test configuration to nodes")
    
    def _collect_test_results(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """收集测试结果"""
        logger.info("Collecting test results")
        
        # 从各个节点收集结果
        nodes = self.node_manager.get_nodes(enabled_only=True)
        collected_files = []
        
        for node in nodes:
            try:
                # 创建结果目录
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                local_result_dir = f"results/{timestamp}/{scenario.name}"
                
                # 下载结果文件
                download_results = self.node_manager.download_files(
                    node.results_path,
                    local_result_dir,
                    [node.name]
                )
                
                if download_results.get(node.name, False):
                    collected_files.append(f"{local_result_dir}/{node.name}")
                    logger.info(f"Collected results from {node.name}")
                else:
                    logger.warning(f"Failed to collect results from {node.name}")
                    
            except Exception as e:
                logger.error(f"Error collecting results from {node.name}: {e}")
        
        result.artifacts = collected_files
        logger.info(f"Collected results from {len(collected_files)} locations")
    
    def _stop_inference_services(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """停止推理服务"""
        logger.info("Stopping inference services")
        
        compose_file = scenario.get_docker_compose_path()
        nodes = self.node_manager.get_nodes(enabled_only=True)
        
        for node in nodes:
            logger.info(f"Stopping services on node {node.name}")
            
            # 使用适配器构建并执行停止命令
            compose_cmd = self.node_manager.build_compose_command(
                node_name=node.name,
                command_type="down",
                file=compose_file
            )

            full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"
            results = self.node_manager.execute_command(
                full_cmd, [node.name], timeout=120
            )
            
            node_result = results.get(node.name)
            if node_result and node_result[0] == 0:
                logger.info(f"Services stopped successfully on {node.name}")
            else:
                error_msg = node_result[2] if node_result else "Unknown error"
                logger.warning(f"Failed to stop services on {node.name}: {error_msg}")
                # 不抛出异常，允许继续清理
    
    def _cleanup_scenario_environment(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """清理场景环境"""
        logger.info("Cleaning up scenario environment")
        
        # 清理临时文件，可以继续添加清理命令
        cleanup_commands = [
            f"rm -f /tmp/test_config_{scenario.name}.json"
        ]
        
        nodes = self.node_manager.get_nodes(enabled_only=True)
        node_names = [node.name for node in nodes]
        
        for cmd in cleanup_commands:
            try:
                results = self.node_manager.execute_command(cmd, node_names, timeout=60)
                success_count = sum(1 for r in results.values() if r[0] == 0)
                logger.info(f"Cleanup command '{cmd}' executed on {success_count}/{len(nodes)} nodes")
            except Exception as e:
                logger.warning(f"Cleanup command failed: {e}")
        
        logger.info("Cleanup completed")
    
    def cancel(self):
        """取消执行"""
        self.cancelled = True
        self.logger.info("Cancelling scenario execution")
        
        if self.current_scenario:
            current_result = self.results.get(self.current_scenario)
            if current_result and current_result.status == ScenarioStatus.RUNNING:
                current_result.cancel()
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """获取执行摘要"""
        if not self.results:
            return {'total': 0, 'status': 'no_results'}
        
        total = len(self.results)
        completed = sum(1 for r in self.results.values() if r.status == ScenarioStatus.COMPLETED)
        failed = sum(1 for r in self.results.values() if r.status == ScenarioStatus.FAILED)
        skipped = sum(1 for r in self.results.values() if r.status == ScenarioStatus.SKIPPED)
        cancelled = sum(1 for r in self.results.values() if r.status == ScenarioStatus.CANCELLED)
        
        total_duration = sum(
            (r.duration.total_seconds() if r.duration else 0)
            for r in self.results.values()
        )
        
        return {
            'total': total,
            'completed': completed,
            'failed': failed,
            'skipped': skipped,
            'cancelled': cancelled,
            'success_rate': (completed / total * 100) if total > 0 else 0,
            'total_duration_seconds': total_duration,
            'is_running': self.is_running,
            'current_scenario': self.current_scenario,
            'results': {name: result.to_dict() for name, result in self.results.items()}
        }
    
    # 新增分布式部署方法
    
    def _validate_deployment_config(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """验证部署配置"""
        scenario_path = Path(scenario.directory)
        services = scenario.metadata.services
        
        logger.info(f"Validating deployment config for {len(services)} services")
        
        # 使用DockerComposeManager验证配置
        validation_results = self.docker_manager.validate_service_deployment(scenario_path, services)
        
        if validation_results['invalid_services'] or validation_results['missing_files'] or validation_results['missing_nodes']:
            error_details = []
            error_details.extend(validation_results['invalid_services'])
            error_details.extend(validation_results['missing_files'])
            error_details.extend(validation_results['missing_nodes'])
            raise RuntimeError(f"Deployment validation failed: {'; '.join(error_details)}")
        
        logger.info(f"Deployment validation passed for {len(validation_results['valid_services'])} services")
        
    def _build_service_dependencies(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """构建服务依赖图"""
        services = scenario.metadata.services
        
        logger.info("Building service dependency graph")
        
        if not self.dependency_resolver.build_dependency_graph(services):
            raise RuntimeError("Failed to build service dependency graph (circular dependency detected)")
        
        # 计算部署批次
        batches = self.dependency_resolver.get_deployment_batches()
        logger.info(f"Calculated {len(batches)} deployment batches")
        
        for i, batch in enumerate(batches, 1):
            service_names = [node.service.name for node in batch]
            logger.info(f"Batch {i}: {service_names}")
    
    def _deploy_services_with_dependencies(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """按依赖顺序并发部署服务"""
        scenario_path = Path(scenario.directory)

        # 获取并发配置
        execution_config = getattr(self.scenario_manager, 'execution_config', {})
        concurrent_config = execution_config.get('concurrent_deployment', {})
        max_concurrent_services = concurrent_config.get('max_concurrent_services', 5)

        # 配置重试策略
        retry_config = execution_config.get('retry_strategy', {})
        retry_strategy = ConcurrentRetryStrategy(
            max_retries=retry_config.get('service_level_retries', 2),
            retry_delay=retry_config.get('retry_delay', 30),
            retry_only_failed=retry_config.get('retry_only_failed', True)
        )

        # 获取超时配置
        deployment_timeout = concurrent_config.get('deployment_timeout', 600)
        health_check_timeout = concurrent_config.get('health_check_timeout', 300)

        # 获取部署批次
        batches = self.dependency_resolver.get_deployment_batches()

        # 创建并发部署日志器
        deployment_logger = ConcurrentDeploymentLogger(logger)

        # 确保使用并发部署
        max_concurrent_services = max(2, max_concurrent_services)
        logger.info(f"Using concurrent deployment with max_concurrent_services={max_concurrent_services}")

        # 使用并发部署器
        try:
            deployment_results = asyncio.run(
                self.concurrent_deployer.deploy_services_concurrent(
                    batches, scenario_path, deployment_logger,
                    max_concurrent_services, retry_strategy, deployment_timeout
                )
            )

            # 转换结果格式
            service_results = {
                name: result.is_success
                for name, result in deployment_results.items()
            }

            # 记录部署结果到result.metrics
            result.metrics['service_deployment'] = service_results
            result.metrics['deployment_summary'] = self.dependency_resolver.get_deployment_summary()
            result.metrics['concurrent_deployment_details'] = {
                name: {
                    'status': result.status.value,
                    'duration': result.duration,
                    'retry_count': result.retry_count,
                    'nodes_deployed': result.nodes_deployed,
                    'error_message': result.error_message
                }
                for name, result in deployment_results.items()
            }

        except RuntimeError as e:
            # 并发部署失败时的错误处理
            logger.error(f"Concurrent deployment failed: {e}")
            raise

    def _run_comprehensive_health_checks(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """运行全面健康检查（支持并发）"""
        services = scenario.metadata.services

        # 获取并发配置
        execution_config = getattr(self.scenario_manager, 'execution_config', {})
        concurrent_config = execution_config.get('concurrent_deployment', {})
        max_concurrent_health_checks = concurrent_config.get('max_concurrent_health_checks', 10)
        health_check_timeout = concurrent_config.get('health_check_timeout', 300)

        logger.info(f"Running health checks for {len(services)} services (max_concurrent={max_concurrent_health_checks}, timeout={health_check_timeout}s)")

        # 运行并发健康检查
        health_results = self.health_checker.run_batch_health_checks(
            services,
            parallel=True,
            max_workers=max_concurrent_health_checks,
            timeout=health_check_timeout
        )

        # 聚合结果
        health_summary = self.health_checker.aggregate_health_results(health_results)

        # 记录到结果中
        result.metrics['health_checks'] = health_summary

        if not health_summary['overall_healthy']:
            failed_checks = health_summary.get('failed_checks', [])
            logger.error(f"Health checks failed: {len(failed_checks)} failures")

            for failed_check in failed_checks[:5]:  # 显示前5个失败
                logger.error(f"  - {failed_check['service']}@{failed_check['node']}: {failed_check['message']}")

            raise RuntimeError(f"Health checks failed for {health_summary['unhealthy_services']} services")

        logger.info(f"All health checks passed: {health_summary['healthy_services']}/{health_summary['total_services']} services healthy")
    
    def _execute_test_scripts(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """执行测试脚本"""
        test_config = scenario.metadata.test_execution
        
        if not test_config.wait_for_all_services:
            logger.info("Skipping service readiness wait as configured")
        else:
            logger.info("All services are healthy, proceeding with test execution")
        
        logger.info(f"Executing test script: {test_config.script} on {test_config.node}")
        
        # 执行测试
        test_result = self.test_executor.execute_test(scenario, test_config)
        
        # 记录测试结果
        result.metrics['test_execution'] = {
            'success': test_result.success,
            'exit_code': test_result.exit_code,
            'duration': test_result.duration,
            'metrics': test_result.metrics,
            'artifacts_count': len(test_result.artifacts)
        }
        
        # 添加artifacts到结果中
        result.artifacts.extend(test_result.artifacts)
        
        if not test_result.success:
            logger.error(f"Test execution failed: {test_result.error_message}")
            logger.error(f"Exit code: {test_result.exit_code}")
            if test_result.stderr:
                logger.error(f"Stderr: {test_result.stderr[:500]}")
            raise RuntimeError(f"Test script execution failed: {test_result.error_message}")
        
        logger.info(f"Test script completed successfully in {test_result.duration:.2f}s")
        if test_result.metrics:
            logger.info(f"Test metrics: {test_result.metrics}")
    
    def _collect_distributed_results(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """收集分布式结果"""
        services = scenario.metadata.services
        test_config = scenario.metadata.test_execution
        
        logger.info("Collecting distributed results")
        
        collected_files = []
        
        # 从测试执行中已经收集了artifacts，这里收集额外的结果
        if test_config.result_paths:
            logger.info(f"Collecting additional results from {len(test_config.result_paths)} paths")
            
            # 结果已在test_executor中收集，这里只需要记录
            logger.info(f"Total artifacts collected: {len(result.artifacts)}")
        
        # 从各个服务节点收集日志（可选）
        for service in services:
            for node_name in service.nodes:
                try:
                    # 收集容器日志
                    log_cmd = f"docker logs {service.name} --tail 100"
                    results = self.node_manager.execute_command(log_cmd, [node_name])
                    
                    if results.get(node_name) and results[node_name][0] == 0:
                        log_content = results[node_name][1]
                        
                        # 保存日志到本地文件
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        log_file = f"results/{timestamp}/{service.name}_{node_name}.log"
                        
                        os.makedirs(os.path.dirname(log_file), exist_ok=True)
                        with open(log_file, 'w') as f:
                            f.write(log_content)
                        
                        collected_files.append(log_file)
                        
                except Exception as e:
                    logger.warning(f"Failed to collect logs from {service.name}@{node_name}: {e}")
        
        result.artifacts.extend(collected_files)
        logger.info(f"Collected results from {len(collected_files)} additional sources")
    
    def _stop_distributed_services(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """停止分布式服务（支持并发）"""
        scenario_path = Path(scenario.directory)
        services = scenario.metadata.services

        # 获取并发配置
        execution_config = getattr(self.scenario_manager, 'execution_config', {})
        concurrent_config = execution_config.get('concurrent_deployment', {})
        max_concurrent_services = concurrent_config.get('max_concurrent_services', 5)
        deployment_timeout = concurrent_config.get('deployment_timeout', 600)

        logger.info(f"Stopping {len(services)} distributed services (max_concurrent={max_concurrent_services})")

        # 按反向依赖顺序停止服务（先停止依赖方）
        batches = list(reversed(self.dependency_resolver.get_deployment_batches()))

        # 确保使用并发停止
        max_concurrent_services = max(2, max_concurrent_services)
        logger.info(f"Using concurrent stop with max_concurrent_services={max_concurrent_services}")

        # 使用并发停止
        try:
            asyncio.run(self._stop_services_concurrent(batches, scenario_path, max_concurrent_services, deployment_timeout, logger))
        except asyncio.TimeoutError:
            logger.error(f"Concurrent service stop timeout after {deployment_timeout}s")
            # 继续执行，不抛出异常以免影响清理
        except Exception as e:
            logger.error(f"Error during concurrent service stop: {e}")
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            # 继续执行，不抛出异常以免影响清理

        logger.info("Distributed services stop completed")

    async def _stop_services_concurrent(self, batches, scenario_path, max_concurrent, deployment_timeout, logger):
        """并发停止服务批次"""
        semaphore = asyncio.Semaphore(max_concurrent)

        for i, batch in enumerate(batches, 1):
            logger.info(f"Stopping batch {i}/{len(batches)}")

            async def stop_single_service(service_node):
                async with semaphore:
                    service = service_node.service
                    loop = asyncio.get_event_loop()

                    for node_name in service.nodes:
                        try:
                            success = await loop.run_in_executor(
                                None, self.docker_manager.stop_service,
                                scenario_path, service, node_name, deployment_timeout
                            )
                            if success:
                                logger.info(f"Stopped {service.name} on {node_name}")
                            else:
                                logger.warning(f"Failed to stop {service.name} on {node_name}")
                        except Exception as e:
                            logger.warning(f"Error stopping {service.name} on {node_name}: {e}")

            # 批次内并发停止
            tasks = [stop_single_service(service_node) for service_node in batch]
            await asyncio.gather(*tasks, return_exceptions=True)

            logger.info(f"Batch {i} stop completed")

    def _cleanup_distributed_environment(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """清理分布式环境"""
        services = scenario.metadata.services
        
        logger.info("Cleaning up distributed environment")
        
        # 获取所有涉及的节点
        all_nodes = set()
        for service in services:
            all_nodes.update(service.nodes)
        
        node_names = list(all_nodes)
        
        # 清理失败的服务
        self.dependency_resolver.cleanup_failed_services(Path(scenario.directory))
        
        # 通用清理
        self.docker_manager.cleanup_node_compose_files(node_names)
        
        logger.info(f"Cleanup completed for {len(node_names)} nodes")
    
