"""
场景执行器
负责按顺序执行测试场景，管理场景生命周期
"""

import os
import time
import logging
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
                    self.logger.info(f"Waiting {self.wait_between_scenarios} seconds before next scenario")
                    time.sleep(self.wait_between_scenarios)
            
            self.logger.info("All scenarios execution completed")
            
            # 执行完成回调
            if self.on_all_complete:
                self.on_all_complete(self.results)
            
        finally:
            self.is_running = False
            self.current_scenario = None
        
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
        """执行场景步骤（新的分布式部署流程）"""
        scenario_path = Path(scenario.directory)
        
        # 检查是否有部署配置
        if scenario.metadata and scenario.metadata.deployment.services:
            logger.info("\n\nUsing advanced distributed deployment configuration")
            
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
        else:
            logger.info("Using legacy deployment mode")
            # 保持向后兼容的简单模式
            self._execute_legacy_scenario_steps(scenario, result, logger)
    
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
    
    def _start_inference_services(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """启动推理服务"""
        logger.info("Starting inference services")
        
        compose_file = scenario.get_docker_compose_path()
        if not compose_file or not Path(compose_file).exists():
            raise RuntimeError(f"Docker Compose file not found: {compose_file}")
        
        # 在每个节点上启动服务
        nodes = self.node_manager.get_nodes(enabled_only=True)
        
        for node in nodes:
            logger.info(f"Starting services on node {node.name}")
            
            # 切换到工作目录并启动服务
            commands = [
                f"cd {node.docker_compose_path}",
                f"docker compose -f {Path(compose_file).name} up -d"
            ]
            
            command = " && ".join(commands)
            results = self.node_manager.execute_command(command, [node.name])
            
            node_result = results.get(node.name)
            if not node_result or node_result[0] != 0:
                error_msg = node_result[2] if node_result else "Unknown error"
                raise RuntimeError(f"Failed to start services on {node.name}: {error_msg}")
            
            logger.info(f"Services started successfully on {node.name}")
    
    def _wait_for_services_ready(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """等待服务就绪"""
        logger.info("Waiting for services to be ready")
        
        max_wait_time = 300  # 5分钟
        check_interval = 10   # 10秒
        
        start_time = time.time()
        nodes = self.node_manager.get_nodes(enabled_only=True)
        
        while time.time() - start_time < max_wait_time:
            all_ready = True
            
            for node in nodes:
                # 检查Docker服务状态
                check_cmd = "docker ps --format '{{.Names}}\\t{{.Status}}' | grep -E '(Up|healthy)'"
                results = self.node_manager.execute_command(check_cmd, [node.name])
                
                node_result = results.get(node.name)
                if not node_result or node_result[0] != 0:
                    all_ready = False
                    break
                
                # 可以添加更多健康检查
                # 例如：检查API端点是否响应
            
            if all_ready:
                logger.info("All services are ready")
                return
            
            logger.info(f"Services not ready yet, waiting {check_interval} seconds...")
            time.sleep(check_interval)
        
        raise RuntimeError("Services did not become ready within timeout")
    
    def _run_benchmark_tests(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """运行基准测试"""
        logger.info("Running benchmark tests")
        
        # 这里会由BenchmarkRunner实现具体逻辑
        # 暂时用占位符实现
        test_script = scenario.get_run_script_path()
        if test_script and Path(test_script).exists():
            logger.info(f"Found test script: {test_script}")
            # 实际执行将在BenchmarkRunner中实现
        
        # 模拟测试执行时间
        estimated_time = 0
        if scenario.metadata and scenario.metadata.estimated_duration:
            estimated_time = scenario.metadata.estimated_duration
            logger.info(f"Estimated test duration: {estimated_time} seconds")
        
        # 这里应该调用BenchmarkRunner来执行实际测试
        logger.info("Benchmark test execution placeholder")
        
        # 记录测试指标
        result.metrics = {
            'test_start_time': datetime.now().isoformat(),
            'estimated_duration': estimated_time,
            'test_type': 'benchmark'
        }
    
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
            
            commands = [
                f"cd {node.docker_compose_path}",
                f"docker compose -f {Path(compose_file).name} down"
            ]
            
            command = " && ".join(commands)
            results = self.node_manager.execute_command(
                command, [node.name], timeout=120
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
        
        # 清理临时文件
        cleanup_commands = [
            f"rm -f /tmp/test_config_{scenario.name}.json",
            "docker system prune -f"
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
        services = scenario.metadata.deployment.services
        
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
        services = scenario.metadata.deployment.services
        
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
        """按依赖顺序部署服务"""
        scenario_path = Path(scenario.directory)
        batches = self.dependency_resolver.get_deployment_batches()
        
        logger.info(f"Deploying {len(batches)} batches of services")
        
        deployment_results = {}
        
        for i, batch in enumerate(batches, 1):
            logger.info(f"Deploying batch {i}/{len(batches)}")
            
            batch_results = self.dependency_resolver.deploy_batch(batch, scenario_path)
            deployment_results.update(batch_results)
            
            # 检查批次是否有失败
            failed_services = [name for name, success in batch_results.items() if not success]
            if failed_services:
                logger.error(f"Batch {i} deployment failed for services: {failed_services}")
                raise RuntimeError(f"Service deployment failed: {failed_services}")
            
            logger.info(f"Batch {i} deployed successfully")
        
        # 记录部署结果到result.metrics
        result.metrics['service_deployment'] = deployment_results
        result.metrics['deployment_summary'] = self.dependency_resolver.get_deployment_summary()
    
    def _run_comprehensive_health_checks(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """运行全面健康检查"""
        services = scenario.metadata.deployment.services
        
        logger.info(f"Running health checks for {len(services)} services")
        
        # 运行健康检查
        health_results = self.health_checker.run_batch_health_checks(services, parallel=True)
        
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
        test_config = scenario.metadata.deployment.test_execution
        
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
        services = scenario.metadata.deployment.services
        test_config = scenario.metadata.deployment.test_execution
        
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
        """停止分布式服务"""
        scenario_path = Path(scenario.directory)
        services = scenario.metadata.deployment.services
        
        logger.info(f"Stopping {len(services)} distributed services")
        
        # 按反向依赖顺序停止服务（先停止依赖方）
        batches = self.dependency_resolver.get_deployment_batches()
        
        for i, batch in enumerate(reversed(batches), 1):
            logger.info(f"Stopping batch {i}/{len(batches)}")
            
            for service_node in batch:
                service = service_node.service
                
                for node_name in service.nodes:
                    try:
                        success = self.docker_manager.stop_service(
                            scenario_path, service, node_name, timeout=120
                        )
                        if success:
                            logger.info(f"Stopped {service.name} on {node_name}")
                        else:
                            logger.warning(f"Failed to stop {service.name} on {node_name}")
                    except Exception as e:
                        logger.warning(f"Error stopping {service.name} on {node_name}: {e}")
        
        logger.info("Distributed services stop completed")
    
    def _cleanup_distributed_environment(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """清理分布式环境"""
        services = scenario.metadata.deployment.services
        
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
    
    def _execute_legacy_scenario_steps(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """执行传统场景步骤（向后兼容）"""
        logger.info("Step 1: Preparing scenario environment")
        self._prepare_scenario_environment(scenario, result, logger)
        
        logger.info("Step 2: Starting inference services")
        self._start_inference_services(scenario, result, logger)
        
        logger.info("Step 3: Waiting for services to be ready")
        self._wait_for_services_ready(scenario, result, logger)
        
        logger.info("Step 4: Running benchmark tests")
        self._run_benchmark_tests(scenario, result, logger)
        
        logger.info("Step 5: Collecting test results")
        self._collect_test_results(scenario, result, logger)
        
        logger.info("Step 6: Stopping inference services")
        self._stop_inference_services(scenario, result, logger)
        
        logger.info("Step 7: Cleaning up scenario environment")
        self._cleanup_scenario_environment(scenario, result, logger)