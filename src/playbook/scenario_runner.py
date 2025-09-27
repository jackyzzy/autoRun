"""
场景执行器
负责按顺序执行测试场景，管理场景生命周期
"""

import time
import logging
import asyncio
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
from datetime import datetime, timedelta
from enum import Enum

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
from ..utils.logger import setup_scenario_logger

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
    
    def run_all_scenarios(self, base_result_dir: Optional[Path] = None) -> Dict[str, ScenarioResult]:
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
                result = self.run_scenario(scenario_name, base_result_dir=base_result_dir)

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
    
    def run_scenario(self, scenario_name: str, base_result_dir: Optional[Path] = None) -> ScenarioResult:
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
                self._execute_scenario_steps(scenario, result, scenario_logger, base_result_dir)
                
                result.complete()
                scenario_logger.info("Scenario completed successfully")
                self.logger.info(f"Scenario {scenario_name} completed successfully")
                break
                
            except Exception as e:
                error_msg = f"Scenario execution failed: {str(e)}"
                scenario_logger.error(error_msg)
                
                if attempt < self.retry_count:
                    self.logger.warning(f"Scenario {scenario_name} failed, will retry: {error_msg}")
                    
                    # 重试前强制清理状态
                    self._cleanup_before_retry(scenario_name, attempt + 1, scenario_logger)
                    
                    continue
                else:
                    result.fail(error_msg)
                    self.logger.error(f"Scenario {scenario_name} failed after {self.retry_count + 1} attempts")
                    break
        
        # 完成回调
        if self.on_scenario_complete:
            self.on_scenario_complete(scenario_name, result)
        
        return result
    
    def _cleanup_before_retry(self, _scenario_name: str, attempt: int, logger: logging.Logger):
        """重试前清理状态和连接"""
        logger.info("Cleaning up before retry...")
        
        try:
            # 1. 强制清理SSH连接池
            logger.info("Clearing SSH connections...")
            from ..utils.ssh_client import ssh_pool
            ssh_pool.close_all()
            
            # 2. 清理资源管理器状态
            if hasattr(self, 'resource_manager'):
                logger.info("Force cleaning resources...")
                self.resource_manager.force_clean_environment()
            
            # 3. 清理节点管理器连通性缓存
            if hasattr(self, 'node_manager'):
                logger.info("Clearing node connectivity cache...")
                self.node_manager.clear_connectivity_cache()
                
                # 4. 重新验证节点连通性（不使用缓存）
                logger.info("Re-verifying node connectivity...")
                connectivity = self.node_manager.test_connectivity(use_cache=False)
                connected_count = sum(connectivity.values()) if isinstance(connectivity, dict) else 0
                total_count = len(connectivity) if isinstance(connectivity, dict) else 0
                
                if connected_count < total_count:
                    logger.warning(f"Node connectivity issues detected: {connected_count}/{total_count} nodes connected")
                    for node_name, status in connectivity.items():
                        if not status:
                            logger.warning(f"Node {node_name} is not connected")
                else:
                    logger.info(f"All {total_count} nodes are connected")
            
            # 5. 添加指数退避重试延迟
            retry_delay = min(30 * (2 ** (attempt - 1)), 120)  # 30s, 60s, 120s (最大2分钟)
            logger.info(f"Waiting {retry_delay}s before retry attempt {attempt}...")
            time.sleep(retry_delay)
            
            logger.info("Cleanup completed, ready for retry")
            
        except Exception as cleanup_error:
            logger.error(f"Cleanup before retry failed: {cleanup_error}")
            logger.warning("Proceeding with retry despite cleanup failure...")
            # 即使清理失败也要继续重试，但至少等待基本延迟
            time.sleep(30)
    
    def _execute_scenario_steps(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger, base_result_dir: Optional[Path] = None):
        """执行场景步骤（分布式部署流程）"""
        # 验证必须配置
        if not scenario.metadata or not scenario.metadata.services:
            raise RuntimeError(f"Scenario {scenario.name} missing required services configuration")

        logger.info("Using distributed deployment configuration")

        logger.info("\n\nStep 0: Preparing scenario environment")
        self._prepare_scenario_environment(scenario, result, logger)

        logger.info("\n\nStep 0.5: Validating deployment prerequisites")
        self._validate_deployment_prerequisites(scenario, result, logger)

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
        self._collect_distributed_results(scenario, result, logger, base_result_dir)

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
        
        # 上传环境变量文件到节点（如果存在）
        env_file_path = scenario.get_env_file_path()
        if env_file_path:
            logger.info("Uploading environment variables file to nodes")
            upload_results = self.node_manager.upload_scenario_env_file(
                env_file_path, scenario.name, node_names
            )
            
            # 检查上传结果
            failed_uploads = [node for node, success in upload_results.items() if not success]
            if failed_uploads:
                raise RuntimeError(f"Failed to upload environment file to nodes: {failed_uploads}")
            
            logger.info(f"Successfully uploaded .env file to {len(upload_results)} nodes")
            
            # 验证环境变量文件
            verification_results = self.node_manager.verify_env_file_on_nodes(scenario.name, node_names)
            failed_verifications = [node for node, result in verification_results.items() 
                                  if not (result['exists'] and result['readable'])]
            if failed_verifications:
                logger.warning(f"Environment file verification failed on nodes: {failed_verifications}")
        else:
            logger.info("No environment variables file found for this scenario")
        
        # 上传场景配置文件到节点（如果需要）
        config_file = scenario.get_test_config_path()
        if config_file:
            self.node_manager.upload_file(
                config_file,
                f"/tmp/test_config_{scenario.name}.json",
                node_names
            )
            logger.info("Uploaded test configuration to nodes")
    
    def _validate_deployment_prerequisites(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """验证部署前置条件 - 增强版验证"""
        logger.info("Validating deployment prerequisites")
        
        nodes = self.node_manager.get_nodes(enabled_only=True)
        prerequisite_failures = []
        
        for node in nodes:
            logger.info(f"🔍 Validating prerequisites on node {node.name}...")
            
            # 1. 验证工作目录路径一致性
            if node.work_dir != node.docker_compose_path:
                logger.warning(f"⚠️  Path inconsistency on {node.name}: work_dir({node.work_dir}) != docker_compose_path({node.docker_compose_path})")
            
            # 2. 验证.env文件（如果场景需要）
            env_file_path = scenario.get_env_file_path()
            if env_file_path:
                env_path = f"{node.docker_compose_path}/.env"
                verify_env_cmd = f"test -f {env_path} && test -r {env_path} && wc -c {env_path} | cut -d' ' -f1"
                
                try:
                    results = self.node_manager.execute_command(verify_env_cmd, [node.name], timeout=10)
                    node_result = results.get(node.name)
                    
                    if node_result and node_result[0] == 0:
                        remote_size = int(node_result[1].strip())
                        local_size = Path(env_file_path).stat().st_size
                        
                        if remote_size == local_size:
                            logger.info(f"✅ .env file validation passed on {node.name} ({remote_size} bytes)")
                        else:
                            error_msg = f".env file size mismatch on {node.name}: local={local_size}, remote={remote_size}"
                            logger.error(f"❌ {error_msg}")
                            prerequisite_failures.append(f"{node.name}: {error_msg}")
                    else:
                        error_msg = f".env file not found or not readable on {node.name}: {env_path}"
                        logger.error(f"❌ {error_msg}")
                        prerequisite_failures.append(f"{node.name}: {error_msg}")
                        
                except Exception as e:
                    error_msg = f".env file validation failed on {node.name}: {e}"
                    logger.error(f"❌ {error_msg}")
                    prerequisite_failures.append(f"{node.name}: {error_msg}")
            else:
                logger.info(f"ℹ️  No .env file required for scenario {scenario.name}")
            
            # 3. 验证目录权限
            perm_check_cmd = f"test -d {node.docker_compose_path} && test -w {node.docker_compose_path}"
            try:
                results = self.node_manager.execute_command(perm_check_cmd, [node.name], timeout=10)
                node_result = results.get(node.name)
                
                if node_result and node_result[0] == 0:
                    logger.info(f"✅ Directory permissions validated on {node.name}: {node.docker_compose_path}")
                else:
                    error_msg = f"Directory permission check failed on {node.name}: {node.docker_compose_path}"
                    logger.error(f"❌ {error_msg}")
                    prerequisite_failures.append(f"{node.name}: {error_msg}")
                    
            except Exception as e:
                error_msg = f"Directory permission validation failed on {node.name}: {e}"
                logger.error(f"❌ {error_msg}")
                prerequisite_failures.append(f"{node.name}: {error_msg}")
            
            # 4. 验证Docker服务可用性
            docker_check_cmd = "docker version --format '{{.Server.Version}}' 2>/dev/null || echo 'FAIL'"
            try:
                results = self.node_manager.execute_command(docker_check_cmd, [node.name], timeout=15)
                node_result = results.get(node.name)
                
                if node_result and node_result[0] == 0 and "FAIL" not in node_result[1]:
                    docker_version = node_result[1].strip()
                    logger.info(f"✅ Docker service validated on {node.name}: v{docker_version}")
                else:
                    error_msg = f"Docker service not available on {node.name}"
                    logger.error(f"❌ {error_msg}")
                    prerequisite_failures.append(f"{node.name}: {error_msg}")
                    
            except Exception as e:
                error_msg = f"Docker service validation failed on {node.name}: {e}"
                logger.warning(f"⚠️  {error_msg}")  # Warning instead of error since Docker might be temporarily unavailable
        
        # 报告验证结果
        if prerequisite_failures:
            failure_summary = "; ".join(prerequisite_failures)
            logger.error(f"❌ Prerequisite validation failed: {failure_summary}")
            raise RuntimeError(f"Deployment prerequisite validation failed: {failure_summary}")
        else:
            logger.info(f"✅ All deployment prerequisites validated successfully on {len(nodes)} nodes")
    
    
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
                file=compose_file,
                env_file=".env"  # 使用节点上的.env文件
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

    def _get_collection_mode(self, scenario: Scenario):
        """根据配置获取收集模式"""
        from .result.result_models import CollectionMode

        # 1. 优先使用场景级别的配置
        if scenario.metadata and scenario.metadata.test_execution:
            config_mode = scenario.metadata.test_execution.collection_mode
            if config_mode:
                try:
                    return CollectionMode(config_mode.lower())
                except ValueError:
                    self.logger.warning(f"Invalid collection mode '{config_mode}', using default")

        # 2. 检查场景管理器的全局配置
        if hasattr(self.scenario_manager, 'execution_config'):
            global_mode = self.scenario_manager.execution_config.get('default_collection_mode')
            if global_mode:
                try:
                    return CollectionMode(global_mode.lower())
                except ValueError:
                    self.logger.warning(f"Invalid global collection mode '{global_mode}', using default")

        # 3. 根据场景复杂度自动选择模式
        if scenario.metadata and scenario.metadata.services:
            service_count = len(scenario.metadata.services)
            if service_count >= 3:
                # 复杂场景使用 COMPREHENSIVE 模式
                return CollectionMode.COMPREHENSIVE
            elif service_count >= 1:
                # 中等复杂度使用 STANDARD 模式
                return CollectionMode.STANDARD

        # 4. 默认使用 STANDARD 模式
        return CollectionMode.STANDARD

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
        
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
    
    def _collect_distributed_results(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger, base_result_dir: Optional[Path] = None):
        """收集分布式结果 - 使用新的ResultCollector"""
        logger.info("Collecting distributed results using ResultCollector")

        try:
            # 从result.metrics中获取test_execution_result
            test_execution_result = None
            if 'test_execution' in result.metrics:
                # 创建TestExecutionResult对象
                from .test_script_executor import TestExecutionResult
                test_execution_result = TestExecutionResult(
                    success=result.metrics['test_execution'].get('success', False),
                    exit_code=result.metrics['test_execution'].get('exit_code', -1),
                    duration=result.metrics['test_execution'].get('duration', 0.0),
                    artifacts=result.artifacts.copy(),  # 使用已收集的artifacts
                    metrics=result.metrics['test_execution'].get('metrics', {})
                )

            # 使用ResultCollector收集结果
            from .result.result_collector import ResultCollector
            result_collector = ResultCollector(self.node_manager)

            # 根据配置选择收集模式
            collection_mode = self._get_collection_mode(scenario)

            # 确定结果目录
            custom_result_dir = None
            if base_result_dir:
                # 在基础目录下创建场景特定目录
                custom_result_dir = base_result_dir / scenario.name
                custom_result_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Using custom result directory: {custom_result_dir}")

            # 执行结果收集
            if test_execution_result is not None:
                summary = result_collector.collect_scenario_results(
                    scenario, result, test_execution_result, collection_mode, custom_result_dir=custom_result_dir
                )
            else:
                # 如果没有测试执行结果，创建一个空的结果对象
                from .test_script_executor import TestExecutionResult
                empty_test_result = TestExecutionResult(
                    success=True,
                    artifacts=result.artifacts.copy()
                )
                summary = result_collector.collect_scenario_results(
                    scenario, result, empty_test_result, collection_mode, custom_result_dir=custom_result_dir
                )

            # 将结果摘要信息添加到scenario result中
            result.metrics['result_collection'] = {
                'collection_mode': collection_mode.value,
                'total_files': summary.total_result_files,
                'total_size_mb': summary.total_size_mb,
                'successful_nodes': summary.successful_nodes,
                'failed_nodes': summary.failed_nodes
            }

            logger.info(f"Result collection completed: {summary.total_result_files} files, "
                       f"{summary.total_size_mb:.2f} MB total")

        except Exception as e:
            logger.error(f"Failed to collect distributed results: {e}")
            # 不抛出异常，允许场景继续完成
            result.metrics['result_collection'] = {
                'error': str(e),
                'collection_mode': 'failed'
            }
    
    def _stop_distributed_services(self, scenario: Scenario, result: ScenarioResult, logger: logging.Logger):
        """停止分布式服务（支持并发）"""
        scenario_path = Path(scenario.directory)
        
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
        if not scenario.metadata:
            raise RuntimeError(f"Scenario {scenario.name} missing metadata")
        
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
    
