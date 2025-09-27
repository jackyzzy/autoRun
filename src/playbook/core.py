"""
Playbook核心模块
整合所有组件，提供统一的API接口
"""

import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from pathlib import Path
import os
from dataclasses import dataclass
from enum import Enum

from .node_manager import NodeManager
from .scenario_manager import ScenarioManager
from .scenario_runner import ScenarioRunner, ScenarioResult
from .docker_manager import DockerComposeManager
from .benchmark_runner import BenchmarkRunner, TestResult
from .result.result_collector import ResultCollector
from .result.result_models import ResultSummary
from .health_checker import HealthChecker, HealthStatus
from .exceptions import (
    PlaybookException, ConfigurationError, ScenarioError,
    NodeOperationError, TestExecutionError, wrap_exception
)
from ..utils.logger import setup_logging
from ..utils.config_validator import ConfigValidator


class SystemStatus(Enum):
    """系统状态枚举"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    ERROR = "error"


@dataclass
class ExecutionResult:
    """执行结果数据类"""
    status: str
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """检查是否执行成功"""
        return self.status in ['completed', 'success']


class PlaybookCore:
    """Playbook核心类，集成所有功能模块

    这个类是整个系统的入口点，负责协调各个组件的工作，
    提供统一的API接口供CLI和其他客户端使用。

    Attributes:
        config_dir: 配置文件目录
        scenarios_dir: 场景目录
        results_dir: 结果目录
        node_manager: 节点管理器实例
        scenario_manager: 场景管理器实例
        docker_manager: Docker管理器实例
        benchmark_runner: 基准测试执行器实例
        result_collector: 结果收集器实例
        health_checker: 健康检查器实例
        scenario_runner: 场景执行器实例
    """

    def __init__(self,
                 config_dir: Union[str, Path] = "config",
                 scenarios_dir: Union[str, Path] = "config/scenarios",
                 results_dir: Union[str, Path] = "results",
                 log_level: str = "INFO") -> None:
        """
        初始化Playbook核心
        
        Args:
            config_dir: 配置文件目录
            scenarios_dir: 场景目录
            results_dir: 结果目录
            log_level: 日志级别
        """
        # 设置日志
        self.logger = setup_logging(log_level)
        self.logger = logging.getLogger("playbook.core")
        
        # 配置路径
        self.config_dir = Path(config_dir)
        self.scenarios_dir = Path(scenarios_dir)
        self.results_dir = Path(results_dir)
        
        # 初始化组件
        self._init_components()
        
        self.logger.info("Playbook core initialized successfully")
    
    def _init_components(self) -> None:
        """初始化各个组件"""
        try:
            # 节点管理器
            nodes_config = self.config_dir / "nodes.yaml"
            if not nodes_config.exists():
                self.logger.warning(f"Node configuration file not found: {nodes_config}")
            self.node_manager = NodeManager(str(nodes_config) if nodes_config.exists() else None)

            # 场景管理器
            scenarios_config = self.config_dir / "scenarios.yaml"
            if not scenarios_config.exists():
                self.logger.warning(f"Scenario configuration file not found: {scenarios_config}")
            self.scenario_manager = ScenarioManager(
                str(scenarios_config) if scenarios_config.exists() else None,
                str(self.scenarios_dir)
            )

            # Docker管理器
            self.docker_manager = DockerComposeManager(self.node_manager)

            # 基准测试执行器
            self.benchmark_runner = BenchmarkRunner(self.node_manager)

            # 结果收集器
            self.result_collector = ResultCollector(self.node_manager, str(self.results_dir))

            # 健康检查器
            self.health_checker = HealthChecker(self.node_manager, self.docker_manager)

            # 场景执行器
            self.scenario_runner = ScenarioRunner(self.scenario_manager, self.node_manager)

            # 设置场景执行器回调
            self._setup_scenario_callbacks()

        except FileNotFoundError as e:
            raise ConfigurationError(
                f"Required configuration file not found: {e.filename}",
                error_code="CONFIG_FILE_NOT_FOUND",
                details={'missing_file': str(e.filename)},
                original_exception=e
            )
        except PermissionError as e:
            raise ConfigurationError(
                f"Permission denied accessing configuration: {e.filename}",
                error_code="CONFIG_PERMISSION_DENIED",
                details={'file': str(e.filename)},
                original_exception=e
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise ConfigurationError(
                f"Component initialization failed: {str(e)}",
                error_code="COMPONENT_INIT_FAILED",
                original_exception=e
            )
    
    def _setup_scenario_callbacks(self) -> None:
        """设置场景执行器回调函数"""
        def on_scenario_start(scenario_name: str):
            self.logger.info(f"Scenario started: {scenario_name}")
            
        def on_scenario_complete(scenario_name: str, result: ScenarioResult):
            self.logger.info(f"Scenario completed: {scenario_name}, status: {result.status.value}")
            
        def on_all_complete(results: Dict[str, ScenarioResult]):
            summary = self.scenario_runner.get_execution_summary()
            self.logger.info(f"All scenarios completed. Success rate: {summary['success_rate']:.1f}%")
        
        self.scenario_runner.on_scenario_start = on_scenario_start
        self.scenario_runner.on_scenario_complete = on_scenario_complete
        self.scenario_runner.on_all_complete = on_all_complete
    
    def run_full_test_suite(self) -> ExecutionResult:
        """
        运行完整的测试套件

        Returns:
            测试执行结果摘要
        """
        self.logger.info("Starting full test suite execution")

        try:
            # 创建统一的结果目录用于全场景执行
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            suite_result_dir = self.results_dir / f"{timestamp}_all_scenarios"
            suite_result_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created suite result directory: {suite_result_dir}")

            # 1. 系统健康检查
            self.logger.info("\n\nStep 1: Performing health checks")
            health_results = self.health_checker.run_all_checks()
            overall_health = self.health_checker.get_overall_health()

            if overall_health == HealthStatus.UNHEALTHY:
                raise RuntimeError("System health check failed. Cannot proceed with tests.")
            elif overall_health == HealthStatus.DEGRADED:
                self.logger.warning("System health is degraded, but continuing with tests")

            # 2. 验证测试环境
            self.logger.info("\n\nStep 2: Validating test environment")
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
            validation_results = self.benchmark_runner.validate_test_environment(node_names)

            failed_nodes = [
                name for name, result in validation_results.items()
                if result['overall_status'] != 'ready'
            ]

            if failed_nodes:
                self.logger.warning(f"Some nodes failed validation: {failed_nodes}")

            # 3. 执行所有场景
            self.logger.info("\n\nStep 3: Executing all scenarios")
            scenario_results = self.scenario_runner.run_all_scenarios(base_result_dir=suite_result_dir)

            # 4. 收集和汇总结果
            self.logger.info("\n\nStep 4: Collecting and summarizing results")

            # 生成测试套件汇总报告（基于已收集的场景结果）
            self.logger.info("Generating test suite summary from all scenario results")
            execution_summary = self.scenario_runner.get_execution_summary()
            health_report = self.health_checker.get_health_report()

            suite_summary = self.result_collector.generate_test_suite_summary(
                scenario_results, execution_summary, health_report, suite_result_dir=suite_result_dir
            )

            # 5. 生成整体报告
            execution_data = {
                'execution_summary': execution_summary,
                'health_report': health_report,
                'test_suite_summary': suite_summary.to_dict(),
                'validation_results': validation_results
            }

            self.logger.info("\n\nFull test suite execution completed successfully")
            return ExecutionResult(
                status='completed',
                message="Full test suite execution completed successfully",
                data=execution_data
            )
            
        except Exception as e:
            self.logger.error(f"Full test suite execution failed: {e}")
            return ExecutionResult(
                status='failed',
                error=str(e),
                data={
                    'execution_summary': self.scenario_runner.get_execution_summary(),
                    'health_report': self.health_checker.get_health_report()
                }
            )
    
    def run_single_scenario(self, scenario_name: str) -> ExecutionResult:
        """
        运行单个场景

        Args:
            scenario_name: 场景名称

        Returns:
            场景执行结果
        """
        self.logger.info(f"Running single scenario: {scenario_name}")

        try:
            scenario = self.scenario_manager.get_scenario(scenario_name)
            if not scenario:
                return ExecutionResult(
                    status='failed',
                    error=f'Scenario not found: {scenario_name}',
                    data={'available_scenarios': list(self.scenario_manager.get_scenarios().keys())}
                )
        except Exception as e:
            self.logger.error(f"Failed to load scenario {scenario_name}: {e}")
            return ExecutionResult(
                status='failed',
                error=f"Failed to load scenario: {str(e)}"
            )

        if not scenario.enabled:
            return ExecutionResult(
                status='skipped',
                message='Scenario is disabled'
            )

        try:
            # 创建单场景结果目录
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            single_result_dir = self.results_dir / f"{timestamp}_{scenario_name}"
            single_result_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Created single scenario result directory: {single_result_dir}")

            # 运行场景
            result = self.scenario_runner.run_scenario(scenario_name, base_result_dir=single_result_dir)
            
            # 收集结果
            if result.is_success:
                nodes = self.node_manager.get_nodes(enabled_only=True)
                node_names = [node.name for node in nodes]
                
                # 注意：TestResult需要正确的参数，这里简化处理
                # 实际使用中，应该从真实的测试执行结果创建
                
                # 注意：在单场景执行中，result_collector的结果收集已经在scenario_runner中完成
                # 这里不需要重复收集，可以从result.metrics中获取收集信息
                if 'result_collection' in result.metrics:
                    # 结果已经收集，创建一个简单的摘要对象
                    summary = ResultSummary(
                        scenario_name=scenario_name,
                        timestamp=result.start_time.strftime("%Y%m%d_%H%M%S") if result.start_time else "",
                        total_nodes=len(node_names),
                        successful_nodes=result.metrics['result_collection'].get('successful_nodes', 0),
                        failed_nodes=result.metrics['result_collection'].get('failed_nodes', 0),
                        total_result_files=result.metrics['result_collection'].get('total_files', 0),
                        total_size_mb=result.metrics['result_collection'].get('total_size_mb', 0.0)
                    )
                else:
                    # 兜底：创建基本摘要
                    summary = ResultSummary(
                        scenario_name=scenario_name,
                        timestamp=result.start_time.strftime("%Y%m%d_%H%M%S") if result.start_time else "",
                        total_nodes=len(node_names),
                        successful_nodes=1 if result.is_success else 0,
                        failed_nodes=0 if result.is_success else 1
                    )
                
                return ExecutionResult(
                    status='completed',
                    message='Scenario completed successfully',
                    data={
                        'scenario_result': result.to_dict(),
                        'result_summary': summary.to_dict()
                    }
                )
            else:
                return ExecutionResult(
                    status='failed',
                    error=result.error_message,
                    data={'scenario_result': result.to_dict()}
                )
                
        except PlaybookException as e:
            self.logger.error(f"Playbook error running scenario {scenario_name}: {e}")
            return ExecutionResult(
                status='failed',
                error=str(e),
                data=e.to_dict()
            )
        except Exception as e:
            self.logger.error(f"Unexpected error running scenario {scenario_name}: {e}")
            return ExecutionResult(
                status='failed',
                error=f"Unexpected error: {str(e)}"
            )
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            # 获取各组件状态
            node_summary = self.node_manager.get_summary()
            scenario_summary = self.scenario_manager.get_summary()
            docker_summary = self.docker_manager.get_summary()
            benchmark_summary = self.benchmark_runner.get_summary()
            health_report = self.health_checker.get_health_report()
            collection_status = self.result_collector.get_collection_status()
            
            return {
                'overall_status': health_report['overall_status'],
                'components': {
                    'nodes': node_summary,
                    'scenarios': scenario_summary,
                    'docker': docker_summary,
                    'benchmark': benchmark_summary,
                    'result_collection': collection_status
                },
                'health': health_report,
                'is_running': self.scenario_runner.is_running,
                'current_scenario': self.scenario_runner.current_scenario
            }
            
        except PlaybookException as e:
            self.logger.error(f"Playbook error getting system status: {e}")
            return {
                'overall_status': 'error',
                'error': str(e),
                'error_details': e.to_dict()
            }
        except Exception as e:
            self.logger.error(f"Unexpected error getting system status: {e}")
            return {
                'overall_status': 'error',
                'error': f"Unexpected error: {str(e)}"
            }
    
    def list_scenarios(self) -> Dict[str, Any]:
        """列出所有场景"""
        scenarios = self.scenario_manager.get_scenarios(enabled_only=False)
        execution_order = self.scenario_manager.get_execution_order()
        
        return {
            'total_scenarios': len(scenarios),
            'enabled_scenarios': len([s for s in scenarios if s.enabled]),
            'execution_order': execution_order,
            'scenarios': {
                scenario.name: {
                    'name': scenario.name,
                    'description': scenario.description,
                    'directory': scenario.directory,
                    'enabled': scenario.enabled,
                    'is_valid': scenario.is_valid,
                    'metadata': scenario.metadata.to_dict() if scenario.metadata else None
                }
                for scenario in scenarios
            }
        }
    
    def list_nodes(self) -> Dict[str, Any]:
        """列出所有节点"""
        nodes = self.node_manager.get_nodes(enabled_only=False)
        
        return {
            'total_nodes': len(nodes),
            'enabled_nodes': len([n for n in nodes if n.enabled]),
            'nodes': {
                node.name: node.to_dict()
                for node in nodes
            },
            'connectivity': self.node_manager.test_connectivity([node.name for node in nodes])
        }
    
    def enable_scenario(self, scenario_name: str) -> bool:
        """启用场景"""
        success = self.scenario_manager.enable_scenario(scenario_name)
        if success:
            self.logger.info(f"Enabled scenario: {scenario_name}")
        return success
    
    def disable_scenario(self, scenario_name: str) -> bool:
        """禁用场景"""
        success = self.scenario_manager.disable_scenario(scenario_name)
        if success:
            self.logger.info(f"Disabled scenario: {scenario_name}")
        return success
    
    def reorder_scenarios(self, new_order: List[str]) -> bool:
        """重新排序场景"""
        success = self.scenario_manager.reorder_scenarios(new_order)
        if success:
            self.logger.info(f"Reordered scenarios: {new_order}")
        return success
    
    def validate_configuration(self) -> Dict[str, Any]:
        """验证配置"""
        results = {
            'overall_valid': True,
            'issues': [],
            'warnings': [],
            'security_issues': [],
            'suggestions': []
        }

        try:
            # 创建配置验证器
            validator = ConfigValidator()
            validation_results = []

            # 验证节点配置文件
            nodes_config = self.config_dir / "nodes.yaml"
            if nodes_config.exists():
                self.logger.info("Validating nodes configuration...")
                nodes_result = validator.validate_nodes_config(nodes_config)
                validation_results.append(nodes_result)

                results['issues'].extend(nodes_result.errors)
                results['warnings'].extend(nodes_result.warnings)
                results['security_issues'].extend(nodes_result.security_issues)
                results['suggestions'].extend(nodes_result.suggestions)

                if not nodes_result.is_valid:
                    results['overall_valid'] = False
            else:
                results['warnings'].append(f"Nodes configuration file not found: {nodes_config}")

            # 验证场景配置文件
            scenarios_config = self.config_dir / "scenarios.yaml"
            if scenarios_config.exists():
                self.logger.info("Validating scenarios configuration...")
                scenarios_result = validator.validate_scenarios_config(scenarios_config)
                validation_results.append(scenarios_result)

                results['issues'].extend(scenarios_result.errors)
                results['warnings'].extend(scenarios_result.warnings)
                results['security_issues'].extend(scenarios_result.security_issues)
                results['suggestions'].extend(scenarios_result.suggestions)

                if not scenarios_result.is_valid:
                    results['overall_valid'] = False
            else:
                results['warnings'].append(f"Scenarios configuration file not found: {scenarios_config}")

            # 验证场景内容
            scenario_validation = self.scenario_manager.validate_scenarios()
            if scenario_validation['invalid']:
                results['issues'].extend([
                    f"Invalid scenario: {name}" for name in scenario_validation['invalid']
                ])
                results['overall_valid'] = False

            if scenario_validation['missing_dependencies']:
                results['warnings'].extend([
                    f"Missing dependencies: {dep}" for dep in scenario_validation['missing_dependencies']
                ])

            # 验证节点连接
            nodes = self.node_manager.get_nodes(enabled_only=True)
            if not nodes:
                results['issues'].append("No enabled nodes configured")
                results['overall_valid'] = False
            else:
                self.logger.info("Testing node connectivity...")
                connectivity = self.node_manager.test_connectivity([node.name for node in nodes])
                failed_nodes = [name for name, connected in connectivity.items() if not connected]
                if failed_nodes:
                    results['warnings'].extend([
                        f"Cannot connect to node: {name}" for name in failed_nodes
                    ])

            # 验证目录结构
            if not self.scenarios_dir.exists():
                results['issues'].append(f"Scenarios directory not found: {self.scenarios_dir}")
                results['overall_valid'] = False

            # 生成安全报告
            if validation_results:
                security_report = validator.generate_security_report(validation_results)
                results['security_report'] = security_report

                # 检查是否有严重安全问题
                if security_report['summary']['critical_issues'] > 0:
                    results['overall_valid'] = False
                    results['issues'].append(
                        f"Found {security_report['summary']['critical_issues']} critical security issues"
                    )

            self.logger.info(f"Configuration validation completed: valid={results['overall_valid']}")

        except PlaybookException as e:
            results['overall_valid'] = False
            results['issues'].append(f"Validation error: {str(e)}")
            self.logger.error(f"Configuration validation failed: {e}")
            if hasattr(e, 'to_dict'):
                results['error_details'] = e.to_dict()
        except Exception as e:
            results['overall_valid'] = False
            results['issues'].append(f"Unexpected validation error: {str(e)}")
            self.logger.error(f"Configuration validation failed unexpectedly: {e}")

        return results
    
    def cleanup(self) -> None:
        """清理资源"""
        try:
            self.logger.info("Cleaning up resources")
            
            # 关闭SSH连接
            self.node_manager.close_all_connections()
            
            # 停止正在运行的任务
            if hasattr(self.scenario_runner, 'cancel'):
                self.scenario_runner.cancel()
            
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def __enter__(self) -> 'PlaybookCore':
        return self
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        self.cleanup()