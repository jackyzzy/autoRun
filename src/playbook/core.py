"""
Playbook核心模块
整合所有组件，提供统一的API接口
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
import os

from .node_manager import NodeManager
from .scenario_manager import ScenarioManager
from .scenario_runner import ScenarioRunner, ScenarioResult
from .docker_manager import DockerComposeManager
from .benchmark_runner import BenchmarkRunner, TestResult
from .result_collector import ResultCollector, ResultSummary
from .health_checker import HealthChecker, HealthStatus
from ..utils.logger import setup_logging


class PlaybookCore:
    """Playbook核心类，集成所有功能模块"""
    
    def __init__(self, 
                 config_dir: str = "config",
                 scenarios_dir: str = "config/scenarios",
                 results_dir: str = "results",
                 log_level: str = "INFO"):
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
    
    def _init_components(self):
        """初始化各个组件"""
        try:
            # 节点管理器
            nodes_config = self.config_dir / "nodes.yaml"
            self.node_manager = NodeManager(str(nodes_config) if nodes_config.exists() else None)
            
            # 场景管理器
            scenarios_config = self.config_dir / "scenarios.yaml"
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
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    def _setup_scenario_callbacks(self):
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
    
    def run_full_test_suite(self) -> Dict[str, Any]:
        """
        运行完整的测试套件
        
        Returns:
            测试执行结果摘要
        """
        self.logger.info("Starting full test suite execution")
        
        try:
            # 1. 系统健康检查
            self.logger.info("Step 1: Performing health checks")
            health_results = self.health_checker.run_all_checks()
            overall_health = self.health_checker.get_overall_health()
            
            if overall_health == HealthStatus.UNHEALTHY:
                raise RuntimeError("System health check failed. Cannot proceed with tests.")
            elif overall_health == HealthStatus.DEGRADED:
                self.logger.warning("System health is degraded, but continuing with tests")
            
            # 2. 验证测试环境
            self.logger.info("Step 2: Validating test environment")
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
            self.logger.info("Step 3: Executing all scenarios")
            scenario_results = self.scenario_runner.run_all_scenarios()
            
            # 4. 收集和汇总结果
            self.logger.info("Step 4: Collecting and summarizing results")
            
            # 为每个场景收集结果（这里简化处理）
            all_summaries = {}
            for scenario_name, scenario_result in scenario_results.items():
                if scenario_result.is_success:
                    # 这里需要根据实际的测试结果来创建TestResult对象
                    # 暂时创建空的结果用于演示
                    test_results = {node_name: TestResult(
                        test_id=f"{scenario_name}_{node_name}",
                        status=scenario_result.status,
                        config=None,  # 这里应该是实际的配置
                        node_name=node_name
                    ) for node_name in node_names}
                    
                    summary = self.result_collector.collect_scenario_results(
                        scenario_name, test_results, node_names
                    )
                    all_summaries[scenario_name] = summary
            
            # 5. 生成整体报告
            execution_summary = self.scenario_runner.get_execution_summary()
            health_report = self.health_checker.get_health_report()
            
            final_results = {
                'status': 'completed',
                'execution_summary': execution_summary,
                'health_report': health_report,
                'scenario_summaries': {name: summary.to_dict() for name, summary in all_summaries.items()},
                'validation_results': validation_results
            }
            
            self.logger.info("Full test suite execution completed successfully")
            return final_results
            
        except Exception as e:
            self.logger.error(f"Full test suite execution failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'execution_summary': self.scenario_runner.get_execution_summary(),
                'health_report': self.health_checker.get_health_report()
            }
    
    def run_single_scenario(self, scenario_name: str) -> Dict[str, Any]:
        """
        运行单个场景
        
        Args:
            scenario_name: 场景名称
            
        Returns:
            场景执行结果
        """
        self.logger.info(f"Running single scenario: {scenario_name}")
        
        scenario = self.scenario_manager.get_scenario(scenario_name)
        if not scenario:
            return {
                'status': 'failed',
                'error': f'Scenario not found: {scenario_name}'
            }
        
        if not scenario.enabled:
            return {
                'status': 'skipped',
                'reason': 'Scenario is disabled'
            }
        
        try:
            # 运行场景
            result = self.scenario_runner.run_scenario(scenario_name)
            
            # 收集结果
            if result.is_success:
                nodes = self.node_manager.get_nodes(enabled_only=True)
                node_names = [node.name for node in nodes]
                
                # 创建模拟的测试结果
                test_results = {node_name: TestResult(
                    test_id=f"{scenario_name}_{node_name}",
                    status=result.status,
                    config=None,
                    node_name=node_name
                ) for node_name in node_names}
                
                summary = self.result_collector.collect_scenario_results(
                    scenario_name, test_results, node_names
                )
                
                return {
                    'status': 'completed',
                    'scenario_result': result.to_dict(),
                    'result_summary': summary.to_dict()
                }
            else:
                return {
                    'status': 'failed',
                    'scenario_result': result.to_dict(),
                    'error': result.error_message
                }
                
        except Exception as e:
            self.logger.error(f"Error running scenario {scenario_name}: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
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
            
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'overall_status': 'error',
                'error': str(e)
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
            'warnings': []
        }
        
        try:
            # 验证场景
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
            
            self.logger.info(f"Configuration validation completed: valid={results['overall_valid']}")
            
        except Exception as e:
            results['overall_valid'] = False
            results['issues'].append(f"Validation error: {str(e)}")
            self.logger.error(f"Configuration validation failed: {e}")
        
        return results
    
    def cleanup(self):
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
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()