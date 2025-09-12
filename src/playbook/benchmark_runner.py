"""
基准测试执行器
负责运行AICP基准测试并收集性能数据
"""

import logging
import time
import json
import re
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from .node_manager import NodeManager
from ..utils.config_loader import ConfigLoader


class TestStatus(Enum):
    """测试状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BenchmarkConfig:
    """基准测试配置"""
    base_url: str
    model: str
    tokenizer_path: str
    dataset_path: str
    dataset_name: str = "sharegpt"
    num_prompts: int = 1000
    max_concurrency: int = 20
    sharegpt_input_len: int = 4096
    sharegpt_output_len: int = 1024
    sharegpt_prompt_len_scale: float = 0.1
    enable_same_prompt: bool = True
    metadata: Dict[str, str] = field(default_factory=dict)
    
    # 容器配置
    container_image: str = "sangfor.com/aicp-benchmark:v0.0.6"
    results_mount_path: str = "/home/zjwei/benchmark/results:/benchmark/data/results"
    network_mode: str = "host"
    ipc_mode: str = "host"
    privileged: bool = True
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'BenchmarkConfig':
        """从字典创建配置"""
        # 提取metadata
        metadata = config.pop('metadata', {})
        
        # 创建实例
        instance = cls(**{k: v for k, v in config.items() if k in cls.__dataclass_fields__})
        instance.metadata = metadata
        
        return instance
    
    def to_command_args(self) -> List[str]:
        """转换为命令行参数"""
        args = [
            f"--base-url {self.base_url}",
            f"--model {self.model}",
            f"--tokenizer-path {self.tokenizer_path}",
            f"--dataset-path {self.dataset_path}",
            f"--dataset-name {self.dataset_name}",
            f"--num-prompts {self.num_prompts}",
            f"--max-concurrency {self.max_concurrency}",
            f"--sharegpt-input-len {self.sharegpt_input_len}",
            f"--sharegpt-output-len {self.sharegpt_output_len}",
            f"--sharegpt-prompt-len-scale {self.sharegpt_prompt_len_scale}",
        ]
        
        if self.enable_same_prompt:
            args.append("--enable-same-prompt")
        
        # 添加metadata
        if self.metadata:
            metadata_str = " ".join([f'{k}="{v}"' for k, v in self.metadata.items()])
            args.append(f"--metadata {metadata_str}")
        
        return args
    
    def to_docker_command(self) -> str:
        """生成完整的Docker运行命令"""
        docker_args = [
            "docker run -it --rm"
        ]
        
        # 网络配置
        if self.network_mode:
            docker_args.append(f"--network {self.network_mode}")
        
        # IPC配置
        if self.ipc_mode:
            docker_args.append(f"--ipc={self.ipc_mode}")
        
        # 特权模式
        if self.privileged:
            docker_args.append("--privileged=true")
        
        # 挂载结果目录
        if self.results_mount_path:
            docker_args.append(f"-v {self.results_mount_path}")
        
        # 镜像和参数
        docker_args.append(self.container_image)
        docker_args.extend(self.to_command_args())
        
        return " \\\\\n  ".join(docker_args)


@dataclass
class TestResult:
    """测试结果"""
    test_id: str
    status: TestStatus
    config: BenchmarkConfig
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    node_name: str = ""
    
    # 性能指标
    throughput: float = 0.0  # tokens/second
    latency_mean: float = 0.0  # seconds
    latency_p50: float = 0.0
    latency_p95: float = 0.0
    latency_p99: float = 0.0
    
    # 统计信息
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_tokens: int = 0
    
    # 错误信息
    error_message: str = ""
    logs: str = ""
    
    # 结果文件
    result_files: List[str] = field(default_factory=list)
    
    @property
    def duration(self) -> Optional[float]:
        """获取执行时长（秒）"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """获取成功率"""
        if self.total_requests > 0:
            return self.successful_requests / self.total_requests * 100
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        config_dict = {}
        if self.config is not None:
            config_dict = {
                'base_url': self.config.base_url,
                'model': self.config.model,
                'num_prompts': self.config.num_prompts,
                'max_concurrency': self.config.max_concurrency,
                'metadata': self.config.metadata
            }
        
        return {
            'test_id': self.test_id,
            'status': self.status.value,
            'node_name': self.node_name,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration,
            'throughput': self.throughput,
            'latency_mean': self.latency_mean,
            'latency_p50': self.latency_p50,
            'latency_p95': self.latency_p95,
            'latency_p99': self.latency_p99,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': self.success_rate,
            'total_tokens': self.total_tokens,
            'error_message': self.error_message,
            'result_files': self.result_files,
            'config': config_dict
        }


class BenchmarkRunner:
    """基准测试执行器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.benchmark_runner")
        self.node_manager = node_manager
        self.config_loader = ConfigLoader()
        
        # 运行状态
        self.active_tests: Dict[str, TestResult] = {}
        
    def load_test_config(self, config_file: str) -> BenchmarkConfig:
        """加载测试配置文件"""
        try:
            config_data = self.config_loader.load_yaml(config_file)
            
            # 支持JSON格式
            if config_file.endswith('.json'):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            
            return BenchmarkConfig.from_dict(config_data)
            
        except Exception as e:
            self.logger.error(f"Failed to load test config from {config_file}: {e}")
            raise
    
    def run_benchmark_test(self, config: BenchmarkConfig, node_names: List[str] = None,
                          test_id: str = None) -> Dict[str, TestResult]:
        """
        运行基准测试
        
        Args:
            config: 测试配置
            node_names: 目标节点列表
            test_id: 测试ID
            
        Returns:
            Dict[node_name, TestResult]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        if not test_id:
            test_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"Starting benchmark test {test_id} on {len(node_names)} nodes")
        self.logger.info(f"Test config: {config.model} @ {config.base_url}, "
                        f"{config.num_prompts} prompts, concurrency={config.max_concurrency}")
        
        results = {}
        
        for node_name in node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                self.logger.error(f"Node not found: {node_name}")
                continue
            
            # 创建测试结果对象
            node_test_id = f"{test_id}_{node_name}"
            result = TestResult(
                test_id=node_test_id,
                status=TestStatus.PENDING,
                config=config,
                node_name=node_name
            )
            
            self.active_tests[node_test_id] = result
            results[node_name] = result
            
            try:
                self.logger.info(f"Starting benchmark on node {node_name}")
                self._run_single_node_test(result)
                
            except Exception as e:
                result.status = TestStatus.FAILED
                result.error_message = str(e)
                result.end_time = datetime.now()
                self.logger.error(f"Benchmark failed on node {node_name}: {e}")
            
            finally:
                if node_test_id in self.active_tests:
                    del self.active_tests[node_test_id]
        
        # 汇总结果
        completed_count = sum(1 for r in results.values() if r.status == TestStatus.COMPLETED)
        self.logger.info(f"Benchmark test {test_id} completed: "
                        f"{completed_count}/{len(results)} nodes succeeded")
        
        return results
    
    def _run_single_node_test(self, result: TestResult):
        """在单个节点上运行测试"""
        result.status = TestStatus.RUNNING
        result.start_time = datetime.now()
        
        node = self.node_manager.get_node(result.node_name)
        if not node:
            raise RuntimeError(f"Node not found: {result.node_name}")
        
        # 生成Docker命令
        docker_cmd = result.config.to_docker_command()
        
        self.logger.info(f"Executing benchmark on {result.node_name}")
        self.logger.debug(f"Docker command: {docker_cmd}")
        
        # 执行测试
        try:
            client = node.get_ssh_client()
            with client.connection_context():
                # 设置较长的超时时间
                timeout = max(3600, result.config.num_prompts * 2)  # 至少1小时
                
                exit_code, stdout, stderr = client.execute_command(
                    docker_cmd,
                    timeout=timeout,
                    check_exit_code=False
                )
                
                result.logs = stdout
                if stderr:
                    result.logs += f"\n\nSTDERR:\n{stderr}"
                
                if exit_code == 0:
                    result.status = TestStatus.COMPLETED
                    self._parse_test_output(result, stdout)
                    self.logger.info(f"Benchmark completed successfully on {result.node_name}")
                else:
                    result.status = TestStatus.FAILED
                    result.error_message = f"Docker command failed with exit code {exit_code}"
                    self.logger.error(f"Benchmark failed on {result.node_name}: {result.error_message}")
                
        except Exception as e:
            result.status = TestStatus.FAILED
            result.error_message = str(e)
            raise
        
        finally:
            result.end_time = datetime.now()
    
    def _parse_test_output(self, result: TestResult, output: str):
        """解析测试输出，提取性能指标"""
        try:
            # 解析AICP基准测试的输出格式
            # 这里需要根据实际的输出格式进行调整
            
            # 查找吞吐量 (throughput)
            throughput_match = re.search(r'throughput[:\s]+([0-9.]+)', output, re.IGNORECASE)
            if throughput_match:
                result.throughput = float(throughput_match.group(1))
            
            # 查找延迟指标
            latency_patterns = [
                (r'latency.*mean[:\s]+([0-9.]+)', 'latency_mean'),
                (r'latency.*p50[:\s]+([0-9.]+)', 'latency_p50'),
                (r'latency.*p95[:\s]+([0-9.]+)', 'latency_p95'),
                (r'latency.*p99[:\s]+([0-9.]+)', 'latency_p99'),
            ]
            
            for pattern, attr in latency_patterns:
                match = re.search(pattern, output, re.IGNORECASE)
                if match:
                    setattr(result, attr, float(match.group(1)))
            
            # 查找请求统计
            total_requests_match = re.search(r'total.*requests[:\s]+([0-9]+)', output, re.IGNORECASE)
            if total_requests_match:
                result.total_requests = int(total_requests_match.group(1))
            
            successful_match = re.search(r'successful.*requests[:\s]+([0-9]+)', output, re.IGNORECASE)
            if successful_match:
                result.successful_requests = int(successful_match.group(1))
            
            failed_match = re.search(r'failed.*requests[:\s]+([0-9]+)', output, re.IGNORECASE)
            if failed_match:
                result.failed_requests = int(failed_match.group(1))
            
            # 查找token统计
            tokens_match = re.search(r'total.*tokens[:\s]+([0-9]+)', output, re.IGNORECASE)
            if tokens_match:
                result.total_tokens = int(tokens_match.group(1))
            
            # 如果没有明确的请求数，使用配置中的数量
            if result.total_requests == 0:
                result.total_requests = result.config.num_prompts
            
            # 如果没有明确的成功/失败数，假设全部成功（如果有吞吐量数据）
            if result.successful_requests == 0 and result.throughput > 0:
                result.successful_requests = result.total_requests
            
            self.logger.info(f"Parsed metrics for {result.node_name}: "
                           f"throughput={result.throughput}, "
                           f"latency_mean={result.latency_mean}, "
                           f"success_rate={result.success_rate}%")
            
        except Exception as e:
            self.logger.warning(f"Failed to parse test output for {result.node_name}: {e}")
            # 即使解析失败，测试仍然算作完成
    
    def run_scenario_tests(self, scenario_name: str, scenario_config_file: str,
                          node_names: List[str] = None) -> Dict[str, TestResult]:
        """
        运行场景测试
        
        Args:
            scenario_name: 场景名称
            scenario_config_file: 场景配置文件路径
            node_names: 目标节点列表
            
        Returns:
            Dict[node_name, TestResult]
        """
        self.logger.info(f"Running scenario tests: {scenario_name}")
        
        # 加载场景配置
        config = self.load_test_config(scenario_config_file)
        
        # 为场景添加特定的metadata
        if 'scenario' not in config.metadata:
            config.metadata['scenario'] = scenario_name
        
        # 生成场景专用的测试ID
        test_id = f"scenario_{scenario_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return self.run_benchmark_test(config, node_names, test_id)
    
    def run_custom_command(self, command: str, node_names: List[str] = None,
                          timeout: int = 3600) -> Dict[str, Tuple[int, str, str]]:
        """
        运行自定义测试命令
        
        Args:
            command: 要执行的命令
            node_names: 目标节点列表
            timeout: 超时时间
            
        Returns:
            Dict[node_name, (exit_code, stdout, stderr)]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Running custom command on {len(node_names)} nodes")
        self.logger.debug(f"Command: {command}")
        
        return self.node_manager.execute_command(command, node_names, timeout=timeout)
    
    def validate_test_environment(self, node_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        验证测试环境
        
        Args:
            node_names: 目标节点列表
            
        Returns:
            Dict[node_name, validation_result]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Validating test environment on {len(node_names)} nodes")
        
        validation_commands = {
            'docker_version': 'docker --version',
            'docker_compose_version': 'docker compose version',
            'gpu_info': 'nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits || echo "No GPU"',
            'disk_space': 'df -h /',
            'memory_info': 'free -h',
            'benchmark_image': f'docker images | grep aicp-benchmark || echo "Image not found"'
        }
        
        results = {}
        
        for node_name in node_names:
            node_result = {
                'node_name': node_name,
                'checks': {},
                'overall_status': 'unknown'
            }
            
            check_passed = 0
            total_checks = len(validation_commands)
            
            for check_name, command in validation_commands.items():
                try:
                    cmd_results = self.node_manager.execute_command(command, [node_name], timeout=30)
                    node_cmd_result = cmd_results.get(node_name)
                    
                    if node_cmd_result and node_cmd_result[0] == 0:
                        node_result['checks'][check_name] = {
                            'status': 'passed',
                            'output': node_cmd_result[1].strip()
                        }
                        check_passed += 1
                    else:
                        error_msg = node_cmd_result[2] if node_cmd_result else "Unknown error"
                        node_result['checks'][check_name] = {
                            'status': 'failed',
                            'error': error_msg
                        }
                        
                except Exception as e:
                    node_result['checks'][check_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            # 计算总体状态
            if check_passed == total_checks:
                node_result['overall_status'] = 'ready'
            elif check_passed > total_checks // 2:
                node_result['overall_status'] = 'partial'
            else:
                node_result['overall_status'] = 'failed'
            
            results[node_name] = node_result
            
            self.logger.info(f"Environment validation for {node_name}: "
                           f"{check_passed}/{total_checks} checks passed, "
                           f"status={node_result['overall_status']}")
        
        return results
    
    def get_active_tests(self) -> Dict[str, Dict[str, Any]]:
        """获取当前活动的测试"""
        return {
            test_id: {
                'test_id': test_id,
                'status': result.status.value,
                'node_name': result.node_name,
                'start_time': result.start_time.isoformat() if result.start_time else None,
                'config': {
                    'model': result.config.model,
                    'num_prompts': result.config.num_prompts,
                    'max_concurrency': result.config.max_concurrency
                }
            }
            for test_id, result in self.active_tests.items()
        }
    
    def cancel_test(self, test_id: str) -> bool:
        """取消测试"""
        if test_id in self.active_tests:
            result = self.active_tests[test_id]
            result.status = TestStatus.CANCELLED
            result.end_time = datetime.now()
            
            self.logger.info(f"Cancelled test: {test_id}")
            return True
        
        return False
    
    def get_summary(self) -> Dict[str, Any]:
        """获取基准测试运行器摘要信息"""
        return {
            'active_tests': len(self.active_tests),
            'active_test_ids': list(self.active_tests.keys()),
            'nodes_available': len(self.node_manager.get_nodes(enabled_only=True))
        }