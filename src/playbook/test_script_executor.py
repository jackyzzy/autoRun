"""
测试脚本执行器
处理本地和远程测试脚本的执行
"""

import os
import time
import logging
import subprocess
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass

from .scenario_manager import TestExecution, Scenario
from .node_manager import NodeManager


@dataclass
class TestExecutionResult:
    """测试执行结果"""
    success: bool
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    duration: float = 0.0
    artifacts: List[str] = None
    metrics: Dict[str, Any] = None
    error_message: str = ""
    
    def __post_init__(self):
        if self.artifacts is None:
            self.artifacts = []
        if self.metrics is None:
            self.metrics = {}


class TestScriptExecutor:
    """测试脚本执行器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.test_script_executor")
        self.node_manager = node_manager
    
    def execute_local_script(self, scenario: Scenario, test_config: TestExecution,
                           timeout: int = None) -> TestExecutionResult:
        """在本地执行测试脚本"""
        if timeout is None:
            timeout = test_config.timeout
        
        start_time = time.time()
        script_path = Path(scenario.directory) / test_config.script
        
        if not script_path.exists():
            self.logger.error(f"Local test script not found: {script_path}")
            self.logger.debug(f"Current working directory: {os.getcwd()}")
            self.logger.debug(f"Scenario directory: {scenario.directory}")
            self.logger.debug(f"Script name from config: {test_config.script}")
            return TestExecutionResult(
                success=False,
                error_message=f"Test script not found: {script_path}",
                duration=time.time() - start_time
            )
        
        self.logger.info(f"Executing test script locally: {script_path}")
        
        try:
            # 设置工作目录为场景目录
            work_dir = Path(scenario.directory)
            
            # 构建执行命令（使用相对于工作目录的路径）
            if script_path.suffix == '.sh':
                cmd = ['bash', test_config.script]
            elif script_path.suffix == '.py':
                cmd = ['python', test_config.script]
            else:
                cmd = [test_config.script]
            
            self.logger.debug(f"执行命令: {' '.join(cmd)}, 工作目录: {work_dir}")
            
            # 设置环境变量
            env = os.environ.copy()
            env['SCENARIO_NAME'] = scenario.name
            env['SCENARIO_PATH'] = str(work_dir)
            
            # 执行脚本
            process = subprocess.Popen(
                cmd,
                cwd=work_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                exit_code = process.returncode
                duration = time.time() - start_time
                
                # 收集结果文件
                artifacts = self._collect_local_artifacts(work_dir, test_config.result_paths)
                
                # 解析测试指标（如果有）
                metrics = self._parse_test_metrics(stdout, stderr)
                
                result = TestExecutionResult(
                    success=(exit_code == 0),
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration=duration,
                    artifacts=artifacts,
                    metrics=metrics
                )
                
                if exit_code == 0:
                    self.logger.info(f"Test script completed successfully in {duration:.2f}s")
                else:
                    result.error_message = f"Script exited with code {exit_code}"
                    self.logger.error(f"Test script failed with exit code {exit_code}")
                
                return result
                
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                return TestExecutionResult(
                    success=False,
                    exit_code=-1,
                    stdout=stdout or "",
                    stderr=stderr or "",
                    duration=time.time() - start_time,
                    error_message=f"Script execution timed out after {timeout}s"
                )
                
        except Exception as e:
            self.logger.error(f"Failed to execute local script: {str(e)}")
            self.logger.debug(f"Exception details:", exc_info=True)
            return TestExecutionResult(
                success=False,
                error_message=f"Failed to execute script: {str(e)}",
                duration=time.time() - start_time
            )
    
    def execute_remote_script(self, scenario: Scenario, test_config: TestExecution,
                            node_name: str, timeout: int = None) -> TestExecutionResult:
        """在远程节点执行测试脚本"""
        if timeout is None:
            timeout = test_config.timeout
        
        start_time = time.time()
        script_path = Path(scenario.directory) / test_config.script
        
        if not script_path.exists():
            self.logger.error(f"Remote test script not found: {script_path}")
            self.logger.debug(f"Current working directory: {os.getcwd()}")
            self.logger.debug(f"Scenario directory: {scenario.directory}")
            self.logger.debug(f"Script name from config: {test_config.script}")
            self.logger.debug(f"Node name: {node_name}")
            return TestExecutionResult(
                success=False,
                error_message=f"Test script not found: {script_path}",
                duration=time.time() - start_time
            )
        
        node = self.node_manager.get_node(node_name)
        if not node:
            return TestExecutionResult(
                success=False,
                error_message=f"Node {node_name} not found",
                duration=time.time() - start_time
            )
        
        self.logger.info(f"Executing test script on remote node {node_name}: {script_path}")
        
        try:
            # 上传测试脚本到远程节点
            remote_script_path = f"{node.work_dir}/test_script_{scenario.name}.sh"
            upload_success = self.node_manager.upload_file(
                str(script_path), remote_script_path, [node_name]
            )
            
            if not upload_success.get(node_name, False):
                return TestExecutionResult(
                    success=False,
                    error_message=f"Failed to upload script to {node_name}",
                    duration=time.time() - start_time
                )
            
            # 设置执行权限并运行脚本
            commands = [
                f"chmod +x {remote_script_path}",
                f"cd {node.work_dir}",
                f"export SCENARIO_NAME='{scenario.name}'",
                f"export SCENARIO_PATH='{node.work_dir}'",
                f"bash {remote_script_path}"
            ]
            
            command = " && ".join(commands)
            results = self.node_manager.execute_command(command, [node_name], timeout=timeout)
            
            duration = time.time() - start_time
            node_result = results.get(node_name)
            
            if not node_result:
                return TestExecutionResult(
                    success=False,
                    error_message="Failed to get execution result from remote node",
                    duration=duration
                )
            
            exit_code, stdout, stderr = node_result
            
            # 收集远程结果文件
            artifacts = self._collect_remote_artifacts(node_name, node, test_config.result_paths)
            
            # 解析测试指标
            metrics = self._parse_test_metrics(stdout, stderr)
            
            result = TestExecutionResult(
                success=(exit_code == 0),
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                duration=duration,
                artifacts=artifacts,
                metrics=metrics
            )
            
            if exit_code != 0:
                result.error_message = f"Remote script exited with code {exit_code}"
                self.logger.error(f"Remote test script failed with exit code {exit_code}")
            else:
                self.logger.info(f"Remote test script completed successfully in {duration:.2f}s")
            
            # 清理远程脚本文件
            try:
                self.node_manager.execute_command(f"rm -f {remote_script_path}", [node_name])
            except Exception as e:
                self.logger.warning(f"Failed to cleanup remote script: {e}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to execute remote script on {node_name}: {str(e)}")
            self.logger.debug(f"Exception details:", exc_info=True)
            return TestExecutionResult(
                success=False,
                error_message=f"Failed to execute remote script: {str(e)}",
                duration=time.time() - start_time
            )
    
    def execute_test(self, scenario: Scenario, test_config: TestExecution) -> TestExecutionResult:
        """执行测试脚本（根据配置选择本地或远程）"""
        if test_config.node == "local":
            return self.execute_local_script(scenario, test_config)
        else:
            return self.execute_remote_script(scenario, test_config, test_config.node)
    
    def _collect_local_artifacts(self, work_dir: Path, result_paths: List[str]) -> List[str]:
        """收集本地结果文件"""
        artifacts = []
        
        for result_path in result_paths:
            try:
                if result_path.startswith('/'):
                    # 绝对路径
                    path = Path(result_path)
                else:
                    # 相对路径（相对于工作目录）
                    path = work_dir / result_path
                
                if path.exists():
                    if path.is_file():
                        artifacts.append(str(path))
                    elif path.is_dir():
                        # 收集目录下所有文件
                        for file_path in path.rglob('*'):
                            if file_path.is_file():
                                artifacts.append(str(file_path))
                
            except Exception as e:
                self.logger.warning(f"Failed to collect artifact from {result_path}: {e}")
        
        return artifacts
    
    def _collect_remote_artifacts(self, node_name: str, node, result_paths: List[str]) -> List[str]:
        """收集远程结果文件"""
        artifacts = []
        
        if not result_paths:
            return artifacts
        
        try:
            # 创建本地结果目录
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            local_result_dir = Path(f"results/{timestamp}/{node_name}")
            local_result_dir.mkdir(parents=True, exist_ok=True)
            
            for result_path in result_paths:
                try:
                    # 下载文件
                    download_results = self.node_manager.download_files(
                        result_path,
                        str(local_result_dir),
                        [node_name]
                    )
                    
                    if download_results.get(node_name, False):
                        # 查找下载的文件
                        for file_path in local_result_dir.rglob('*'):
                            if file_path.is_file():
                                artifacts.append(str(file_path))
                    
                except Exception as e:
                    self.logger.warning(f"Failed to download artifact from {result_path}: {e}")
            
        except Exception as e:
            self.logger.error(f"Failed to collect remote artifacts: {e}")
        
        return artifacts
    
    def _parse_test_metrics(self, stdout: str, stderr: str) -> Dict[str, Any]:
        """解析测试输出中的指标"""
        metrics = {}
        
        try:
            # 查找JSON格式的指标输出
            import json
            import re
            
            # 在stdout中查找JSON块
            json_pattern = r'METRICS_START\s*(\{.*?\})\s*METRICS_END'
            matches = re.findall(json_pattern, stdout, re.DOTALL)
            
            for match in matches:
                try:
                    parsed_metrics = json.loads(match)
                    metrics.update(parsed_metrics)
                except json.JSONDecodeError:
                    pass
            
            # 查找简单的key=value格式指标
            kv_pattern = r'METRIC:(\w+)=([^\s]+)'
            kv_matches = re.findall(kv_pattern, stdout)
            
            for key, value in kv_matches:
                try:
                    # 尝试转换为数字
                    if '.' in value:
                        metrics[key] = float(value)
                    else:
                        metrics[key] = int(value)
                except ValueError:
                    metrics[key] = value
            
        except Exception as e:
            self.logger.debug(f"Failed to parse metrics: {e}")
        
        return metrics