"""
测试脚本执行器
处理本地和远程测试脚本的执行
"""

import os
import time
import logging
import subprocess
import threading
import select
import sys
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from .scenario_manager import TestExecution, Scenario
from .node_manager import NodeManager
from ..utils.progress_logger import StatusSpinner


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
        self.spinner = StatusSpinner(self.logger)
    
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
            
            # 执行脚本并实时显示输出
            result = self._execute_with_realtime_output(
                cmd, work_dir, env, timeout, start_time
            )

            # 收集结果文件
            if result.success:
                artifacts = self._collect_local_artifacts(work_dir, test_config.result_paths)
                result.artifacts = artifacts

            return result
                
        except Exception as e:
            self.logger.error(f"Failed to execute local script: {str(e)}")
            self.logger.debug(f"Exception details:", exc_info=True)
            return TestExecutionResult(
                success=False,
                error_message=f"Failed to execute script: {str(e)}",
                duration=time.time() - start_time
            )

    def _execute_with_realtime_output(self, cmd: List[str], work_dir: Path, env: Dict[str, str],
                                    timeout: int, start_time: float) -> TestExecutionResult:
        """执行脚本并实时显示输出"""
        self.logger.info(f"🚀 Starting test execution: {' '.join(cmd)}")

        # 启动进度指示器
        self.spinner.start(f"Executing test script...")

        try:
            # 启动进程
            process = subprocess.Popen(
                cmd,
                cwd=work_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # 行缓冲
                universal_newlines=True
            )

            stdout_lines = []
            stderr_lines = []

            # 创建线程来实时读取输出
            def read_stdout():
                for line in iter(process.stdout.readline, ''):
                    if line:
                        line = line.rstrip('\n')
                        stdout_lines.append(line)
                        # 为脚本输出添加时间戳和前缀
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        self.logger.info(f"📊 [{timestamp}] {line}")

            def read_stderr():
                for line in iter(process.stderr.readline, ''):
                    if line:
                        line = line.rstrip('\n')
                        stderr_lines.append(line)
                        # 错误输出用不同的前缀
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        self.logger.warning(f"⚠️  [{timestamp}] {line}")

            # 启动读取线程
            stdout_thread = threading.Thread(target=read_stdout, daemon=True)
            stderr_thread = threading.Thread(target=read_stderr, daemon=True)

            stdout_thread.start()
            stderr_thread.start()

            # 定期更新进度消息
            last_update = time.time()
            update_interval = 10  # 每10秒更新一次

            try:
                while process.poll() is None:
                    current_time = time.time()
                    elapsed = current_time - start_time

                    # 检查超时
                    if timeout and elapsed > timeout:
                        self.logger.warning(f"⏰ Test execution timeout ({timeout}s), terminating...")
                        process.terminate()
                        time.sleep(5)
                        if process.poll() is None:
                            process.kill()
                        break

                    # 定期更新进度消息
                    if current_time - last_update >= update_interval:
                        self.spinner.update_message(f"Test running for {elapsed:.0f}s...")
                        last_update = current_time

                    time.sleep(1)

                # 等待线程完成
                stdout_thread.join(timeout=2)
                stderr_thread.join(timeout=2)

            except KeyboardInterrupt:
                self.logger.warning("⚠️  Test execution interrupted by user")
                process.terminate()
                raise

            # 获取最终结果
            exit_code = process.returncode
            duration = time.time() - start_time

            # 停止进度指示器
            self.spinner.stop()

            # 合并输出
            stdout = '\n'.join(stdout_lines)
            stderr = '\n'.join(stderr_lines)

            # 解析测试指标
            metrics = self._parse_test_metrics(stdout, stderr)

            # 创建结果
            success = (exit_code == 0)
            result = TestExecutionResult(
                success=success,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                duration=duration,
                metrics=metrics
            )

            if success:
                self.logger.info(f"✅ Test script completed successfully in {duration:.2f}s")
            else:
                result.error_message = f"Script exited with code {exit_code}"
                self.logger.error(f"❌ Test script failed with exit code {exit_code} in {duration:.2f}s")

            return result

        except Exception as e:
            self.spinner.stop("❌ Test execution failed")
            duration = time.time() - start_time

            return TestExecutionResult(
                success=False,
                exit_code=-1,
                duration=duration,
                error_message=f"Script execution failed: {str(e)}"
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
            # 上传测试脚本到远程节点（带重试机制）
            remote_script_path = f"{node.work_dir}/test_script_{scenario.name}.sh"

            # 记录原始脚本信息用于验证
            original_size = script_path.stat().st_size
            self.logger.info(f"Uploading test script: {script_path} (size: {original_size} bytes) to {remote_script_path}")

            # 计算本地文件的 MD5 用于验证
            import hashlib
            with open(script_path, 'rb') as f:
                expected_hash = hashlib.md5(f.read()).hexdigest()

            # 重试上传机制
            max_upload_retries = 3
            upload_successful = False

            for upload_attempt in range(max_upload_retries):
                try:
                    if upload_attempt > 0:
                        self.logger.info(f"Upload retry attempt {upload_attempt + 1}/{max_upload_retries}")

                    # 如果是重试，先清理可能存在的损坏文件
                    if upload_attempt > 0:
                        cleanup_cmd = f"rm -f {remote_script_path}"
                        self.node_manager.execute_command(cleanup_cmd, [node_name], timeout=10)
                        time.sleep(1)  # 短暂延迟

                    upload_success = self.node_manager.upload_file(
                        str(script_path), remote_script_path, [node_name]
                    )

                    if not upload_success.get(node_name, False):
                        self.logger.warning(f"Upload failed for {node_name} (attempt {upload_attempt + 1})")
                        continue

                    # 验证上传的脚本文件
                    self.logger.info(f"Verifying uploaded script on {node_name} (attempt {upload_attempt + 1})")
                    verify_result = self._verify_remote_script(node_name, remote_script_path, original_size, expected_hash)

                    if verify_result["success"]:
                        upload_successful = True
                        self.logger.info(f"Script upload and verification successful on attempt {upload_attempt + 1}")
                        break
                    else:
                        self.logger.warning(f"Script verification failed on attempt {upload_attempt + 1}: {verify_result['error']}")

                except Exception as e:
                    self.logger.error(f"Upload attempt {upload_attempt + 1} failed with exception: {e}")

                # 如果不是最后一次尝试，等待一段时间再重试
                if upload_attempt < max_upload_retries - 1:
                    time.sleep(2)

            if not upload_successful:
                return TestExecutionResult(
                    success=False,
                    error_message=f"Failed to upload script to {node_name} after {max_upload_retries} attempts",
                    duration=time.time() - start_time
                )
            
            # 设置执行权限并运行脚本
            commands = [
                f"chmod +x {remote_script_path}",
                f"cd {node.work_dir}",
                f"export SCENARIO_NAME='{scenario.name}'",
                f"export SCENARIO_PATH='{node.work_dir}'",
                f"export SCENARIO_RESULT_PATH='{node.results_path}'",
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

    def _verify_remote_script(self, node_name: str, remote_script_path: str, expected_size: int, expected_hash: str = None) -> dict:
        """验证远程脚本文件的完整性"""
        try:
            # 第一层：检查文件是否存在且获取大小
            check_cmd = f"test -f {remote_script_path} && stat -f%z {remote_script_path} 2>/dev/null || stat -c%s {remote_script_path} 2>/dev/null"
            results = self.node_manager.execute_command(check_cmd, [node_name], timeout=10)

            node_result = results.get(node_name)
            if not node_result:
                return {"success": False, "error": "No response from node"}

            exit_code, stdout, stderr = node_result
            if exit_code != 0:
                return {"success": False, "error": f"File check failed: {stderr.strip()}"}

            try:
                remote_size = int(stdout.strip())
            except ValueError:
                return {"success": False, "error": f"Invalid file size response: {stdout.strip()}"}

            # 验证文件大小
            if remote_size != expected_size:
                return {
                    "success": False,
                    "error": f"File size mismatch: expected {expected_size}, got {remote_size}"
                }

            # 验证文件不为空
            if remote_size == 0:
                return {"success": False, "error": "Script file is empty"}

            self.logger.info(f"Size verification passed: {remote_size} bytes")

            # 第二层：MD5 哈希验证（如果提供了期望的哈希值）
            if expected_hash:
                hash_cmd = f"md5sum {remote_script_path} 2>/dev/null | cut -d' ' -f1 || openssl dgst -md5 {remote_script_path} | cut -d' ' -f2"
                hash_results = self.node_manager.execute_command(hash_cmd, [node_name], timeout=10)
                hash_result = hash_results.get(node_name)

                if hash_result:
                    hash_exit_code, hash_stdout, hash_stderr = hash_result
                    if hash_exit_code == 0:
                        remote_hash = hash_stdout.strip()
                        if remote_hash != expected_hash:
                            return {
                                "success": False,
                                "error": f"MD5 hash mismatch: expected {expected_hash}, got {remote_hash}"
                            }
                        self.logger.info(f"MD5 verification passed: {remote_hash}")
                    else:
                        self.logger.warning(f"Failed to calculate remote MD5: {hash_stderr}")

            # 第三层：脚本内容验证
            head_cmd = f"head -5 {remote_script_path}"
            head_results = self.node_manager.execute_command(head_cmd, [node_name], timeout=5)
            head_result = head_results.get(node_name)

            if head_result:
                head_exit_code, head_stdout, head_stderr = head_result
                if head_exit_code == 0:
                    first_lines = head_stdout.strip()
                    if len(first_lines) == 0:
                        return {"success": False, "error": "Script file appears to be empty"}

                    first_line = first_lines.split('\n')[0] if first_lines else ""

                    # 检查是否为有效的脚本文件
                    if not (first_line.startswith("#!/") or first_line.startswith("#") or len(first_line.strip()) > 0):
                        return {"success": False, "error": "Script file does not appear to be a valid script"}

                    self.logger.info(f"Script content verification passed: {remote_script_path}")
                    self.logger.info(f"  Size: {remote_size} bytes")
                    self.logger.info(f"  First line: {first_line[:100]}...")

                    return {
                        "success": True,
                        "size": remote_size,
                        "first_line": first_line,
                        "content_preview": first_lines[:200]
                    }
                else:
                    return {"success": False, "error": f"Failed to read script content: {head_stderr}"}

            return {"success": True, "size": remote_size}

        except Exception as e:
            return {"success": False, "error": f"Verification failed: {str(e)}"}