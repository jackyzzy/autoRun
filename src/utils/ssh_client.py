"""
SSH客户端工具模块
提供SSH连接、命令执行、文件传输等功能
"""

import paramiko
import socket
import logging
import hashlib
import threading
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager
import time
import os

# 尝试导入 SCPClient（可选依赖）
try:
    from scp import SCPClient
    _HAS_SCP = True
except Exception:
    SCPClient = None
    _HAS_SCP = False


class SSHConnectionError(Exception):
    """SSH连接错误"""
    pass


class SSHExecutionError(Exception):
    """SSH命令执行错误"""
    pass


class SSHClient:
    """SSH客户端类"""
    
    def __init__(self, host: str, username: str, password: str = None, 
                 key_filename: str = None, port: int = 22, timeout: int = 30):
        self.host = host
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.port = port
        self.timeout = timeout
        self.client: Optional[paramiko.SSHClient] = None
        self.sftp: Optional[paramiko.SFTPClient] = None
        self.logger = logging.getLogger(f"ssh.{host}")
        
    def connect(self) -> bool:
        """建立SSH连接"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_params = {
                'hostname': self.host,
                'username': self.username,
                'port': self.port,
                'timeout': self.timeout
            }
            
            if self.password:
                connect_params['password'] = self.password
            if self.key_filename and os.path.exists(self.key_filename):
                connect_params['key_filename'] = self.key_filename
                
            self.client.connect(**connect_params)
            self.logger.info(f"Successfully connected to {self.host}:{self.port}")
            return True
            
        except (paramiko.AuthenticationException, paramiko.SSHException, 
                socket.error, Exception) as e:
            self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
            raise SSHConnectionError(f"Cannot connect to {self.host}:{self.port}: {e}")
    
    def disconnect(self):
        """关闭SSH连接"""
        if self.sftp:
            self.sftp.close()
            self.sftp = None
        if self.client:
            self.client.close()
            self.client = None
        self.logger.info(f"Disconnected from {self.host}")
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        if not self.client:
            return False
        try:
            transport = self.client.get_transport()
            return transport and transport.is_active()
        except:
            return False
    
    def execute_command(self, command: str, timeout: int = 300, 
                       check_exit_code: bool = True) -> Tuple[int, str, str]:
        """
        执行SSH命令
        
        Args:
            command: 要执行的命令
            timeout: 超时时间（秒）
            check_exit_code: 是否检查退出码
            
        Returns:
            Tuple[exit_code, stdout, stderr]
        """
        if not self.is_connected():
            self.connect()
            
        try:
            self.logger.info(f"Executing command: {command}")
            stdin, stdout, stderr = self.client.exec_command(
                command, timeout=timeout, get_pty=True
            )
            
            # 等待命令完成
            exit_code = stdout.channel.recv_exit_status()
            
            stdout_data = stdout.read().decode('utf-8', errors='ignore')
            stderr_data = stderr.read().decode('utf-8', errors='ignore')

            # 记录命令执行结果的详细信息
            self.logger.info(f"Command completed with exit code: {exit_code}")

            # 如果有输出内容，记录到日志中
            if stdout_data.strip():
                self.logger.info(f"Command stdout: {stdout_data.strip()}")
            if stderr_data.strip():
                self.logger.warning(f"Command stderr: {stderr_data.strip()}")

            if check_exit_code and exit_code != 0:
                # 构建更详细的错误信息
                error_parts = [f"Command failed with exit code {exit_code}"]
                if stderr_data.strip():
                    error_parts.append(f"stderr: {stderr_data.strip()}")
                if stdout_data.strip():
                    error_parts.append(f"stdout: {stdout_data.strip()}")

                error_msg = "; ".join(error_parts)
                self.logger.error(error_msg)
                raise SSHExecutionError(error_msg)
                
            return exit_code, stdout_data, stderr_data
            
        except Exception as e:
            self.logger.error(f"Command execution failed: {e}")
            raise SSHExecutionError(f"Failed to execute command '{command}': {e}")
    
    def execute_commands(self, commands: List[str], timeout: int = 300,
                        stop_on_error: bool = True) -> List[Tuple[int, str, str]]:
        """
        执行多个命令
        
        Args:
            commands: 命令列表
            timeout: 超时时间
            stop_on_error: 遇到错误是否停止
            
        Returns:
            List[Tuple[exit_code, stdout, stderr]]
        """
        results = []
        for i, command in enumerate(commands):
            try:
                result = self.execute_command(command, timeout, check_exit_code=False)
                results.append(result)
                
                if stop_on_error and result[0] != 0:
                    self.logger.error(f"Command {i+1} failed, stopping execution")
                    break
                    
            except Exception as e:
                self.logger.error(f"Failed to execute command {i+1}: {e}")
                if stop_on_error:
                    raise
                results.append((-1, "", str(e)))
                
        return results

    def calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> str:
        """计算文件哈希值

        Args:
            file_path: 文件路径
            algorithm: 哈希算法 ('md5', 'sha256')

        Returns:
            文件的哈希值
        """
        try:
            hash_obj = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            self.logger.error(f"Failed to calculate {algorithm} hash for {file_path}: {e}")
            raise SSHExecutionError(f"Hash calculation failed: {e}")

    def _cleanup_failed_upload(self, remote_path: str) -> None:
        """清理上传失败的远程文件"""
        try:
            if self.sftp:
                self.sftp.remove(remote_path)
                self.logger.info(f"Cleaned up corrupted remote file: {remote_path}")
        except Exception as cleanup_error:
            self.logger.warning(f"Failed to cleanup corrupted remote file {remote_path}: {cleanup_error}")

    def _sftp_put_with_timeout(self, local_path: str, remote_path: str, timeout: int = 120) -> None:
        """使用线程包装sftp.put以实现超时控制。

        如果在timeout秒内未完成，则尝试关闭transport以中断阻塞的sftp操作。
        该函数在失败时会抛出异常。
        """
        exception_holder = {}

        def target():
            try:
                # sftp.put在网络问题或对端无响应时可能阻塞
                self.sftp.put(local_path, remote_path)
            except Exception as e:
                exception_holder['exc'] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            # 超时：尝试通过关闭transport来中断阻塞的sftp调用
            self.logger.error(f"sftp.put timed out after {timeout}s, attempting to abort by closing transport")
            try:
                if self.client:
                    transport = self.client.get_transport()
                    if transport:
                        transport.close()
                        self.logger.debug("Closed transport to abort stalled sftp.put")
            except Exception as close_err:
                self.logger.warning(f"Error while closing transport after sftp.put timeout: {close_err}")

            # 等待线程短暂退出
            thread.join(2)

            # 清理sftp引用，因为transport已关闭
            try:
                if self.sftp:
                    self.sftp.close()
            except Exception:
                pass
            self.sftp = None

            raise SSHExecutionError(f"sftp.put timed out after {timeout} seconds and transport was closed")

        # 线程已结束，检查是否捕获到异常
        if 'exc' in exception_holder:
            raise exception_holder['exc']

    def _scp_put_with_timeout(self, local_path: str, remote_path: str, timeout: int = 120) -> None:
        """使用 SCPClient 上传文件并提供超时保护。

        要求安装 `scp` 包（pip install scp）。如果未安装，会抛出 SSHExecutionError 指示用户安装。
        """
        if not _HAS_SCP or SCPClient is None:
            raise SSHExecutionError("scp upload requires the 'scp' package. Install with: pip install scp")

        exception_holder = {}

        def target():
            scp = None
            try:
                # 通过 paramiko transport 创建 SCPClient
                transport = self.client.get_transport()
                scp = SCPClient(transport)
                # remote_path 可能需要指定目录权限，SCPClient.put 支持远程路径
                scp.put(local_path, remote_path)
            except Exception as e:
                exception_holder['exc'] = e
            finally:
                try:
                    if scp:
                        scp.close()
                except Exception:
                    pass

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            self.logger.error(f"scp.put timed out after {timeout}s, attempting to abort by closing transport")
            try:
                if self.client:
                    transport = self.client.get_transport()
                    if transport:
                        transport.close()
                        self.logger.debug("Closed transport to abort stalled scp.put")
            except Exception as close_err:
                self.logger.warning(f"Error while closing transport after scp.put timeout: {close_err}")

            thread.join(2)

            # 清理sftp引用（scp使用相同transport）
            try:
                if self.sftp:
                    self.sftp.close()
            except Exception:
                pass
            self.sftp = None

            raise SSHExecutionError(f"scp.put timed out after {timeout} seconds and transport was closed")

        if 'exc' in exception_holder:
            raise exception_holder['exc']

    def upload_file(self, local_path: str, remote_path: str, method: str = 'scp', timeout: int = 120) -> bool:
        """上传文件到远程服务器"""
        if not self.is_connected():
            self.connect()
            
        try:
            if not self.sftp:
                self.sftp = self.client.open_sftp()
                
            # 确保远程目录存在
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                self.execute_command(f"mkdir -p {remote_dir}", check_exit_code=False)
            
            self.logger.info(f"Uploading {local_path} to {remote_path} start ...")
            # 上传文件：支持 'sftp'（默认）和 'scp'
            try:
                if method.lower() == 'sftp':
                    # 使用带超时的sftp put包装器，避免长时间阻塞
                    self._sftp_put_with_timeout(local_path, remote_path, timeout=timeout)
                elif method.lower() == 'scp':
                    # 使用scp上传（需要安装 python-scp 包）
                    self._scp_put_with_timeout(local_path, remote_path, timeout=timeout)
                else:
                    raise SSHExecutionError(f"Unknown upload method: {method}")
            except Exception as put_err:
                # 如果发生超时或底层错误，确保远端残留文件被移除并重新抛出为SSHExecutionError
                self.logger.error(f"file upload failed ({method}): {put_err}")
                try:
                    self._cleanup_failed_upload(remote_path)
                except Exception:
                    pass
                # 如果put_err不是SSHExecutionError，统一包装
                if isinstance(put_err, SSHExecutionError):
                    raise
                else:
                    raise SSHExecutionError(f"Failed during {method} upload: {put_err}")

            # 分层验证上传结果
            try:
                # 第一层：快速大小检查
                local_size = os.path.getsize(local_path)
                remote_stat = self.sftp.stat(remote_path)
                remote_size = remote_stat.st_size

                if local_size != remote_size:
                    self.logger.error(f"File size mismatch after upload: local={local_size}, remote={remote_size}")
                    self._cleanup_failed_upload(remote_path)
                    raise SSHExecutionError(f"File upload verification failed: size mismatch (local={local_size}, remote={remote_size})")

                self.logger.info(f"Size verification passed: {local_size} bytes")

                # 第二层：MD5 哈希完整性验证
                local_hash = self.calculate_file_hash(local_path, 'md5')

                # 在远程计算 MD5
                hash_cmd = f"md5sum {remote_path} 2>/dev/null | cut -d' ' -f1 || openssl dgst -md5 {remote_path} | cut -d' ' -f2"
                exit_code, remote_hash_output, stderr = self.execute_command(hash_cmd, timeout=30, check_exit_code=False)

                if exit_code != 0:
                    self.logger.warning(f"Failed to calculate remote MD5, fallback to basic verification: {stderr}")
                    # 回退到基本的可读性检查
                    try:
                        with self.sftp.open(remote_path, 'r') as remote_file:
                            remote_file.read(100)
                        self.logger.info(f"Upload verification: file {remote_path} appears readable")
                    except Exception as read_error:
                        self.logger.error(f"Upload verification failed: cannot read remote file {remote_path}: {read_error}")
                        self._cleanup_failed_upload(remote_path)
                        raise SSHExecutionError(f"File upload failed: remote file is not readable: {read_error}")
                else:
                    remote_hash = remote_hash_output.strip()

                    if local_hash != remote_hash:
                        self.logger.error(f"MD5 hash mismatch after upload:")
                        self.logger.error(f"  Local MD5:  {local_hash}")
                        self.logger.error(f"  Remote MD5: {remote_hash}")
                        self._cleanup_failed_upload(remote_path)
                        raise SSHExecutionError(f"File upload verification failed: MD5 mismatch (local={local_hash}, remote={remote_hash})")

                    self.logger.info(f"MD5 verification passed: {local_hash}")

                self.logger.info(f"Successfully uploaded {local_path} to {remote_path} (size: {local_size} bytes)")

            except SSHExecutionError:
                # 重新抛出验证错误
                raise
            except Exception as e:
                self.logger.error(f"Upload verification failed with exception: {e}")
                self._cleanup_failed_upload(remote_path)
                raise SSHExecutionError(f"File upload verification failed: {e}")

            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e}")
            raise SSHExecutionError(f"Failed to upload {local_path}: {e}")
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """从远程服务器下载文件"""
        if not self.is_connected():
            self.connect()
            
        try:
            if not self.sftp:
                self.sftp = self.client.open_sftp()
                
            # 确保本地目录存在
            local_dir = os.path.dirname(local_path)
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)
            
            self.sftp.get(remote_path, local_path)
            self.logger.info(f"Downloaded {remote_path} to {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download file: {e}")
            raise SSHExecutionError(f"Failed to download {remote_path}: {e}")
    
    def file_exists(self, remote_path: str) -> bool:
        """检查远程文件是否存在"""
        try:
            exit_code, _, _ = self.execute_command(
                f"test -f {remote_path}", check_exit_code=False
            )
            return exit_code == 0
        except:
            return False
    
    def directory_exists(self, remote_path: str) -> bool:
        """检查远程目录是否存在"""
        try:
            exit_code, _, _ = self.execute_command(
                f"test -d {remote_path}", check_exit_code=False
            )
            return exit_code == 0
        except:
            return False
    
    @contextmanager
    def connection_context(self):
        """SSH连接上下文管理器"""
        try:
            if not self.is_connected():
                self.connect()
            yield self
        finally:
            pass  # 保持连接，由连接池管理
    
    def __enter__(self):
        if not self.is_connected():
            self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # 保持连接，由连接池管理
    
    def __del__(self):
        self.disconnect()


class SSHConnectionPool:
    """SSH连接池"""
    
    def __init__(self, max_connections: int = 15):  # 从10增加到15
        self.max_connections = max_connections
        self.connections: Dict[str, SSHClient] = {}
        self.logger = logging.getLogger("ssh.pool")
    
    def get_connection(self, host: str, username: str, password: str = None,
                      key_filename: str = None, port: int = 22) -> SSHClient:
        """获取SSH连接"""
        connection_key = f"{username}@{host}:{port}"
        
        if connection_key in self.connections:
            client = self.connections[connection_key]
            if client.is_connected():
                return client
            else:
                # 连接断开，重新连接
                try:
                    client.connect()
                    return client
                except:
                    # 连接失败，创建新的连接
                    del self.connections[connection_key]
        
        # 创建新连接
        if len(self.connections) >= self.max_connections:
            # 清理断开的连接
            self._cleanup_connections()
            
            if len(self.connections) >= self.max_connections:
                # 移除最旧的连接
                oldest_key = next(iter(self.connections))
                self.connections[oldest_key].disconnect()
                del self.connections[oldest_key]
        
        client = SSHClient(host, username, password, key_filename, port)
        client.connect()
        self.connections[connection_key] = client
        
        self.logger.info(f"Created new SSH connection to {connection_key}")
        return client
    
    def _cleanup_connections(self):
        """清理断开的连接"""
        disconnected_keys = []
        for key, client in self.connections.items():
            if not client.is_connected():
                disconnected_keys.append(key)
        
        for key in disconnected_keys:
            self.connections[key].disconnect()
            del self.connections[key]
    
    def close_all(self):
        """关闭所有连接"""
        connection_count_before = len(self.connections)
        if connection_count_before == 0:
            self.logger.debug("No SSH connections to close")
            return

        self.logger.info(f"Closing {connection_count_before} SSH connections...")

        # 记录关闭前的连接详情
        for key, client in self.connections.items():
            try:
                is_connected = client.is_connected()
                self.logger.debug(f"Closing connection: {key} (connected: {is_connected})")
                client.disconnect()
            except Exception as e:
                self.logger.warning(f"Error closing connection {key}: {e}")

        self.connections.clear()
        self.logger.info(f"Successfully closed {connection_count_before} SSH connections")
    
    def get_connection_count(self) -> int:
        """获取活动连接数"""
        return len(self.connections)
    
    def get_connection_info(self) -> Dict[str, bool]:
        """获取连接信息"""
        return {key: client.is_connected() for key, client in self.connections.items()}

    def _force_disconnect_remaining_connections(self):
        """强制断开残留连接"""
        for key, client in list(self.connections.items()):
            try:
                if client.client:
                    transport = client.client.get_transport()
                    if transport:
                        transport.close()
                client.disconnect()
                del self.connections[key]
                self.logger.debug(f"Force disconnected: {key}")
            except Exception as e:
                self.logger.warning(f"Error force disconnecting {key}: {e}")

    def force_cleanup_with_verification(self) -> Dict[str, Any]:
        """强制清理连接池并验证结果（增强版）

        Returns:
            Dict containing cleanup results and verification status
        """
        cleanup_result = {
            'initial_connections': self.get_connection_count(),
            'cleanup_successful': False,
            'final_connections': 0,
            'errors': [],
            'verification_passed': False,
            'retry_attempts': 0  # 新增：记录重试次数
        }

        try:
            self.logger.info("🔧 Starting force cleanup with verification...")

            # 记录初始状态
            initial_info = self.get_connection_info()
            cleanup_result['initial_connection_details'] = initial_info

            # 增强部分：重试机制
            max_retries = 3
            for attempt in range(max_retries):
                cleanup_result['retry_attempts'] = attempt + 1

                # 执行强制清理
                self.close_all()

                # 增加等待时间（从0.2秒增加到渐进式等待）
                wait_time = 0.5 + (attempt * 1.0)  # 0.5s, 1.5s, 2.5s
                import time
                time.sleep(wait_time)

                # 验证清理结果
                final_count = self.get_connection_count()
                cleanup_result['final_connections'] = final_count

                if final_count == 0:
                    cleanup_result['cleanup_successful'] = True
                    cleanup_result['verification_passed'] = True
                    self.logger.info(f"✅ Force cleanup verification passed on attempt {attempt + 1}")
                    break
                else:
                    if attempt < max_retries - 1:
                        remaining_connections = self.get_connection_info()
                        self.logger.warning(f"⚠️ Attempt {attempt + 1}: {final_count} connections remain, retrying...")
                        # 强制断开残留连接
                        self._force_disconnect_remaining_connections()
                    else:
                        # 最后一次尝试失败
                        cleanup_result['cleanup_successful'] = True  # 清理动作完成了
                        cleanup_result['verification_passed'] = False  # 但验证失败
                        remaining_connections = self.get_connection_info()
                        cleanup_result['remaining_connections'] = remaining_connections
                        self.logger.error(f"❌ Force cleanup failed after {max_retries} attempts - {final_count} connections remain")

        except Exception as e:
            cleanup_result['errors'].append(str(e))
            self.logger.error(f"Force cleanup failed: {e}")

        return cleanup_result

    def get_detailed_status(self) -> Dict[str, Any]:
        """获取连接池的详细状态信息"""
        total_connections = self.get_connection_count()
        connection_info = self.get_connection_info()

        connected_count = sum(1 for is_connected in connection_info.values() if is_connected)
        disconnected_count = total_connections - connected_count

        return {
            'total_connections': total_connections,
            'connected_count': connected_count,
            'disconnected_count': disconnected_count,
            'max_connections': self.max_connections,
            'connection_details': connection_info,
            'pool_utilization': f"{total_connections}/{self.max_connections}"
        }


# 全局连接池实例
ssh_pool = SSHConnectionPool()