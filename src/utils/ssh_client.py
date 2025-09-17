"""
SSH客户端工具模块
提供SSH连接、命令执行、文件传输等功能
"""

import paramiko
import socket
import logging
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager
import time
import os


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
    
    def upload_file(self, local_path: str, remote_path: str) -> bool:
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
            
            self.sftp.put(local_path, remote_path)
            self.logger.info(f"Uploaded {local_path} to {remote_path}")
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
    
    def __init__(self, max_connections: int = 10):
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
        for client in self.connections.values():
            client.disconnect()
        self.connections.clear()
        self.logger.info("Closed all SSH connections")
    
    def get_connection_count(self) -> int:
        """获取活动连接数"""
        return len(self.connections)
    
    def get_connection_info(self) -> Dict[str, bool]:
        """获取连接信息"""
        return {key: client.is_connected() for key, client in self.connections.items()}


# 全局连接池实例
ssh_pool = SSHConnectionPool()