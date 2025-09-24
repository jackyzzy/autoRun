"""节点管理器

管理多个分布式节点的SSH连接和操作，提供节点配置加载、连接管理、
命令执行等功能。支持节点池化管理和连接复用。

主要功能:
    - 从YAML配置文件加载节点信息
    - 管理SSH连接池和连接生命周期
    - 提供统一的远程命令执行接口
    - 支持并发操作多个节点
    - 集成Docker Compose版本适配
    - 节点健康状态监控和连接测试

典型用法:
    >>> manager = NodeManager("config/nodes.yaml")
    >>> nodes = manager.get_nodes(enabled_only=True)
    >>> results = manager.execute_command("ls /", [node.name for node in nodes])
    >>> connectivity = manager.test_connectivity([node.name for node in nodes])
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from pathlib import Path

from ..utils.ssh_client import SSHClient, ssh_pool, SSHConnectionError, SSHExecutionError
from ..utils.docker_compose_adapter import DockerComposeAdapterManager, ComposeCommand
from ..utils.common import (
    setup_module_logger, load_yaml_config, validate_required_fields,
    retry_on_failure, ContextTimer
)


class Node:
    """节点信息类

    封装单个节点的配置信息和连接参数。

    Attributes:
        name: 节点名称，用作唯一标识符
        host: 节点主机地址或IP
        username: SSH连接用户名
        password: SSH连接密码（可选）
        key_filename: SSH私钥文件路径（可选）
        port: SSH连接端口，默认22
        tags: 节点标签列表，用于分组和过滤
        role: 节点角色，如'master', 'worker'等
        enabled: 是否启用此节点
        docker_compose_path: Docker Compose文件路径
        work_dir: 工作目录路径
        env_vars: 环境变量映射

    Examples:
        >>> config = {
        ...     'host': '192.168.1.100',
        ...     'username': 'ubuntu',
        ...     'password': 'secret',
        ...     'enabled': True,
        ...     'tags': ['gpu', 'high-mem']
        ... }
        >>> node = Node('node1', config)
        >>> print(f"{node.name}: {node.host}")
        node1: 192.168.1.100
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.host = config['host']
        self.username = config['username']
        self.password = config.get('password')
        self.key_filename = config.get('key_filename')
        self.port = config.get('port', 22)
        self.tags = config.get('tags', [])
        self.role = config.get('role', 'worker')
        self.enabled = config.get('enabled', True)
        
        # 节点特定配置
        self.docker_compose_path = config.get('docker_compose_path', '/opt/inference')
        self.work_dir = config.get('work_dir', '/opt/inference')

        # Docker Compose版本配置（新增）
        self.docker_compose_version = config.get('docker_compose_version', 'auto')
        
    def get_ssh_client(self) -> SSHClient:
        """获取SSH客户端"""
        return ssh_pool.get_connection(
            self.host, self.username, self.password, 
            self.key_filename, self.port
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'host': self.host,
            'username': self.username,
            'port': self.port,
            'tags': self.tags,
            'role': self.role,
            'enabled': self.enabled,
            'docker_compose_path': self.docker_compose_path,
            'work_dir': self.work_dir,
            'docker_compose_version': self.docker_compose_version
        }


class NodeManager:
    """节点管理器"""
    
    def __init__(self, config_file: str = None):
        self.logger = setup_module_logger("playbook.node_manager")
        self.nodes: Dict[str, Node] = {}
        self.config_file = config_file

        # 初始化Docker Compose适配器管理器
        self.compose_adapter_manager = DockerComposeAdapterManager(self.execute_command)

        if config_file and Path(config_file).exists():
            self.load_config(config_file)
    
    def load_config(self, config_file: str):
        """加载节点配置"""
        with ContextTimer(self.logger, f"Loading node configuration from {config_file}"):
            config = load_yaml_config(config_file, required=True)

            self.nodes.clear()
            nodes_config = config.get('nodes', {})

            for node_name, node_config in nodes_config.items():
                # 验证必需字段
                validate_required_fields(
                    node_config,
                    ['host', 'username'],
                    f"node '{node_name}'"
                )

                node = Node(node_name, node_config)
                self.nodes[node_name] = node
                self.logger.debug(f"Loaded node: {node_name} ({node.host})")

            self.logger.info(f"Successfully loaded {len(self.nodes)} nodes")
    
    def add_node(self, name: str, host: str, username: str, **kwargs) -> Node:
        """添加节点"""
        config = {
            'host': host,
            'username': username,
            **kwargs
        }
        node = Node(name, config)
        self.nodes[name] = node
        self.logger.info(f"Added node: {name} ({host})")
        return node
    
    def remove_node(self, name: str) -> bool:
        """移除节点"""
        if name in self.nodes:
            del self.nodes[name]
            self.logger.info(f"Removed node: {name}")
            return True
        return False
    
    def get_node(self, name: str) -> Optional[Node]:
        """获取节点"""
        return self.nodes.get(name)
    
    def get_nodes(self, tags: List[str] = None, role: str = None, 
                  enabled_only: bool = True) -> List[Node]:
        """获取符合条件的节点列表"""
        nodes = []
        
        for node in self.nodes.values():
            # 检查是否启用
            if enabled_only and not node.enabled:
                continue
                
            # 检查标签
            if tags and not any(tag in node.tags for tag in tags):
                continue
                
            # 检查角色
            if role and node.role != role:
                continue
                
            nodes.append(node)
        
        return nodes
    
    def get_node_names(self, **kwargs) -> List[str]:
        """获取节点名称列表"""
        return [node.name for node in self.get_nodes(**kwargs)]
    
    def test_connectivity(self, node_names: List[str] = None, 
                         timeout: int = 10) -> Dict[str, bool]:
        """测试节点连接性"""
        if node_names is None:
            node_names = list(self.nodes.keys())
        
        results = {}
        
        def test_node(node_name: str) -> Tuple[str, bool]:
            try:
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                with client.connection_context():
                    # 执行简单命令测试连接
                    exit_code, _, _ = client.execute_command("echo 'test'", timeout=timeout)
                    return node_name, exit_code == 0
            except Exception as e:
                self.logger.error(f"Connectivity test failed for {node_name}: {e}")
                return node_name, False
        
        # 并发测试连接
        with ThreadPoolExecutor(max_workers=min(len(node_names), 10)) as executor:
            future_to_node = {
                executor.submit(test_node, node_name): node_name 
                for node_name in node_names
            }
            
            for future in as_completed(future_to_node):
                node_name, is_connected = future.result()
                results[node_name] = is_connected
        
        # 记录结果
        connected_count = sum(results.values())
        self.logger.info(f"Connectivity test: {connected_count}/{len(node_names)} nodes connected")
        
        return results
    
    def execute_command(self, command: str, node_names: List[str] = None,
                       parallel: bool = True, timeout: int = 300,
                       stop_on_error: bool = False) -> Dict[str, Tuple[int, str, str]]:
        """在节点上执行命令"""
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        results = {}
        
        def execute_on_node(node_name: str) -> Tuple[str, Tuple[int, str, str]]:
            try:
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                with client.connection_context():
                    result = client.execute_command(
                        command, timeout=timeout, check_exit_code=False
                    )
                    self.logger.info(f"Command executed on {node_name}: exit_code={result[0]}")
                    return node_name, result
            except Exception as e:
                self.logger.error(f"Command execution failed on {node_name}: {e}")
                return node_name, (-1, "", str(e))
        
        if parallel:
            # 并发执行
            with ThreadPoolExecutor(max_workers=min(len(node_names), 10)) as executor:
                future_to_node = {
                    executor.submit(execute_on_node, node_name): node_name
                    for node_name in node_names
                }
                
                for future in as_completed(future_to_node):
                    node_name, result = future.result()
                    results[node_name] = result
                    
                    if stop_on_error and result[0] != 0:
                        self.logger.error(f"Command failed on {node_name}, stopping execution")
                        # 取消未完成的任务
                        for f in future_to_node:
                            if not f.done():
                                f.cancel()
                        break
        else:
            # 顺序执行
            for node_name in node_names:
                node_name, result = execute_on_node(node_name)
                results[node_name] = result
                
                if stop_on_error and result[0] != 0:
                    self.logger.error(f"Command failed on {node_name}, stopping execution")
                    break
        
        return results
    
    def upload_file(self, local_path: str, remote_path: str,
                   node_names: List[str] = None) -> Dict[str, bool]:
        """上传文件到节点"""
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        results = {}
        
        def upload_to_node(node_name: str) -> Tuple[str, bool]:
            max_retries = 2
            retry_delay = 1

            for attempt in range(max_retries + 1):
                try:
                    node = self.nodes[node_name]
                    client = node.get_ssh_client()

                    if attempt > 0:
                        self.logger.info(f"Retrying file upload to {node_name} (attempt {attempt + 1})")
                        time.sleep(retry_delay)

                    with client.connection_context():
                        success = client.upload_file(local_path, remote_path)
                        if success:
                            if attempt > 0:
                                self.logger.info(f"File upload succeeded to {node_name} after {attempt + 1} attempts")
                            return node_name, True
                        else:
                            if attempt < max_retries:
                                self.logger.warning(f"File upload failed to {node_name}, will retry")
                            continue

                except Exception as e:
                    error_msg = str(e)
                    if attempt < max_retries:
                        self.logger.warning(f"File upload failed to {node_name} (attempt {attempt + 1}): {error_msg}, will retry")
                    else:
                        self.logger.error(f"File upload failed to {node_name} after {max_retries + 1} attempts: {error_msg}")

            return node_name, False
        
        # 并发上传
        with ThreadPoolExecutor(max_workers=min(len(node_names), 5)) as executor:
            future_to_node = {
                executor.submit(upload_to_node, node_name): node_name
                for node_name in node_names
            }
            
            for future in as_completed(future_to_node):
                node_name, success = future.result()
                results[node_name] = success
        
        success_count = sum(results.values())
        self.logger.info(f"File upload: {success_count}/{len(node_names)} nodes succeeded")
        
        return results
    
    def download_files(self, remote_path: str, local_base_path: str,
                      node_names: List[str] = None) -> Dict[str, bool]:
        """从节点下载文件"""
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        results = {}
        local_base = Path(local_base_path)
        local_base.mkdir(parents=True, exist_ok=True)
        
        def download_from_node(node_name: str) -> Tuple[str, bool]:
            try:
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                
                # 为每个节点创建子目录
                local_path = local_base / node_name / Path(remote_path).name
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                with client.connection_context():
                    success = client.download_file(remote_path, str(local_path))
                    return node_name, success
            except Exception as e:
                self.logger.error(f"File download failed from {node_name}: {e}")
                return node_name, False
        
        # 并发下载
        with ThreadPoolExecutor(max_workers=min(len(node_names), 5)) as executor:
            future_to_node = {
                executor.submit(download_from_node, node_name): node_name
                for node_name in node_names
            }
            
            for future in as_completed(future_to_node):
                node_name, success = future.result()
                results[node_name] = success
        
        success_count = sum(results.values())
        self.logger.info(f"File download: {success_count}/{len(node_names)} nodes succeeded")
        
        return results
    
    def get_node_status(self, node_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """获取节点状态信息"""
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        status_commands = [
            "uptime",
            "df -h /",
            "free -h",
            "docker ps --format 'table {{.Names}}\\t{{.Status}}\\t{{.Ports}}'",
        ]
        
        results = {}
        
        def get_status(node_name: str) -> Tuple[str, Dict[str, Any]]:
            try:
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                status = {'connected': False, 'info': {}}
                
                with client.connection_context():
                    status['connected'] = True
                    
                    for cmd in status_commands:
                        try:
                            exit_code, stdout, stderr = client.execute_command(
                                cmd, timeout=30, check_exit_code=False
                            )
                            if exit_code == 0:
                                status['info'][cmd] = stdout.strip()
                            else:
                                status['info'][cmd] = f"Error: {stderr.strip()}"
                        except Exception as e:
                            status['info'][cmd] = f"Failed: {str(e)}"
                
                return node_name, status
                
            except Exception as e:
                self.logger.error(f"Failed to get status for {node_name}: {e}")
                return node_name, {'connected': False, 'error': str(e)}
        
        # 并发获取状态
        with ThreadPoolExecutor(max_workers=min(len(node_names), 10)) as executor:
            future_to_node = {
                executor.submit(get_status, node_name): node_name
                for node_name in node_names
            }
            
            for future in as_completed(future_to_node):
                node_name, status = future.result()
                results[node_name] = status
        
        return results
    
    def close_all_connections(self):
        """关闭所有SSH连接"""
        ssh_pool.close_all()
        self.logger.info("Closed all SSH connections")
    
    def get_summary(self) -> Dict[str, Any]:
        """获取节点管理器摘要信息"""
        total_nodes = len(self.nodes)
        enabled_nodes = len(self.get_nodes(enabled_only=True))
        
        roles = {}
        tags = set()
        
        for node in self.nodes.values():
            roles[node.role] = roles.get(node.role, 0) + 1
            tags.update(node.tags)
        
        return {
            'total_nodes': total_nodes,
            'enabled_nodes': enabled_nodes,
            'disabled_nodes': total_nodes - enabled_nodes,
            'roles': roles,
            'tags': sorted(list(tags)),
            'connection_pool_size': ssh_pool.get_connection_count()
        }

    def build_compose_command(self, node_name: str, command_type: str, **kwargs) -> ComposeCommand:
        """
        为指定节点构建Docker Compose命令

        Args:
            node_name: 节点名称
            command_type: 命令类型 (up/down/stop/logs等)
            **kwargs: 命令参数

        Returns:
            构建的命令对象
        """
        node = self.get_node(node_name)
        if not node:
            raise ValueError(f"Node not found: {node_name}")

        return self.compose_adapter_manager.build_command(
            node_name=node_name,
            command_type=command_type,
            forced_version=node.docker_compose_version,
            **kwargs
        )

    def get_compose_version_info(self, node_names: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        获取节点的Docker Compose版本信息

        Args:
            node_names: 节点名称列表，为空则获取所有节点信息

        Returns:
            节点版本信息字典
        """
        if node_names is None:
            node_names = list(self.nodes.keys())

        # 构建强制版本映射
        forced_versions = {}
        for node_name in node_names:
            node = self.get_node(node_name)
            if node:
                forced_versions[node_name] = node.docker_compose_version

        return self.compose_adapter_manager.detect_all_versions(node_names, forced_versions)

    def execute_compose_command(self, node_names: List[str], command_type: str,
                               timeout: int = 300, **kwargs) -> Dict[str, Tuple[int, str, str]]:
        """
        在多个节点上执行Docker Compose命令

        Args:
            node_names: 节点名称列表
            command_type: 命令类型
            timeout: 超时时间
            **kwargs: 命令参数

        Returns:
            执行结果字典
        """
        results = {}

        for node_name in node_names:
            try:
                node = self.get_node(node_name)
                if not node:
                    results[node_name] = (-1, "", f"Node not found: {node_name}")
                    continue

                # 构建适配的命令
                compose_cmd = self.build_compose_command(node_name, command_type, **kwargs)

                # 切换到工作目录并执行
                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"

                self.logger.info(f"Executing compose command on {node_name}: {compose_cmd.full_cmd}")
                cmd_results = self.execute_command(full_cmd, [node_name], timeout=timeout)

                node_result = cmd_results.get(node_name, (-1, "", "No result"))
                results[node_name] = node_result

                if node_result[0] == 0:
                    self.logger.info(f"Compose command succeeded on {node_name}")
                else:
                    self.logger.error(f"Compose command failed on {node_name}: {node_result[2]}")

            except Exception as e:
                self.logger.error(f"Error executing compose command on {node_name}: {e}")
                results[node_name] = (-1, "", str(e))

        return results