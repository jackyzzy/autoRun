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
from datetime import datetime, timedelta

from ..utils.ssh_client import SSHClient, ssh_pool, SSHConnectionError, SSHExecutionError
from ..utils.docker_compose_adapter import DockerComposeAdapterManager, ComposeCommand
from ..utils.common import (
    setup_module_logger, load_yaml_config, validate_required_fields,
    retry_on_failure, ContextTimer
)
from ..utils.exception_handler import (
    with_exception_handling, node_operation, safe_execute, exception_context
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

        # 连通性缓存配置
        self.connectivity_cache: Dict[str, Tuple[bool, datetime]] = {}
        self.connectivity_cache_ttl = 300  # 缓存TTL: 5分钟
        self.connectivity_cache_enabled = True

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
    
    @with_exception_handling("connectivity testing", return_on_error={})
    def test_connectivity(self, node_names: Optional[List[str]] = None, 
                         timeout: int = 10, force_refresh: bool = False) -> Dict[str, bool]:
        """测试节点连接性（支持缓存优化）
        
        Args:
            node_names: 要测试的节点名称列表
            timeout: 连接超时时间
            force_refresh: 是否强制刷新缓存
            
        Returns:
            节点名称到连接状态的映射
        """
        if node_names is None:
            node_names = list(self.nodes.keys())
        
        current_time = datetime.now()
        results = {}
        nodes_to_test = []
        
        # 检查缓存
        if self.connectivity_cache_enabled and not force_refresh:
            cache_hits = 0
            for node_name in node_names:
                if node_name in self.connectivity_cache:
                    cached_result, cached_time = self.connectivity_cache[node_name]
                    # 检查缓存是否过期
                    if (current_time - cached_time).total_seconds() < self.connectivity_cache_ttl:
                        results[node_name] = cached_result
                        cache_hits += 1
                        continue
                
                # 缓存未命中或已过期，加入待测试列表
                nodes_to_test.append(node_name)
            
            if cache_hits > 0:
                self.logger.debug(f"Connectivity cache hits: {cache_hits}/{len(node_names)} nodes")
        else:
            nodes_to_test = node_names
        
        # 对需要测试的节点进行连通性检查
        if nodes_to_test:
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
            with ThreadPoolExecutor(max_workers=min(len(nodes_to_test), 10)) as executor:
                future_to_node = {
                    executor.submit(test_node, node_name): node_name 
                    for node_name in nodes_to_test
                }
                
                for future in as_completed(future_to_node):
                    node_name, is_connected = future.result()
                    results[node_name] = is_connected
                    
                    # 更新缓存
                    if self.connectivity_cache_enabled:
                        self.connectivity_cache[node_name] = (is_connected, current_time)
        
        # 记录结果
        connected_count = sum(results.values())
        total_nodes = len(node_names)
        cache_used = len(node_names) - len(nodes_to_test)
        
        if cache_used > 0:
            self.logger.info(f"Connectivity test: {connected_count}/{total_nodes} nodes connected "
                           f"({cache_used} from cache, {len(nodes_to_test)} tested)")
        else:
            self.logger.info(f"Connectivity test: {connected_count}/{total_nodes} nodes connected")
        
        return results
    
    def execute_command(self, command: str, node_names: Optional[List[str]] = None,
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
    
    @with_exception_handling("file upload", return_on_error={})
    def upload_file(self, local_path: str, remote_path: str,
                   node_names: Optional[List[str]] = None) -> Dict[str, bool]:
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
                      node_names: Optional[List[str]] = None) -> Dict[str, bool]:
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
    
    @with_exception_handling("scenario env file upload", return_on_error={})
    def upload_scenario_env_file(self, env_file_path: str, scenario_name: str,
                                node_names: Optional[List[str]] = None) -> Dict[str, bool]:
        """增强版scenario环境变量文件上传
        
        Args:
            env_file_path: 本地环境变量文件路径
            scenario_name: scenario名称（用于日志记录）
            node_names: 目标节点名称列表，为空则上传到所有启用的节点
            
        Returns:
            节点名称到上传结果的映射
        """
        if not env_file_path or not Path(env_file_path).exists():
            self.logger.error(f"Environment file not found: {env_file_path}")
            return {}
        
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        # 记录本地文件信息
        local_file_info = Path(env_file_path).stat()
        self.logger.info(f"Uploading .env file for scenario '{scenario_name}': {env_file_path} ({local_file_info.st_size} bytes)")
        
        results = {}
        
        def upload_env_to_node(node_name: str) -> Tuple[str, bool]:
            start_time = time.time()
            
            try:
                # 详细步骤日志
                self.logger.info(f"[{node_name}] Step 1: Establishing connection...")
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                
                # 🔧 增强: 检查路径一致性并选择最佳上传路径
                self.logger.info(f"[{node_name}] Step 2: Determining target directory...")
                if node.work_dir != node.docker_compose_path:
                    self.logger.warning(f"Path inconsistency on {node_name}: work_dir({node.work_dir}) != docker_compose_path({node.docker_compose_path})")
                    self.logger.info(f"Using docker_compose_path for {node_name}: {node.docker_compose_path}")
                    target_dir = node.docker_compose_path
                else:
                    target_dir = node.work_dir
                
                remote_env_path = f"{target_dir}/.env"
                self.logger.info(f"[{node_name}] Target path: {remote_env_path}")
                
                with client.connection_context():
                    # 🔧 增强: 确保目标目录存在
                    self.logger.info(f"[{node_name}] Step 3: Preparing remote directory...")
                    mkdir_start = time.time()
                    mkdir_result = client.execute_command(f"mkdir -p {target_dir}", check_exit_code=False)
                    mkdir_duration = time.time() - mkdir_start
                    self.logger.info(f"[{node_name}] Directory creation: {mkdir_duration:.1f}s (exit_code: {mkdir_result[0]})")
                    
                    # 🔧 增强: 备份现有文件（如果存在）
                    self.logger.info(f"[{node_name}] Step 4: Backing up existing file...")
                    backup_start = time.time()
                    backup_cmd = f"test -f {remote_env_path} && cp {remote_env_path} {remote_env_path}.bak || true"
                    backup_result = client.execute_command(backup_cmd, check_exit_code=False)
                    backup_duration = time.time() - backup_start
                    self.logger.info(f"[{node_name}] Backup operation: {backup_duration:.1f}s (exit_code: {backup_result[0]})")
                    
                    # 上传环境变量文件
                    self.logger.info(f"[{node_name}] Step 5: Starting file upload...")
                    upload_start = time.time()
                    success = client.upload_file(env_file_path, remote_env_path)
                    upload_duration = time.time() - upload_start
                    
                    if success:
                        self.logger.info(f"[{node_name}] Upload completed in {upload_duration:.1f}s")
                        
                        # 设置正确的文件权限
                        self.logger.info(f"[{node_name}] Step 6: Setting file permissions...")
                        chmod_start = time.time()
                        chmod_result = client.execute_command(f"chmod 644 {remote_env_path}", check_exit_code=False)
                        chmod_duration = time.time() - chmod_start
                        self.logger.info(f"[{node_name}] Permission setting: {chmod_duration:.1f}s (exit_code: {chmod_result[0]})")
                        
                        # 🔧 增强: 立即验证上传结果
                        self.logger.info(f"[{node_name}] Step 7: Verifying upload...")
                        verify_start = time.time()
                        verify_result = client.execute_command(f"test -f {remote_env_path} && wc -c {remote_env_path}", check_exit_code=False)
                        verify_duration = time.time() - verify_start
                        
                        if verify_result[0] == 0:
                            remote_size = int(verify_result[1].strip().split()[0])
                            self.logger.info(f"[{node_name}] Size verification: {verify_duration:.1f}s (local: {local_file_info.st_size}, remote: {remote_size})")
                            
                            if remote_size == local_file_info.st_size:
                                # 🔧 增强: 验证文件可读性和内容
                                self.logger.info(f"[{node_name}] Step 8: Content verification...")
                                content_start = time.time()
                                content_check = client.execute_command(f"head -1 {remote_env_path}", check_exit_code=False)
                                content_duration = time.time() - content_start
                                
                                if content_check[0] == 0:
                                    first_line = content_check[1].strip()
                                    self.logger.info(f"[{node_name}] Content check: {content_duration:.1f}s")
                                    self.logger.debug(f"Remote .env first line on {node_name}: {first_line[:50]}...")
                                
                                total_duration = time.time() - start_time
                                self.logger.info(f"✅ [{node_name}] Upload successful in {total_duration:.1f}s total")
                                return node_name, True
                            else:
                                total_duration = time.time() - start_time
                                self.logger.error(f"❌ [{node_name}] File size mismatch after {total_duration:.1f}s: local={local_file_info.st_size}, remote={remote_size}")
                                return node_name, False
                        else:
                            total_duration = time.time() - start_time
                            self.logger.error(f"❌ [{node_name}] Failed to verify uploaded file after {total_duration:.1f}s")
                            return node_name, False
                    else:
                        total_duration = time.time() - start_time
                        self.logger.error(f"❌ [{node_name}] Upload failed after {upload_duration:.1f}s (total: {total_duration:.1f}s)")
                        return node_name, False
                        
            except Exception as e:
                total_duration = time.time() - start_time
                self.logger.error(f"❌ [{node_name}] Upload failed after {total_duration:.1f}s with exception: {e}")
                import traceback
                self.logger.debug(f"Full traceback: {traceback.format_exc()}")
                return node_name, False
        
        # 添加总体超时控制
        import signal
        import time
        
        def timeout_handler(signum, frame):
            self.logger.error("File upload timeout, forcing cleanup...")
            ssh_pool.close_all()
            raise TimeoutError("File upload timeout after 300 seconds")
        
        # 设置信号处理器（仅在支持的系统上）
        old_handler = None
        try:
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(300)  # 5分钟总超时
        except (AttributeError, OSError):
            self.logger.warning("Signal-based timeout not available, using manual monitoring")
        
        try:
            # 优化: 上传前强制清理SSH连接池
            self.logger.info("Clearing SSH connections before upload...")
            ssh_pool.close_all()
            
            # 优化: 减少并发度，避免连接冲突
            max_workers = min(len(node_names), 2)  # 从5减到2
            self.logger.debug(f"Using {max_workers} concurrent workers for upload")
            
            # 根据节点数量选择上传策略
            if len(node_names) <= 2:
                # 小规模顺序上传，更稳定
                self.logger.info("Using sequential upload for better reliability")
                for node_name in node_names:
                    try:
                        self.logger.info(f"Uploading to {node_name}...")
                        start_time = time.time()
                        result = upload_env_to_node(node_name)
                        duration = time.time() - start_time
                        results[result[0]] = result[1]
                        
                        if result[1]:
                            self.logger.info(f"✅ {node_name}: Upload completed in {duration:.1f}s")
                        else:
                            self.logger.error(f"❌ {node_name}: Upload failed after {duration:.1f}s")
                            # 失败时立即清理连接
                            self._cleanup_node_connection(node_name)
                            
                    except Exception as e:
                        self.logger.error(f"Upload failed for {node_name}: {e}")
                        results[node_name] = False
                        self._cleanup_node_connection(node_name)
            else:
                # 并发上传（节点较多时）
                self.logger.info(f"Using concurrent upload with {max_workers} workers")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_node = {
                        executor.submit(upload_env_to_node, node_name): node_name
                        for node_name in node_names
                    }
                    
                    for future in as_completed(future_to_node):
                        node_name, success = future.result()
                        results[node_name] = success
                        
                        if not success:
                            # 失败时清理连接
                            self._cleanup_node_connection(node_name)
                            
        finally:
            # 恢复信号处理器
            if old_handler is not None:
                try:
                    signal.alarm(0)  # 取消超时
                    signal.signal(signal.SIGALRM, old_handler)
                except (AttributeError, OSError):
                    pass
        
        success_count = sum(results.values())
        failed_nodes = [name for name, success in results.items() if not success]
        
        if success_count == len(node_names):
            self.logger.info(f"✅ Environment file upload for scenario '{scenario_name}': {success_count}/{len(node_names)} nodes succeeded")
        else:
            self.logger.error(f"❌ Environment file upload for scenario '{scenario_name}': {success_count}/{len(node_names)} nodes succeeded. Failed nodes: {failed_nodes}")
        
        return results
    
    def verify_env_file_on_nodes(self, scenario_name: str, node_names: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """验证节点上的环境变量文件是否存在且正确
        
        Args:
            scenario_name: scenario名称（用于日志记录）
            node_names: 要验证的节点名称列表
            
        Returns:
            节点名称到验证结果的映射
        """
        if node_names is None:
            node_names = self.get_node_names(enabled_only=True)
        
        results = {}
        
        def verify_env_on_node(node_name: str) -> Tuple[str, Dict[str, Any]]:
            result = {
                'exists': False,
                'readable': False,
                'size': 0,
                'error': None
            }
            
            try:
                node = self.nodes[node_name]
                client = node.get_ssh_client()
                
                remote_env_path = f"{node.work_dir}/.env"
                
                with client.connection_context():
                    # 检查文件是否存在
                    exit_code, _, _ = client.execute_command(f"test -f {remote_env_path}", check_exit_code=False)
                    result['exists'] = exit_code == 0
                    
                    if result['exists']:
                        # 检查文件是否可读
                        exit_code, _, _ = client.execute_command(f"test -r {remote_env_path}", check_exit_code=False)
                        result['readable'] = exit_code == 0
                        
                        # 获取文件大小
                        exit_code, stdout, _ = client.execute_command(f"stat -c %s {remote_env_path}", check_exit_code=False)
                        if exit_code == 0:
                            result['size'] = int(stdout.strip())
                    
            except Exception as e:
                result['error'] = str(e)
                self.logger.error(f"Failed to verify .env file on {node_name}: {e}")
            
            return node_name, result
        
        # 并发验证
        with ThreadPoolExecutor(max_workers=min(len(node_names), 5)) as executor:
            future_to_node = {
                executor.submit(verify_env_on_node, node_name): node_name
                for node_name in node_names
            }
            
            for future in as_completed(future_to_node):
                node_name, result = future.result()
                results[node_name] = result
        
        # 记录验证结果
        valid_count = sum(1 for r in results.values() if r['exists'] and r['readable'])
        self.logger.info(f"Environment file verification for scenario '{scenario_name}': {valid_count}/{len(node_names)} nodes have valid .env files")
        
        return results
    
    def get_node_status(self, node_names: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
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
    
    def _cleanup_node_connection(self, node_name: str):
        """清理指定节点的连接"""
        try:
            node = self.nodes.get(node_name)
            if node:
                client = node.get_ssh_client()
                if client:
                    client.disconnect()
                    self.logger.debug(f"Cleaned up connection for {node_name}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup connection for {node_name}: {e}")
    
    def close_all_connections(self):
        """关闭所有SSH连接"""
        ssh_pool.close_all()
        self.logger.info("Closed all SSH connections")
    
    def clear_connectivity_cache(self, node_names: Optional[List[str]] = None):
        """清理连通性缓存
        
        Args:
            node_names: 要清理的节点名称列表，为空则清理全部缓存
        """
        if node_names is None:
            self.connectivity_cache.clear()
            self.logger.info("Cleared all connectivity cache")
        else:
            for node_name in node_names:
                self.connectivity_cache.pop(node_name, None)
            self.logger.info(f"Cleared connectivity cache for nodes: {node_names}")
    
    def get_connectivity_cache_stats(self) -> Dict[str, Any]:
        """获取连通性缓存统计信息"""
        current_time = datetime.now()
        valid_entries = 0
        expired_entries = 0
        
        for node_name, (result, cache_time) in self.connectivity_cache.items():
            if (current_time - cache_time).total_seconds() < self.connectivity_cache_ttl:
                valid_entries += 1
            else:
                expired_entries += 1
        
        return {
            'enabled': self.connectivity_cache_enabled,
            'ttl_seconds': self.connectivity_cache_ttl,
            'total_entries': len(self.connectivity_cache),
            'valid_entries': valid_entries,
            'expired_entries': expired_entries,
            'cache_hit_potential': f"{(valid_entries / len(self.nodes) * 100):.1f}%" if self.nodes else "0%"
        }
    
    def set_connectivity_cache_ttl(self, ttl_seconds: int):
        """设置连通性缓存TTL
        
        Args:
            ttl_seconds: 缓存有效时间（秒）
        """
        old_ttl = self.connectivity_cache_ttl
        self.connectivity_cache_ttl = ttl_seconds
        self.logger.info(f"Updated connectivity cache TTL: {old_ttl}s -> {ttl_seconds}s")
    
    def enable_connectivity_cache(self, enabled: bool = True):
        """启用或禁用连通性缓存
        
        Args:
            enabled: 是否启用缓存
        """
        old_state = self.connectivity_cache_enabled
        self.connectivity_cache_enabled = enabled
        
        if not enabled:
            self.clear_connectivity_cache()
        
        self.logger.info(f"Connectivity cache: {'enabled' if enabled else 'disabled'} (was {'enabled' if old_state else 'disabled'})")
    
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
            'connection_pool_size': ssh_pool.get_connection_count(),
            'connectivity_cache': self.get_connectivity_cache_stats()
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

    def get_compose_version_info(self, node_names: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
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