"""Docker Compose版本适配器

这个模块提供了自动检测并适配不同版本的Docker Compose命令的功能。
支持Docker Compose V1 (docker-compose) 和 V2 (docker compose) 的自动切换。

主要功能:
    - 自动检测节点上的Docker Compose版本
    - 适配不同版本的命令格式
    - 缓存检测结果以提高性能
    - 支持强制指定版本
    - 统一的命令构建接口

典型用法:
    >>> adapter = DockerComposeAdapter("node1", ssh_executor_func)
    >>> cmd = adapter.build_command("up", file="docker-compose.yml", services=["app"])
    >>> print(cmd.full_cmd)
    'docker compose -f docker-compose.yml up -d app'
"""

import logging
from typing import Dict, Optional, Any, Tuple
from enum import Enum
from dataclasses import dataclass
from pathlib import Path
import time


class DockerComposeVersion(Enum):
    """Docker Compose版本枚举

    定义支持的Docker Compose版本类型。

    Attributes:
        V1: Docker Compose V1 版本 (docker-compose 命令)
        V2: Docker Compose V2 版本 (docker compose 命令)
        UNKNOWN: 未知版本或检测失败
        AUTO: 自动检测版本模式
    """
    V1 = "v1"  # docker-compose
    V2 = "v2"  # docker compose
    UNKNOWN = "unknown"
    AUTO = "auto"  # 自动检测


@dataclass
class ComposeCommand:
    """Docker Compose命令封装

    封装构建好的Docker Compose命令及其相关信息。

    Attributes:
        base_cmd: 基础命令 (如 'docker-compose' 或 'docker compose')
        full_cmd: 完整的命令字符串
        version: 使用的Docker Compose版本

    Examples:
        >>> cmd = ComposeCommand(
        ...     base_cmd="docker compose",
        ...     full_cmd="docker compose -f app.yml up -d",
        ...     version=DockerComposeVersion.V2
        ... )
        >>> str(cmd)
        'docker compose -f app.yml up -d'
    """
    base_cmd: str
    full_cmd: str
    version: DockerComposeVersion

    def __str__(self) -> str:
        return self.full_cmd


class DockerComposeAdapter:
    """Docker Compose版本适配器

    为单个节点提供Docker Compose版本检测和命令构建功能。
    支持自动检测节点上的Docker Compose版本并生成相应格式的命令。

    该类会缓存检测结果以提高性能，并支持强制指定版本。
    当检测失败时会采用降级策略。

    Attributes:
        node_name: 目标节点名称
        ssh_executor: SSH命令执行函数
        forced_version: 强制指定的版本
        cache_ttl: 缓存有效期（秒）

    Examples:
        >>> def ssh_executor(cmd, nodes, **kwargs):
        ...     # 模拟SSH执行
        ...     return {nodes[0]: (0, "Docker Compose version v2.10.0", "")}
        >>>
        >>> adapter = DockerComposeAdapter("node1", ssh_executor)
        >>> version = adapter.detect_version()
        >>> print(version)
        DockerComposeVersion.V2
        >>>
        >>> cmd = adapter.build_command("up", file="app.yml", services=["web"])
        >>> print(cmd.full_cmd)
        'docker compose -f app.yml up -d web'
    """

    # 命令模板映射
    COMMAND_TEMPLATES = {
        DockerComposeVersion.V1: {
            "base_cmd": "docker-compose",
            "up": "docker-compose{env_file} -f {file} up -d {services}",
            "down": "docker-compose{env_file} -f {file} down",
            "stop": "docker-compose{env_file} -f {file} stop {services}",
            "rm": "docker-compose{env_file} -f {file} rm -f {services}",
            "logs": "docker-compose{env_file} -f {file} logs --tail {lines} {services}",
            "scale": "docker-compose{env_file} -f {file} up -d --scale {service}={replicas}",
            "version": "docker-compose version",
            "ps": "docker-compose{env_file} -f {file} ps",
            "exec": "docker-compose{env_file} -f {file} exec {service} {command}",
            "pull": "docker-compose{env_file} -f {file} pull {services}",
            "build": "docker-compose{env_file} -f {file} build {services}",
            "restart": "docker-compose{env_file} -f {file} restart {services}",
            "pause": "docker-compose{env_file} -f {file} pause {services}",
            "unpause": "docker-compose{env_file} -f {file} unpause {services}"
        },
        DockerComposeVersion.V2: {
            "base_cmd": "docker compose",
            "up": "docker compose{env_file} -f {file} up -d {services}",
            "down": "docker compose{env_file} -f {file} down",
            "stop": "docker compose{env_file} -f {file} stop {services}",
            "rm": "docker compose{env_file} -f {file} rm -f {services}",
            "logs": "docker compose{env_file} -f {file} logs --tail {lines} {services}",
            "scale": "docker compose{env_file} -f {file} up -d --scale {service}={replicas}",
            "version": "docker compose version",
            "ps": "docker compose{env_file} -f {file} ps",
            "exec": "docker compose{env_file} -f {file} exec {service} {command}",
            "pull": "docker compose{env_file} -f {file} pull {services}",
            "build": "docker compose{env_file} -f {file} build {services}",
            "restart": "docker compose{env_file} -f {file} restart {services}",
            "pause": "docker compose{env_file} -f {file} pause {services}",
            "unpause": "docker compose{env_file} -f {file} unpause {services}"
        }
    }

    def __init__(self, node_name: str, ssh_executor_func,
                 forced_version: str = "auto", cache_ttl: int = 3600):
        """
        初始化适配器

        Args:
            node_name: 节点名称
            ssh_executor_func: SSH命令执行函数，签名为 (cmd, node_names, **kwargs) -> Dict[str, Tuple[int, str, str]]
            forced_version: 强制指定版本 (auto/v1/v2)
            cache_ttl: 缓存有效期（秒）
        """
        self.node_name = node_name
        self.ssh_executor = ssh_executor_func
        self.forced_version = DockerComposeVersion(forced_version) if forced_version != "auto" else DockerComposeVersion.AUTO
        self.cache_ttl = cache_ttl

        # 缓存检测结果
        self._cached_version: Optional[DockerComposeVersion] = None
        self._cache_timestamp: float = 0

        self.logger = logging.getLogger(f"docker_compose_adapter.{node_name}")

    def detect_version(self, force_refresh: bool = False) -> DockerComposeVersion:
        """检测Docker Compose版本

        通过执行版本命令来检测节点上安装的Docker Compose版本。
        优先检测V2版本，然后检测V1版本。检测结果会被缓存。

        Args:
            force_refresh: 是否强制重新检测，忽略缓存。默认为False。

        Returns:
            检测到的Docker Compose版本。如果检测失败返回UNKNOWN。

        Note:
            - 如果设置了forced_version且不是AUTO，直接返回强制版本
            - 检测结果会缓存cache_ttl秒，避免重复检测
            - 优先级: V2 > V1 > UNKNOWN

        Examples:
            >>> adapter = DockerComposeAdapter("node1", ssh_func)
            >>> version = adapter.detect_version()
            >>> print(version)
            DockerComposeVersion.V2
        """
        # 如果强制指定版本，直接返回
        if self.forced_version != DockerComposeVersion.AUTO:
            return self.forced_version

        # 检查缓存是否有效
        current_time = time.time()
        if (not force_refresh and
            self._cached_version is not None and
            (current_time - self._cache_timestamp) < self.cache_ttl):
            return self._cached_version

        self.logger.info(f"Detecting Docker Compose version for node {self.node_name}")

        # 优先检测V2版本
        if self._test_version_command("docker compose version"):
            detected_version = DockerComposeVersion.V2
            self.logger.info(f"Node {self.node_name}: Docker Compose V2 detected")
        elif self._test_version_command("docker-compose version"):
            detected_version = DockerComposeVersion.V1
            self.logger.info(f"Node {self.node_name}: Docker Compose V1 detected")
        else:
            detected_version = DockerComposeVersion.UNKNOWN
            self.logger.warning(f"Node {self.node_name}: No Docker Compose version detected")

        # 更新缓存
        self._cached_version = detected_version
        self._cache_timestamp = current_time

        return detected_version

    def _test_version_command(self, version_cmd: str) -> bool:
        """测试版本命令是否可用"""
        try:
            results = self.ssh_executor(version_cmd, [self.node_name], timeout=10)
            node_result = results.get(self.node_name)
            if node_result and node_result[0] == 0:
                return True
        except Exception as e:
            self.logger.debug(f"Version command '{version_cmd}' failed: {e}")
        return False

    def build_command(self, command_type: str, **kwargs) -> ComposeCommand:
        """构建Docker Compose命令

        根据检测到的版本构建相应格式的Docker Compose命令。
        支持的命令类型包括: up, down, stop, logs, scale, ps, exec等。

        Args:
            command_type: 命令类型，如'up', 'down', 'stop', 'logs'等
            **kwargs: 命令参数，常用参数包括:
                - file: docker-compose文件路径
                - env_file: 环境变量文件路径（可选）
                - services: 服务名列表或字符串
                - lines: 日志行数(仅logs命令)
                - service: 服务名(仅exec/scale命令)
                - replicas: 副本数(仅scale命令)
                - command: 执行的命令(仅exec命令)

        Returns:
            封装的命令对象，包含完整命令字符串和版本信息

        Raises:
            ValueError: 当版本不支持或命令类型不支持时
            KeyError: 当缺少必需参数时

        Examples:
            >>> adapter = DockerComposeAdapter("node1", ssh_func)
            >>> cmd = adapter.build_command("up", file="app.yml", services=["web", "db"])
            >>> print(cmd.full_cmd)
            'docker compose -f app.yml up -d web db'
            >>>
            >>> cmd_with_env = adapter.build_command("up", file="app.yml", env_file=".env", services="web")
            >>> print(cmd_with_env.full_cmd)
            'docker compose --env-file .env -f app.yml up -d web'
            >>>
            >>> log_cmd = adapter.build_command("logs", file="app.yml", services="web", lines=100)
            >>> print(log_cmd.full_cmd)
            'docker compose -f app.yml logs --tail 100 web'
        """
        version = self.detect_version()

        if version == DockerComposeVersion.UNKNOWN:
            # 降级策略：如果检测失败，默认使用V2
            self.logger.warning(f"Unknown compose version, defaulting to V2 for node {self.node_name}")
            version = DockerComposeVersion.V2

        if version not in self.COMMAND_TEMPLATES:
            raise ValueError(f"Unsupported Docker Compose version: {version}")

        templates = self.COMMAND_TEMPLATES[version]

        if command_type not in templates:
            raise ValueError(f"Unsupported command type: {command_type}")

        # 构建命令
        template = templates[command_type]
        base_cmd = templates["base_cmd"]

        # 处理参数
        params = self._process_command_params(kwargs)

        try:
            full_cmd = template.format(**params)
        except KeyError as e:
            raise ValueError(f"Missing required parameter for command '{command_type}': {e}")

        return ComposeCommand(
            base_cmd=base_cmd,
            full_cmd=full_cmd,
            version=version
        )

    def _process_command_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        """处理命令参数，确保正确格式化"""
        processed = {}

        # 处理文件路径
        if "file" in params:
            file_path = params["file"]
            if isinstance(file_path, Path):
                processed["file"] = str(file_path.name)
            else:
                processed["file"] = str(Path(file_path).name)

        # 处理环境变量文件
        if "env_file" in params and params["env_file"]:
            env_file_path = params["env_file"]
            if isinstance(env_file_path, Path):
                processed["env_file"] = f" --env-file {env_file_path.name}"
            else:
                processed["env_file"] = f" --env-file {Path(env_file_path).name}"
        else:
            processed["env_file"] = ""

        # 处理服务列表
        if "services" in params:
            services = params["services"]
            if isinstance(services, list):
                processed["services"] = " ".join(services) if services else ""
            else:
                processed["services"] = str(services) if services else ""

        # 处理其他参数
        for key, value in params.items():
            if key not in processed:
                processed[key] = str(value) if value is not None else ""

        # 设置默认值
        processed.setdefault("services", "")
        processed.setdefault("lines", "50")

        return processed

    def get_version_info(self) -> Dict[str, Any]:
        """获取版本信息"""
        version = self.detect_version()
        return {
            "node_name": self.node_name,
            "detected_version": version.value,
            "forced_version": self.forced_version.value,
            "is_cached": self._cached_version is not None,
            "cache_age": time.time() - self._cache_timestamp if self._cached_version else 0
        }

    def clear_cache(self):
        """清除版本检测缓存"""
        self._cached_version = None
        self._cache_timestamp = 0
        self.logger.info(f"Cleared version cache for node {self.node_name}")


class DockerComposeAdapterManager:
    """Docker Compose适配器管理器"""

    def __init__(self, ssh_executor_func):
        """
        初始化管理器

        Args:
            ssh_executor_func: SSH命令执行函数
        """
        self.ssh_executor = ssh_executor_func
        self.adapters: Dict[str, DockerComposeAdapter] = {}
        self.logger = logging.getLogger("docker_compose_adapter_manager")

    def get_adapter(self, node_name: str, forced_version: str = "auto") -> DockerComposeAdapter:
        """
        获取或创建节点适配器

        Args:
            node_name: 节点名称
            forced_version: 强制版本

        Returns:
            适配器实例
        """
        cache_key = f"{node_name}_{forced_version}"

        if cache_key not in self.adapters:
            self.adapters[cache_key] = DockerComposeAdapter(
                node_name=node_name,
                ssh_executor_func=self.ssh_executor,
                forced_version=forced_version
            )
            self.logger.info(f"Created adapter for node {node_name} with version {forced_version}")

        return self.adapters[cache_key]

    def build_command(self, node_name: str, command_type: str,
                     forced_version: str = "auto", **kwargs) -> ComposeCommand:
        """
        为指定节点构建命令

        Args:
            node_name: 节点名称
            command_type: 命令类型
            forced_version: 强制版本
            **kwargs: 命令参数

        Returns:
            命令对象
        """
        adapter = self.get_adapter(node_name, forced_version)
        return adapter.build_command(command_type, **kwargs)

    def detect_all_versions(self, node_names: list,
                           forced_versions: Dict[str, str] = None) -> Dict[str, Dict[str, Any]]:
        """
        批量检测所有节点的版本

        Args:
            node_names: 节点名称列表
            forced_versions: 节点强制版本映射

        Returns:
            节点版本信息字典
        """
        if forced_versions is None:
            forced_versions = {}

        results = {}
        for node_name in node_names:
            forced_version = forced_versions.get(node_name, "auto")
            adapter = self.get_adapter(node_name, forced_version)
            results[node_name] = adapter.get_version_info()

        return results

    def clear_all_caches(self):
        """清除所有适配器的缓存"""
        for adapter in self.adapters.values():
            adapter.clear_cache()
        self.logger.info("Cleared all adapter caches")