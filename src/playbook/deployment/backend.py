"""部署后端抽象接口

定义统一的服务部署操作接口，所有部署后端（Docker Compose、Kubernetes等）
都需要实现这个接口。

主要组件:
    - DeploymentPlatform: 部署平台枚举
    - DeploymentResult: 部署结果数据类
    - ServiceStatus: 服务状态数据类
    - DeploymentBackend: 部署后端抽象基类

典型用法:
    >>> backend = KubectlBackend(cluster_config)
    >>> result = backend.deploy_service(scenario_path, service_config)
    >>> if result.success:
    ...     print(f"Deployed {result.service_name} successfully")
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import time


class DeploymentPlatform(Enum):
    """部署平台枚举

    定义支持的部署平台类型。

    Attributes:
        DOCKER_COMPOSE: Docker Compose部署平台
        KUBERNETES: Kubernetes部署平台（使用kubectl）
        AUTO: 自动检测平台类型
    """
    DOCKER_COMPOSE = "docker_compose"
    KUBERNETES = "kubernetes"
    AUTO = "auto"


@dataclass
class DeploymentResult:
    """部署结果

    封装服务部署操作的结果信息。

    Attributes:
        success: 部署是否成功
        service_name: 服务名称
        platform: 部署平台类型
        message: 结果消息（成功或错误信息）
        details: 详细信息字典
        duration: 部署耗时（秒）

    Examples:
        >>> result = DeploymentResult(
        ...     success=True,
        ...     service_name="web-api",
        ...     platform=DeploymentPlatform.KUBERNETES,
        ...     message="Successfully deployed",
        ...     duration=45.2
        ... )
    """
    success: bool
    service_name: str
    platform: DeploymentPlatform
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    duration: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'success': self.success,
            'service_name': self.service_name,
            'platform': self.platform.value,
            'message': self.message,
            'details': self.details,
            'duration': self.duration
        }


@dataclass
class ServiceStatus:
    """服务状态

    封装服务的运行状态信息。

    Attributes:
        service_name: 服务名称
        platform: 部署平台类型
        running: 服务是否正在运行
        ready: 服务是否就绪（通过健康检查）
        replicas: 副本总数
        ready_replicas: 就绪的副本数
        details: 详细状态信息

    Examples:
        >>> status = ServiceStatus(
        ...     service_name="web-api",
        ...     platform=DeploymentPlatform.KUBERNETES,
        ...     running=True,
        ...     ready=True,
        ...     replicas=3,
        ...     ready_replicas=3
        ... )
    """
    service_name: str
    platform: DeploymentPlatform
    running: bool
    ready: bool
    replicas: int = 0
    ready_replicas: int = 0
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'service_name': self.service_name,
            'platform': self.platform.value,
            'running': self.running,
            'ready': self.ready,
            'replicas': self.replicas,
            'ready_replicas': self.ready_replicas,
            'details': self.details
        }


class DeploymentBackend(ABC):
    """部署后端抽象基类

    定义所有部署后端必须实现的接口。
    Docker Compose后端和Kubernetes后端都需要继承此类。

    Methods:
        platform: 返回平台类型
        deploy_service: 部署服务
        stop_service: 停止服务
        get_service_status: 获取服务状态
        health_check: 执行健康检查
        cleanup: 清理服务资源
        validate_config: 验证服务配置
    """

    @property
    @abstractmethod
    def platform(self) -> DeploymentPlatform:
        """返回平台类型

        Returns:
            DeploymentPlatform: 当前后端的平台类型
        """
        pass

    @abstractmethod
    def deploy_service(self, scenario_path: Path, service_config: Dict[str, Any],
                      node_name: str, timeout: int = 300,
                      is_retry: bool = False) -> DeploymentResult:
        """部署服务

        将服务部署到指定的节点或集群。

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置字典
            node_name: 目标节点/集群名称
            timeout: 部署超时时间（秒）
            is_retry: 是否为重试操作

        Returns:
            DeploymentResult: 部署结果
        """
        pass

    @abstractmethod
    def stop_service(self, scenario_path: Path, service_config: Dict[str, Any],
                    node_name: str, timeout: int = 120) -> bool:
        """停止服务

        停止在指定节点/集群上运行的服务。

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置字典
            node_name: 目标节点/集群名称
            timeout: 停止超时时间（秒）

        Returns:
            bool: 停止是否成功
        """
        pass

    @abstractmethod
    def get_service_status(self, service_name: str, node_name: str,
                          context: Dict[str, Any] = None) -> ServiceStatus:
        """获取服务状态

        查询服务在指定节点/集群上的运行状态。

        Args:
            service_name: 服务名称
            node_name: 目标节点/集群名称
            context: 额外的上下文信息（如namespace、kind等）

        Returns:
            ServiceStatus: 服务状态
        """
        pass

    @abstractmethod
    def health_check(self, service_name: str, node_name: str,
                    check_config: Dict[str, Any],
                    context: Dict[str, Any] = None) -> bool:
        """执行健康检查

        对服务执行指定的健康检查。

        Args:
            service_name: 服务名称
            node_name: 目标节点/集群名称
            check_config: 健康检查配置
            context: 额外的上下文信息

        Returns:
            bool: 健康检查是否通过
        """
        pass

    @abstractmethod
    def cleanup(self, scenario_path: Path, service_configs: List[Dict[str, Any]],
               node_name: str, force: bool = False) -> bool:
        """清理服务资源

        清理指定节点/集群上的服务资源。

        Args:
            scenario_path: 场景配置目录路径
            service_configs: 服务配置列表
            node_name: 目标节点/集群名称
            force: 是否强制清理

        Returns:
            bool: 清理是否成功
        """
        pass

    def validate_config(self, service_config: Dict[str, Any]) -> Dict[str, List[str]]:
        """验证服务配置

        验证服务配置是否有效。

        Args:
            service_config: 服务配置字典

        Returns:
            Dict包含'errors'和'warnings'列表
        """
        return {'errors': [], 'warnings': []}

    def _measure_time(self, func, *args, **kwargs) -> Tuple[Any, float]:
        """测量函数执行时间

        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            Tuple[Any, float]: (函数返回值, 执行时间秒)
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        return result, duration
