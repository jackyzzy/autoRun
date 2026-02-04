"""部署后端工厂

集中管理平台类型检测和后端实例创建，作为编排层与部署后端之间的唯一桥梁。

主要功能:
    - 根据服务配置自动检测部署平台类型
    - 根据节点类型自动选择部署后端
    - 缓存后端实例，避免重复创建

典型用法:
    >>> factory = DeploymentBackendFactory(node_manager, docker_manager)
    >>> backend = factory.get_backend(service, node_name)
    >>> result = backend.deploy_service(scenario_path, config, node_name)
"""

import logging
from typing import Any, Optional, Union

from .backend import DeploymentBackend, DeploymentPlatform
from .docker_compose_backend import DockerComposeBackend
from .kubectl_backend import KubectlBackend
from ...utils.common import setup_module_logger


class DeploymentBackendFactory:
    """部署后端工厂

    根据服务配置或节点类型自动选择并缓存后端实例。
    这是编排层与部署后端之间的唯一接口，消除了编排层中的平台判断逻辑。

    Attributes:
        node_manager: NodeManager实例，用于判断节点类型
        docker_manager: DockerComposeManager实例

    Examples:
        >>> factory = DeploymentBackendFactory(node_manager, docker_manager)
        >>> # 根据服务配置获取后端
        >>> backend = factory.get_backend(service, 'node1')
        >>> # 仅根据节点类型获取后端
        >>> backend = factory.get_backend(node_name='k8s_cluster')
    """

    def __init__(self, node_manager, docker_manager):
        """初始化后端工厂

        Args:
            node_manager: NodeManager实例
            docker_manager: DockerComposeManager实例
        """
        self.logger = setup_module_logger("playbook.backend_factory")
        self.node_manager = node_manager
        self.docker_manager = docker_manager
        self._docker_backend: Optional[DockerComposeBackend] = None
        self._kubectl_backend: Optional[KubectlBackend] = None

    def get_backend(self, service: Any = None, node_name: str = None) -> DeploymentBackend:
        """获取部署后端

        根据服务配置和节点类型自动选择合适的部署后端。

        检测优先级:
        1. 服务有kubectl配置 → K8S后端
        2. 节点是K8S集群 → K8S后端
        3. 其他情况 → Docker Compose后端

        Args:
            service: 服务配置（ServiceDeployment对象或字典），可选
            node_name: 节点名称，可选

        Returns:
            DeploymentBackend: 适合的部署后端实例

        Examples:
            >>> # K8S服务（有kubectl配置）
            >>> backend = factory.get_backend(k8s_service, 'k8s_cluster')
            >>> assert backend.platform == DeploymentPlatform.KUBERNETES

            >>> # Docker服务
            >>> backend = factory.get_backend(docker_service, 'docker_node')
            >>> assert backend.platform == DeploymentPlatform.DOCKER_COMPOSE
        """
        platform = self._detect_platform(service, node_name)

        if platform == DeploymentPlatform.KUBERNETES:
            return self._get_kubectl_backend()
        return self._get_docker_backend()

    def _detect_platform(self, service: Any, node_name: str) -> DeploymentPlatform:
        """检测服务应该使用的部署平台

        Args:
            service: 服务配置
            node_name: 节点名称

        Returns:
            DeploymentPlatform: 检测到的平台类型
        """
        # 1. 检查服务级别的kubectl配置
        if service is not None:
            kubectl_config = self._get_kubectl_config(service)
            if kubectl_config is not None:
                self.logger.debug(f"Service has kubectl config, using K8S backend")
                return DeploymentPlatform.KUBERNETES

        # 2. 检查节点是否为K8S集群
        if node_name and self.node_manager and self.node_manager.is_k8s_cluster(node_name):
            self.logger.debug(f"Node {node_name} is K8S cluster, using K8S backend")
            return DeploymentPlatform.KUBERNETES

        # 3. 默认使用Docker Compose
        return DeploymentPlatform.DOCKER_COMPOSE

    def _get_kubectl_config(self, service: Any) -> Optional[dict]:
        """从服务配置中提取kubectl配置

        Args:
            service: 服务配置（ServiceDeployment对象或字典）

        Returns:
            kubectl配置字典，如果没有则返回None
        """
        # 处理ServiceDeployment对象
        if hasattr(service, 'kubectl'):
            return service.kubectl

        # 处理字典配置
        if isinstance(service, dict):
            return service.get('kubectl')

        return None

    def _get_docker_backend(self) -> DockerComposeBackend:
        """获取Docker Compose后端（懒加载）"""
        if self._docker_backend is None:
            self._docker_backend = DockerComposeBackend(
                self.docker_manager, self.node_manager
            )
            self.logger.debug("Created DockerComposeBackend instance")
        return self._docker_backend

    def _get_kubectl_backend(self) -> KubectlBackend:
        """获取Kubectl后端（懒加载）"""
        if self._kubectl_backend is None:
            self._kubectl_backend = KubectlBackend(self.node_manager)
            self.logger.debug("Created KubectlBackend instance")
        return self._kubectl_backend
