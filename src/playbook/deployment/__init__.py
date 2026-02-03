"""部署后端抽象层

提供统一的部署后端接口，支持Docker Compose和Kubernetes两种部署方式。

主要组件:
    - DeploymentBackend: 部署后端抽象基类
    - DockerComposeBackend: Docker Compose部署后端
    - KubectlBackend: Kubernetes部署后端
    - DeploymentManager: 统一部署管理器（自动选择后端）
"""

from .backend import (
    DeploymentPlatform,
    DeploymentResult,
    ServiceStatus,
    DeploymentBackend
)
from .docker_compose_backend import DockerComposeBackend
from .kubectl_backend import KubectlBackend

__all__ = [
    'DeploymentPlatform',
    'DeploymentResult',
    'ServiceStatus',
    'DeploymentBackend',
    'DockerComposeBackend',
    'KubectlBackend'
]
