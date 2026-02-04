"""Docker Compose部署后端

包装现有的DockerComposeManager逻辑，实现DeploymentBackend接口。
提供与KubectlBackend一致的API接口。

主要功能:
    - 包装DockerComposeManager的部署功能
    - 实现统一的DeploymentBackend接口
    - 支持与Kubernetes后端互操作

典型用法:
    >>> backend = DockerComposeBackend(docker_manager, node_manager)
    >>> result = backend.deploy_service(scenario_path, service_config, 'node1')
"""

import logging
import time
from typing import Dict, List, Optional, Any
from pathlib import Path

from .backend import (
    DeploymentBackend, DeploymentPlatform, DeploymentResult, ServiceStatus
)
from ...utils.common import setup_module_logger


class DockerComposeBackend(DeploymentBackend):
    """Docker Compose部署后端

    包装现有的DockerComposeManager，实现统一的DeploymentBackend接口。

    Attributes:
        docker_manager: DockerComposeManager实例
        node_manager: NodeManager实例
        logger: 日志记录器

    Examples:
        >>> backend = DockerComposeBackend(docker_manager, node_manager)
        >>> result = backend.deploy_service(
        ...     Path('/config/scenarios/test'),
        ...     service_config,
        ...     'node1'
        ... )
    """

    def __init__(self, docker_manager, node_manager):
        """初始化Docker Compose后端

        Args:
            docker_manager: DockerComposeManager实例
            node_manager: NodeManager实例
        """
        self.logger = setup_module_logger("playbook.docker_compose_backend")
        self.docker_manager = docker_manager
        self.node_manager = node_manager

    @property
    def platform(self) -> DeploymentPlatform:
        return DeploymentPlatform.DOCKER_COMPOSE

    def deploy_service(self, scenario_path: Path, service_config: Dict[str, Any],
                      node_name: str, timeout: int = 300,
                      is_retry: bool = False) -> DeploymentResult:
        """部署Docker Compose服务

        调用DockerComposeManager的deploy_service方法。

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置
            node_name: 目标节点名称
            timeout: 部署超时时间
            is_retry: 是否为重试

        Returns:
            DeploymentResult: 部署结果
        """
        service_name = service_config.get('name', 'unknown')
        start_time = time.time()

        self.logger.info(f"Deploying Docker Compose service: {service_name} to {node_name}")

        try:
            # 创建ServiceDeployment对象（模拟）
            from ..scenario_manager import ServiceDeployment
            service = self._config_to_service_deployment(service_config)

            # 调用现有的deploy_service方法
            success = self.docker_manager.deploy_service(
                scenario_path, service, node_name, timeout, is_retry
            )

            duration = time.time() - start_time

            if success:
                return DeploymentResult(
                    success=True,
                    service_name=service_name,
                    platform=self.platform,
                    message=f"Successfully deployed {service_name}",
                    details={'node': node_name},
                    duration=duration
                )
            else:
                return DeploymentResult(
                    success=False,
                    service_name=service_name,
                    platform=self.platform,
                    message=f"Failed to deploy {service_name}",
                    details={'node': node_name},
                    duration=duration
                )

        except Exception as e:
            self.logger.error(f"Failed to deploy service {service_name}: {e}")
            return DeploymentResult(
                success=False,
                service_name=service_name,
                platform=self.platform,
                message=str(e),
                duration=time.time() - start_time
            )

    def stop_service(self, scenario_path: Path, service_config: Dict[str, Any],
                    node_name: str, timeout: int = 120) -> bool:
        """停止Docker Compose服务

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置
            node_name: 目标节点名称
            timeout: 停止超时时间

        Returns:
            bool: 停止是否成功
        """
        service_name = service_config.get('name', 'unknown')
        self.logger.info(f"Stopping Docker Compose service: {service_name} on {node_name}")

        try:
            from ..scenario_manager import ServiceDeployment
            service = self._config_to_service_deployment(service_config)

            return self.docker_manager.stop_service(
                scenario_path, service, node_name, timeout
            )

        except Exception as e:
            self.logger.error(f"Error stopping service {service_name}: {e}")
            return False

    def get_service_status(self, service_name: str, node_name: str,
                          context: Dict[str, Any] = None) -> ServiceStatus:
        """获取Docker Compose服务状态

        Args:
            service_name: 服务名称
            node_name: 目标节点名称
            context: 上下文信息

        Returns:
            ServiceStatus: 服务状态
        """
        try:
            # 调用docker_manager获取状态
            status_info = self.docker_manager.get_service_status(service_name, node_name)

            if status_info:
                running = status_info.get('running', False)
                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=running,
                    ready=running,  # Docker Compose没有就绪概念，简化处理
                    details=status_info
                )
            else:
                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=False,
                    ready=False,
                    details={'error': 'Status not available'}
                )

        except Exception as e:
            self.logger.error(f"Failed to get status for {service_name}: {e}")
            return ServiceStatus(
                service_name=service_name,
                platform=self.platform,
                running=False,
                ready=False,
                details={'error': str(e)}
            )

    def health_check(self, service_name: str, node_name: str,
                    check_config: Dict[str, Any],
                    context: Dict[str, Any] = None) -> bool:
        """执行Docker Compose服务健康检查

        Args:
            service_name: 服务名称
            node_name: 目标节点名称
            check_config: 健康检查配置
            context: 上下文信息

        Returns:
            bool: 健康检查是否通过
        """
        check_type = check_config.get('type', 'docker_status')

        if check_type == 'docker_status':
            status = self.get_service_status(service_name, node_name, context)
            return status.running
        else:
            # 其他检查类型交给HealthCheckManager处理
            self.logger.debug(f"Health check type '{check_type}' not handled by backend")
            return True

    def cleanup(self, scenario_path: Path, service_configs: List[Dict[str, Any]],
               node_name: str, force: bool = False) -> bool:
        """清理Docker Compose服务资源

        Args:
            scenario_path: 场景配置目录
            service_configs: 服务配置列表
            node_name: 目标节点名称
            force: 是否强制清理

        Returns:
            bool: 清理是否成功
        """
        success = True

        for service_config in service_configs:
            try:
                if not self.stop_service(scenario_path, service_config, node_name):
                    success = False
            except Exception as e:
                self.logger.error(f"Cleanup failed for {service_config.get('name')}: {e}")
                success = False

        return success

    def validate_config(self, service_config: Dict[str, Any]) -> Dict[str, List[str]]:
        """验证Docker Compose服务配置

        Args:
            service_config: 服务配置

        Returns:
            Dict包含'errors'和'warnings'列表
        """
        errors = []
        warnings = []

        # 检查compose_file
        if not service_config.get('compose_file'):
            errors.append("Missing 'compose_file' configuration")

        # 检查nodes
        if not service_config.get('nodes'):
            warnings.append("No target nodes specified")

        return {'errors': errors, 'warnings': warnings}

    def _config_to_service_deployment(self, service_config: Dict[str, Any]):
        """将配置字典转换为ServiceDeployment对象

        Args:
            service_config: 服务配置字典

        Returns:
            ServiceDeployment对象
        """
        from ..scenario_manager import ServiceDeployment, ServiceHealthCheck

        # 构建健康检查配置
        health_check = None
        hc_config = service_config.get('health_check', {})
        if hc_config.get('enabled', False):
            health_check = ServiceHealthCheck(
                enabled=True,
                strategy=hc_config.get('strategy', 'standard'),
                startup_timeout=hc_config.get('startup_timeout', 60),
                startup_grace_period=hc_config.get('startup_grace_period', 10),
                check_interval=hc_config.get('check_interval', 10),
                max_retries=hc_config.get('max_retries', 3),
                checks=hc_config.get('checks', []),
                failure_action=hc_config.get('failure_action', 'retry'),
                retry_delay=hc_config.get('retry_delay', 5)
            )

        return ServiceDeployment(
            name=service_config.get('name'),
            compose_file=service_config.get('compose_file'),
            nodes=service_config.get('nodes', []),
            depends_on=service_config.get('depends_on', []),
            health_check=health_check
        )
