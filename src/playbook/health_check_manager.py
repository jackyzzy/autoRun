"""
服务健康检查管理器
提供多层次的服务健康检查机制
支持Docker和Kubernetes两种部署平台
"""

import json
import time
import logging
import requests
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed

from .scenario_manager import ServiceHealthCheck, ServiceDeployment
from .node_manager import NodeManager
from .docker_container_service import DockerContainerService
from ..utils.progress_logger import EnhancedProgressLogger


class HealthCheckType(Enum):
    """健康检查类型"""
    # Docker相关检查
    DOCKER_STATUS = "docker_status"
    DOCKER_HEALTH = "docker_health"
    # 通用检查
    HTTP_ENDPOINT = "http_endpoint"
    CUSTOM_SCRIPT = "custom_script"
    # Kubernetes相关检查
    POD_READY = "pod_ready"
    DEPLOYMENT_READY = "deployment_ready"
    RESOURCE_EXISTS = "resource_exists"
    SERVICE_ENDPOINTS = "service_endpoints"


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    check_type: str
    service_name: str
    node_name: str
    success: bool
    message: str = ""
    details: Dict[str, Any] = None
    duration: float = 0.0
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


class ServiceHealthChecker:
    """单个服务的健康检查器

    支持Docker和Kubernetes两种部署平台的健康检查。
    根据service的配置自动选择合适的检查方法。
    """

    def __init__(self, service: ServiceDeployment, node_manager: NodeManager):
        self.service = service
        self.node_manager = node_manager
        self.logger = logging.getLogger(f"playbook.health_checker.{service.name}")
        self.container_service = DockerContainerService(node_manager)

        # K8S相关延迟导入，避免循环引用
        self._k8s_cluster_cache: Dict[str, Any] = {}
        
        
    def check_docker_status(self, node_name: str) -> HealthCheckResult:
        """检查Docker容器状态"""
        start_time = time.time()
        
        try:
            # 使用统一的容器服务查找容器
            result = self.container_service.find_containers_for_service(
                service_name=self.service.name,
                node_name=node_name
            )
            
            duration = time.time() - start_time
            
            if not result.success:
                self.logger.error(f"容器检查失败 - service: {self.service.name}, node: {node_name}, error: {result.error}")
                return HealthCheckResult(
                    check_type=HealthCheckType.DOCKER_STATUS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=result.error or "Container not found or not running",
                    duration=duration
                )
            
            # 检查是否有运行中的容器
            success = result.has_running_containers()
            running_containers = result.get_running_containers()
            
            self.logger.info(f"容器状态检查完成 - 策略: {result.strategy_used}, 找到{len(result.containers)}个容器, 运行中: {len(running_containers)}个")
            for container in result.containers:
                self.logger.info(f"  容器: {container.name}, 状态: {container.status}, 运行中: {container.is_running()}")
            
            # 转换为旧格式的containers信息用于详细信息
            containers_details = [
                {
                    'name': container.name,
                    'status': container.status,
                    'ports': container.ports,
                    'running': container.is_running()
                }
                for container in result.containers
            ]
            
            return HealthCheckResult(
                check_type=HealthCheckType.DOCKER_STATUS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=success,
                message=f"Found {len(running_containers)} running container(s) using {result.strategy_used}" if success else "No running containers found",
                details={'containers': containers_details, 'strategy': result.strategy_used},
                duration=duration
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.DOCKER_STATUS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Docker status check failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def check_http_endpoint(self, node_name: str, check_config: Dict[str, Any]) -> HealthCheckResult:
        """检查HTTP端点

        支持Docker节点和K8S集群。对于K8S集群，可以检查NodePort或LoadBalancer服务。
        """
        start_time = time.time()

        try:
            # 获取节点/集群信息构建URL
            node = self.node_manager.get_node(node_name)
            cluster = self._get_k8s_cluster(node_name) if not node else None

            if not node and not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.HTTP_ENDPOINT.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Node or K8S cluster {node_name} not found",
                    duration=time.time() - start_time
                )

            # 获取主机地址（支持Docker节点和K8S集群）
            host = node.host if node else cluster.host

            # 构建URL，替换localhost为实际节点/集群IP
            url = check_config.get('url', '')
            if url.startswith('http://localhost'):
                url = url.replace('localhost', host)
            elif url.startswith('http://127.0.0.1'):
                url = url.replace('127.0.0.1', host)
            
            method = check_config.get('method', 'GET').upper()
            expected_status = check_config.get('expected_status', 200)
            timeout = check_config.get('timeout', 10)
            
            # 执行HTTP检查
            response = requests.request(
                method=method,
                url=url,
                timeout=timeout,
                headers={'User-Agent': 'playbook-health-checker/1.0'}
            )
            
            duration = time.time() - start_time
            success = response.status_code == expected_status
            
            return HealthCheckResult(
                check_type=HealthCheckType.HTTP_ENDPOINT.value,
                service_name=self.service.name,
                node_name=node_name,
                success=success,
                message=f"HTTP {method} {url} returned {response.status_code}",
                details={
                    'url': url,
                    'method': method,
                    'status_code': response.status_code,
                    'expected_status': expected_status,
                    'response_time': duration
                },
                duration=duration
            )
            
        except requests.Timeout:
            return HealthCheckResult(
                check_type=HealthCheckType.HTTP_ENDPOINT.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"HTTP request timeout after {timeout}s",
                duration=time.time() - start_time
            )
        except requests.RequestException as e:
            return HealthCheckResult(
                check_type=HealthCheckType.HTTP_ENDPOINT.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"HTTP request failed: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.HTTP_ENDPOINT.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"HTTP endpoint check failed: {str(e)}",
                duration=time.time() - start_time
            )

    # ==================== K8S 健康检查方法 ====================

    def _get_k8s_cluster(self, node_name: str):
        """获取K8S集群对象

        Args:
            node_name: 集群名称

        Returns:
            K8SCluster对象，如果不是K8S集群则返回None
        """
        if node_name in self._k8s_cluster_cache:
            return self._k8s_cluster_cache[node_name]

        cluster = self.node_manager.get_k8s_cluster(node_name)
        self._k8s_cluster_cache[node_name] = cluster
        return cluster

    def _execute_kubectl(self, cluster, cmd: str) -> tuple:
        """执行kubectl命令

        Args:
            cluster: K8SCluster对象
            cmd: 要执行的命令

        Returns:
            tuple: (success, stdout, stderr)
        """
        try:
            ssh_client = cluster.get_ssh_client()
            result = ssh_client.execute_command(cmd)

            if hasattr(result, 'exit_status'):
                # 新版SSH返回对象
                return result.exit_status == 0, result.stdout or '', result.stderr or ''
            elif isinstance(result, tuple):
                # 兼容旧版返回元组
                return result[0] == 0, result[1] or '', result[2] or ''
            else:
                # 直接返回stdout
                return True, str(result), ''
        except Exception as e:
            self.logger.error(f"kubectl command failed: {cmd}, error: {e}")
            return False, '', str(e)

    def _get_service_namespace(self) -> str:
        """获取服务的命名空间"""
        if self.service.kubectl:
            return self.service.kubectl.get('namespace', 'default')
        return 'default'

    def check_pod_ready(self, node_name: str, check_config: Dict[str, Any]) -> HealthCheckResult:
        """检查Pod是否就绪

        Args:
            node_name: K8S集群名称
            check_config: 检查配置，包含:
                - selector: Pod标签选择器 (如 "app=myapp")
                - min_ready: 最少就绪Pod数量 (默认1)
                - timeout: 超时时间秒数 (默认60)

        Returns:
            HealthCheckResult: 检查结果
        """
        start_time = time.time()

        try:
            cluster = self._get_k8s_cluster(node_name)
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.POD_READY.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Node {node_name} is not a K8S cluster",
                    duration=time.time() - start_time
                )

            selector = check_config.get('selector', '')
            min_ready = check_config.get('min_ready', 1)
            namespace = check_config.get('namespace') or self._get_service_namespace()

            # 构建kubectl命令获取Pod状态
            cmd = cluster.build_kubectl_command(
                'get', namespace=namespace,
                extra_args=['pods', '-l', selector, '-o', 'json']
            )

            success, stdout, stderr = self._execute_kubectl(cluster, cmd)

            if not success:
                return HealthCheckResult(
                    check_type=HealthCheckType.POD_READY.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Failed to get pods: {stderr}",
                    duration=time.time() - start_time
                )

            # 解析JSON输出
            pods_data = json.loads(stdout)
            pods = pods_data.get('items', [])

            # 统计就绪的Pod数量
            ready_count = 0
            pod_statuses = []

            for pod in pods:
                pod_name = pod.get('metadata', {}).get('name', 'unknown')
                pod_phase = pod.get('status', {}).get('phase', 'Unknown')

                # 检查所有容器是否就绪
                conditions = pod.get('status', {}).get('conditions', [])
                is_ready = False
                for condition in conditions:
                    if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                        is_ready = True
                        break

                if is_ready and pod_phase == 'Running':
                    ready_count += 1

                pod_statuses.append({
                    'name': pod_name,
                    'phase': pod_phase,
                    'ready': is_ready
                })

            check_passed = ready_count >= min_ready
            duration = time.time() - start_time

            return HealthCheckResult(
                check_type=HealthCheckType.POD_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=check_passed,
                message=f"Ready pods: {ready_count}/{min_ready} (total: {len(pods)})",
                details={
                    'selector': selector,
                    'namespace': namespace,
                    'ready_count': ready_count,
                    'min_ready': min_ready,
                    'total_pods': len(pods),
                    'pods': pod_statuses
                },
                duration=duration
            )

        except json.JSONDecodeError as e:
            return HealthCheckResult(
                check_type=HealthCheckType.POD_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Failed to parse pod status: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.POD_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Pod ready check failed: {str(e)}",
                duration=time.time() - start_time
            )

    def check_deployment_ready(self, node_name: str, check_config: Dict[str, Any]) -> HealthCheckResult:
        """检查Deployment是否就绪

        Args:
            node_name: K8S集群名称
            check_config: 检查配置，包含:
                - name: Deployment名称
                - timeout: 超时时间秒数 (默认60)

        Returns:
            HealthCheckResult: 检查结果
        """
        start_time = time.time()

        try:
            cluster = self._get_k8s_cluster(node_name)
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.DEPLOYMENT_READY.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Node {node_name} is not a K8S cluster",
                    duration=time.time() - start_time
                )

            deployment_name = check_config.get('name', self.service.name)
            namespace = check_config.get('namespace') or self._get_service_namespace()

            # 构建kubectl命令获取Deployment状态
            cmd = cluster.build_kubectl_command(
                'get', namespace=namespace,
                extra_args=['deployment', deployment_name, '-o', 'json']
            )

            success, stdout, stderr = self._execute_kubectl(cluster, cmd)

            if not success:
                return HealthCheckResult(
                    check_type=HealthCheckType.DEPLOYMENT_READY.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Failed to get deployment: {stderr}",
                    duration=time.time() - start_time
                )

            # 解析JSON输出
            deployment = json.loads(stdout)
            status = deployment.get('status', {})

            replicas = status.get('replicas', 0)
            ready_replicas = status.get('readyReplicas', 0)
            available_replicas = status.get('availableReplicas', 0)
            updated_replicas = status.get('updatedReplicas', 0)

            # 检查Deployment是否完全就绪
            check_passed = (
                replicas > 0 and
                ready_replicas == replicas and
                available_replicas == replicas
            )

            duration = time.time() - start_time

            return HealthCheckResult(
                check_type=HealthCheckType.DEPLOYMENT_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=check_passed,
                message=f"Deployment {deployment_name}: {ready_replicas}/{replicas} ready, {available_replicas} available",
                details={
                    'name': deployment_name,
                    'namespace': namespace,
                    'replicas': replicas,
                    'ready_replicas': ready_replicas,
                    'available_replicas': available_replicas,
                    'updated_replicas': updated_replicas
                },
                duration=duration
            )

        except json.JSONDecodeError as e:
            return HealthCheckResult(
                check_type=HealthCheckType.DEPLOYMENT_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Failed to parse deployment status: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.DEPLOYMENT_READY.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Deployment ready check failed: {str(e)}",
                duration=time.time() - start_time
            )

    def check_resource_exists(self, node_name: str, check_config: Dict[str, Any]) -> HealthCheckResult:
        """检查K8S资源是否存在

        Args:
            node_name: K8S集群名称
            check_config: 检查配置，包含:
                - kind: 资源类型 (如 "ConfigMap", "Service", "Secret")
                - name: 资源名称

        Returns:
            HealthCheckResult: 检查结果
        """
        start_time = time.time()

        try:
            cluster = self._get_k8s_cluster(node_name)
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.RESOURCE_EXISTS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Node {node_name} is not a K8S cluster",
                    duration=time.time() - start_time
                )

            resource_kind = check_config.get('kind', '')
            resource_name = check_config.get('name', '')
            namespace = check_config.get('namespace') or self._get_service_namespace()

            if not resource_kind or not resource_name:
                return HealthCheckResult(
                    check_type=HealthCheckType.RESOURCE_EXISTS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message="Missing 'kind' or 'name' in check config",
                    duration=time.time() - start_time
                )

            # 构建kubectl命令检查资源
            cmd = cluster.build_kubectl_command(
                'get', namespace=namespace,
                extra_args=[resource_kind.lower(), resource_name, '-o', 'json']
            )

            success, stdout, stderr = self._execute_kubectl(cluster, cmd)

            duration = time.time() - start_time

            if not success:
                # 资源不存在
                return HealthCheckResult(
                    check_type=HealthCheckType.RESOURCE_EXISTS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"{resource_kind}/{resource_name} not found in namespace {namespace}",
                    details={
                        'kind': resource_kind,
                        'name': resource_name,
                        'namespace': namespace,
                        'error': stderr
                    },
                    duration=duration
                )

            # 资源存在
            return HealthCheckResult(
                check_type=HealthCheckType.RESOURCE_EXISTS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=True,
                message=f"{resource_kind}/{resource_name} exists in namespace {namespace}",
                details={
                    'kind': resource_kind,
                    'name': resource_name,
                    'namespace': namespace
                },
                duration=duration
            )

        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.RESOURCE_EXISTS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Resource exists check failed: {str(e)}",
                duration=time.time() - start_time
            )

    def check_service_endpoints(self, node_name: str, check_config: Dict[str, Any]) -> HealthCheckResult:
        """检查K8S Service的Endpoints是否就绪

        Args:
            node_name: K8S集群名称
            check_config: 检查配置，包含:
                - name: Service名称
                - min_endpoints: 最少端点数量 (默认1)

        Returns:
            HealthCheckResult: 检查结果
        """
        start_time = time.time()

        try:
            cluster = self._get_k8s_cluster(node_name)
            if not cluster:
                return HealthCheckResult(
                    check_type=HealthCheckType.SERVICE_ENDPOINTS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Node {node_name} is not a K8S cluster",
                    duration=time.time() - start_time
                )

            service_name = check_config.get('name', self.service.name)
            min_endpoints = check_config.get('min_endpoints', 1)
            namespace = check_config.get('namespace') or self._get_service_namespace()

            # 构建kubectl命令获取Endpoints
            cmd = cluster.build_kubectl_command(
                'get', namespace=namespace,
                extra_args=['endpoints', service_name, '-o', 'json']
            )

            success, stdout, stderr = self._execute_kubectl(cluster, cmd)

            if not success:
                return HealthCheckResult(
                    check_type=HealthCheckType.SERVICE_ENDPOINTS.value,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Failed to get endpoints: {stderr}",
                    duration=time.time() - start_time
                )

            # 解析JSON输出
            endpoints = json.loads(stdout)
            subsets = endpoints.get('subsets', [])

            # 统计就绪的端点数量
            ready_addresses = []
            for subset in subsets:
                addresses = subset.get('addresses', [])
                for addr in addresses:
                    ready_addresses.append(addr.get('ip', 'unknown'))

            endpoint_count = len(ready_addresses)
            check_passed = endpoint_count >= min_endpoints

            duration = time.time() - start_time

            return HealthCheckResult(
                check_type=HealthCheckType.SERVICE_ENDPOINTS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=check_passed,
                message=f"Service {service_name}: {endpoint_count}/{min_endpoints} endpoints ready",
                details={
                    'name': service_name,
                    'namespace': namespace,
                    'endpoint_count': endpoint_count,
                    'min_endpoints': min_endpoints,
                    'addresses': ready_addresses
                },
                duration=duration
            )

        except json.JSONDecodeError as e:
            return HealthCheckResult(
                check_type=HealthCheckType.SERVICE_ENDPOINTS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Failed to parse endpoints status: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return HealthCheckResult(
                check_type=HealthCheckType.SERVICE_ENDPOINTS.value,
                service_name=self.service.name,
                node_name=node_name,
                success=False,
                message=f"Service endpoints check failed: {str(e)}",
                duration=time.time() - start_time
            )

    # ==================== 通用检查方法 ====================

    def run_comprehensive_check(self, node_name: str) -> List[HealthCheckResult]:
        """运行综合健康检查"""
        results = []
        health_config = self.service.health_check
        
        if not health_config.enabled:
            return results
        
        # 执行所有配置的检查
        for check_config in health_config.checks:
            check_type = check_config.get('type', '')
            required = check_config.get('required', True)

            try:
                # Docker检查
                if check_type == HealthCheckType.DOCKER_STATUS.value:
                    result = self.check_docker_status(node_name)
                # 通用HTTP检查
                elif check_type == HealthCheckType.HTTP_ENDPOINT.value:
                    result = self.check_http_endpoint(node_name, check_config)
                # K8S检查类型
                elif check_type == HealthCheckType.POD_READY.value:
                    result = self.check_pod_ready(node_name, check_config)
                elif check_type == HealthCheckType.DEPLOYMENT_READY.value:
                    result = self.check_deployment_ready(node_name, check_config)
                elif check_type == HealthCheckType.RESOURCE_EXISTS.value:
                    result = self.check_resource_exists(node_name, check_config)
                elif check_type == HealthCheckType.SERVICE_ENDPOINTS.value:
                    result = self.check_service_endpoints(node_name, check_config)
                else:
                    result = HealthCheckResult(
                        check_type=check_type,
                        service_name=self.service.name,
                        node_name=node_name,
                        success=False,
                        message=f"Unknown check type: {check_type}"
                    )
                
                results.append(result)
                
                # 如果是必需的检查且失败，可以选择提前退出
                if required and not result.success and health_config.failure_action == "abort":
                    self.logger.error(f"Required check {check_type} failed for {self.service.name} on {node_name}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Health check {check_type} failed with exception: {e}")
                results.append(HealthCheckResult(
                    check_type=check_type,
                    service_name=self.service.name,
                    node_name=node_name,
                    success=False,
                    message=f"Check failed with exception: {str(e)}"
                ))
        
        return results


class HealthCheckManager:
    """健康检查管理器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.health_check_manager")
        self.node_manager = node_manager
        self.progress_logger = EnhancedProgressLogger(self.logger)
        
    def create_checker(self, service: ServiceDeployment) -> ServiceHealthChecker:
        """创建服务健康检查器"""
        return ServiceHealthChecker(service, self.node_manager)
    
    def run_service_health_check(self, service: ServiceDeployment, 
                                timeout: int = None) -> Dict[str, List[HealthCheckResult]]:
        """运行单个服务的健康检查"""
        if timeout is None:
            timeout = service.health_check.startup_timeout
        
        checker = self.create_checker(service)
        results = {}
        
        # 系统级超时控制
        start_time = time.time()

        # 等待启动宽限期 - 使用倒计时显示
        if service.health_check.startup_grace_period > 0:
            grace_period = service.health_check.startup_grace_period
            self.progress_logger.log_waiting_with_countdown(
                total_seconds=grace_period,
                message=f"Startup grace period for {service.name}",
                interval=10 if grace_period >= 30 else 5  # 长时间等待用10s间隔，短时间用5s
            )

        # 在每个部署节点上进行健康检查
        for node_name in service.nodes:
            node_results = []

            # 重试机制
            for attempt in range(service.health_check.max_retries + 1):
                # 检查是否超过系统级超时
                if timeout and (time.time() - start_time) > timeout:
                    self.logger.warning(f"Health check timeout ({timeout}s) exceeded for {service.name} on {node_name}")
                    break

                if attempt > 0:
                    # 显示重试进度
                    self.progress_logger.log_retry_progress(
                        current_attempt=attempt + 1,
                        max_attempts=service.health_check.max_retries + 1,
                        operation=f"Health check for {service.name} on {node_name}",
                        delay=service.health_check.retry_delay
                    )
                
                # 执行健康检查并显示详细进度
                self.logger.info(f"🔍 Running health checks for {service.name} on {node_name} (attempt {attempt + 1})")
                check_results = checker.run_comprehensive_check(node_name)

                # 显示各项检查的结果
                for result in check_results:
                    status_icon = "✅" if result.success else "❌"
                    self.logger.info(f"  {status_icon} {result.check_type}: {result.message}")

                # 检查是否所有必需的检查都通过
                required_checks = [r for r in check_results if self._is_required_check(r, service)]
                all_required_passed = all(r.success for r in required_checks)

                if all_required_passed:
                    self.logger.info(f"✅ All health checks passed for {service.name} on {node_name}")
                    node_results = check_results
                    break
                else:
                    # 记录失败的检查
                    failed_checks = [r for r in required_checks if not r.success]
                    failed_types = [r.check_type for r in failed_checks]
                    self.logger.warning(f"❌ Health check attempt {attempt + 1} failed for {service.name} on {node_name}")
                    self.logger.warning(f"   Failed checks: {failed_types}")
                    
                    if attempt == service.health_check.max_retries:
                        node_results = check_results  # 保存最后一次的结果
            
            results[node_name] = node_results

            # 检查系统级超时
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning(f"Health check timeout ({timeout}s) exceeded, stopping remaining nodes for {service.name}")
                break

        return results
    
    def _is_required_check(self, result: HealthCheckResult, service: ServiceDeployment) -> bool:
        """检查是否为必需的健康检查"""
        for check_config in service.health_check.checks:
            if (check_config.get('type') == result.check_type and 
                check_config.get('required', True)):
                return True
        return False
    
    def aggregate_health_results(self, results: Dict[str, Dict[str, List[HealthCheckResult]]]) -> Dict[str, Any]:
        """聚合健康检查结果"""
        summary = {
            'overall_healthy': True,
            'total_services': len(results),
            'healthy_services': 0,
            'unhealthy_services': 0,
            'service_details': {},
            'failed_checks': []
        }
        
        for service_name, node_results in results.items():
            service_healthy = True
            service_details = {
                'nodes': {},
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0
            }
            
            for node_name, check_results in node_results.items():
                node_healthy = True
                node_details = {
                    'checks': [],
                    'healthy': True
                }
                
                for result in check_results:
                    service_details['total_checks'] += 1
                    
                    if result.success:
                        service_details['passed_checks'] += 1
                    else:
                        service_details['failed_checks'] += 1
                        node_healthy = False
                        summary['failed_checks'].append({
                            'service': service_name,
                            'node': node_name,
                            'check_type': result.check_type,
                            'message': result.message
                        })
                    
                    node_details['checks'].append({
                        'type': result.check_type,
                        'success': result.success,
                        'message': result.message,
                        'duration': result.duration
                    })
                
                node_details['healthy'] = node_healthy
                service_details['nodes'][node_name] = node_details
                
                if not node_healthy:
                    service_healthy = False
            
            if service_healthy:
                summary['healthy_services'] += 1
            else:
                summary['unhealthy_services'] += 1
                summary['overall_healthy'] = False
            
            service_details['healthy'] = service_healthy
            summary['service_details'][service_name] = service_details
        
        return summary
    
    def run_batch_health_checks(self, services: List[ServiceDeployment], parallel: bool = True, max_workers: int = 4, timeout: int = None) -> Dict[str, Dict[str, List[HealthCheckResult]]]:
        """批量运行多个服务的健康检查"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        
        if parallel and len(services) > 1:
            # 并行执行健康检查
            with ThreadPoolExecutor(max_workers=min(len(services), max_workers)) as executor:
                future_to_service = {
                    executor.submit(self.run_service_health_check, service, timeout): service.name
                    for service in services
                }
                
                for future in as_completed(future_to_service):
                    service_name = future_to_service[future]
                    try:
                        service_results = future.result()
                        results[service_name] = service_results
                        self.logger.info(f"Completed health check for service: {service_name}")
                    except Exception as e:
                        self.logger.error(f"Health check failed for service {service_name}: {e}")
                        results[service_name] = {}
        else:
            # 串行执行健康检查
            for service in services:
                try:
                    service_results = self.run_service_health_check(service, timeout)
                    results[service.name] = service_results
                    self.logger.info(f"Completed health check for service: {service.name}")
                except Exception as e:
                    self.logger.error(f"Health check failed for service {service.name}: {e}")
                    results[service.name] = {}
        
        return results