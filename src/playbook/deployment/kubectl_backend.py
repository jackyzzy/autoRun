"""Kubectl部署后端

实现基于kubectl的K8S服务部署后端。
通过SSH连接到K8S集群节点，执行kubectl命令进行服务部署。

主要功能:
    - 上传K8S manifest文件到远程节点
    - 按步骤执行kubectl apply命令
    - 执行步骤检查（资源存在性、就绪状态）
    - 服务停止和资源清理

典型用法:
    >>> backend = KubectlBackend(node_manager)
    >>> result = backend.deploy_service(scenario_path, service_config, 'k8s_prod')
    >>> if result.success:
    ...     print("Service deployed successfully")
"""

import logging
import time
import json
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from .backend import (
    DeploymentBackend, DeploymentPlatform, DeploymentResult, ServiceStatus
)
from ..k8s_cluster import K8SCluster
from ...utils.common import setup_module_logger


class KubectlBackend(DeploymentBackend):
    """Kubectl部署后端

    实现基于kubectl命令的K8S服务部署。
    与Docker Compose后端类似，通过SSH连接到K8S集群节点执行命令。

    Attributes:
        node_manager: 节点管理器实例
        logger: 日志记录器

    Examples:
        >>> backend = KubectlBackend(node_manager)
        >>> result = backend.deploy_service(
        ...     Path('/config/scenarios/k8s_test'),
        ...     service_config,
        ...     'k8s_prod'
        ... )
    """

    def __init__(self, node_manager):
        """初始化Kubectl后端

        Args:
            node_manager: NodeManager实例，用于获取K8S集群配置和执行SSH命令
        """
        self.logger = setup_module_logger("playbook.kubectl_backend")
        self.node_manager = node_manager

    @property
    def platform(self) -> DeploymentPlatform:
        return DeploymentPlatform.KUBERNETES

    def deploy_service(self, scenario_path: Path, service_config: Dict[str, Any],
                      node_name: str, timeout: int = 300,
                      is_retry: bool = False) -> DeploymentResult:
        """部署K8S服务

        执行流程:
        1. 获取K8S集群配置
        2. 上传manifest文件到远程节点
        3. 按steps顺序执行kubectl apply
        4. 每个step执行后运行对应的check（如果配置了）

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置，包含kubectl配置和steps
            node_name: K8S集群名称
            timeout: 部署超时时间
            is_retry: 是否为重试

        Returns:
            DeploymentResult: 部署结果
        """
        service_name = service_config.get('name', 'unknown')
        start_time = time.time()

        self.logger.info(f"Deploying K8S service: {service_name} to {node_name}")

        try:
            # 1. 获取K8S集群
            cluster = self._get_cluster(node_name)
            if not cluster:
                return DeploymentResult(
                    success=False,
                    service_name=service_name,
                    platform=self.platform,
                    message=f"K8S cluster not found: {node_name}",
                    duration=time.time() - start_time
                )

            # 2. 解析kubectl配置
            kubectl_config = service_config.get('kubectl', {})
            steps = kubectl_config.get('steps', [])
            namespace = kubectl_config.get('namespace', cluster.default_namespace)

            if not steps:
                return DeploymentResult(
                    success=False,
                    service_name=service_name,
                    platform=self.platform,
                    message="No kubectl steps defined in service config",
                    duration=time.time() - start_time
                )

            # 3. 上传manifest文件
            manifests_dir = scenario_path / "manifests"
            remote_dir = f"{cluster.kubectl_work_dir}/{service_name}"

            if not self._upload_manifests(cluster, manifests_dir, remote_dir, steps):
                return DeploymentResult(
                    success=False,
                    service_name=service_name,
                    platform=self.platform,
                    message="Failed to upload manifest files",
                    duration=time.time() - start_time
                )

            # 4. 按步骤执行kubectl apply
            for i, step in enumerate(steps):
                step_name = step.get('name', f'step_{i}')
                manifest_file = step.get('manifest')

                if not manifest_file:
                    self.logger.warning(f"Step '{step_name}' has no manifest file, skipping")
                    continue

                remote_path = f"{remote_dir}/{manifest_file}"
                self.logger.info(f"Executing step: {step_name} ({manifest_file})")

                # 执行kubectl apply
                cmd = cluster.build_kubectl_command('apply', remote_path, namespace)
                success, stdout, stderr = self._execute_ssh(cluster, cmd, timeout)

                if not success:
                    return DeploymentResult(
                        success=False,
                        service_name=service_name,
                        platform=self.platform,
                        message=f"Step '{step_name}' failed: {stderr}",
                        details={'step': step_name, 'stderr': stderr},
                        duration=time.time() - start_time
                    )

                self.logger.debug(f"Step '{step_name}' output: {stdout}")

                # 执行步骤检查（如果配置了）
                check_config = step.get('check')
                if check_config:
                    check_timeout = check_config.get('timeout', 60)
                    if not self._run_step_check(cluster, check_config, namespace, check_timeout):
                        return DeploymentResult(
                            success=False,
                            service_name=service_name,
                            platform=self.platform,
                            message=f"Check for step '{step_name}' failed",
                            details={'step': step_name, 'check': check_config},
                            duration=time.time() - start_time
                        )

            self.logger.info(f"Successfully deployed K8S service: {service_name}")
            return DeploymentResult(
                success=True,
                service_name=service_name,
                platform=self.platform,
                message=f"Successfully deployed {service_name}",
                details={
                    'namespace': namespace,
                    'steps_count': len(steps),
                    'cluster': node_name
                },
                duration=time.time() - start_time
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
        """停止K8S服务

        反向执行kubectl delete命令删除资源。

        Args:
            scenario_path: 场景配置目录路径
            service_config: 服务配置
            node_name: K8S集群名称
            timeout: 停止超时时间

        Returns:
            bool: 停止是否成功
        """
        service_name = service_config.get('name', 'unknown')
        self.logger.info(f"Stopping K8S service: {service_name} on {node_name}")

        try:
            cluster = self._get_cluster(node_name)
            if not cluster:
                self.logger.warning(f"K8S cluster not found: {node_name}")
                return False

            kubectl_config = service_config.get('kubectl', {})
            steps = kubectl_config.get('steps', [])
            namespace = kubectl_config.get('namespace', cluster.default_namespace)
            remote_dir = f"{cluster.kubectl_work_dir}/{service_name}"

            # 反向删除资源
            for step in reversed(steps):
                manifest_file = step.get('manifest')
                if not manifest_file:
                    continue

                step_name = step.get('name', manifest_file)
                remote_path = f"{remote_dir}/{manifest_file}"

                self.logger.debug(f"Deleting resources from: {step_name}")
                cmd = cluster.build_kubectl_delete_command(remote_path, namespace)
                success, stdout, stderr = self._execute_ssh(cluster, cmd, timeout)

                if not success:
                    self.logger.warning(f"Failed to delete {step_name}: {stderr}")
                    # 继续删除其他资源，不立即返回

            self.logger.info(f"Stopped K8S service: {service_name}")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping service {service_name}: {e}")
            return False

    def get_service_status(self, service_name: str, node_name: str,
                          context: Dict[str, Any] = None) -> ServiceStatus:
        """获取K8S服务状态

        Args:
            service_name: 服务名称
            node_name: K8S集群名称
            context: 上下文，包含namespace、kind等信息

        Returns:
            ServiceStatus: 服务状态
        """
        context = context or {}
        namespace = context.get('namespace', 'default')
        resource_type = context.get('kind', 'deployment')

        try:
            cluster = self._get_cluster(node_name)
            if not cluster:
                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=False,
                    ready=False,
                    details={'error': f'K8S cluster not found: {node_name}'}
                )

            # 构建并执行kubectl get命令
            cmd = cluster.build_kubectl_get_command(
                resource_type, service_name, namespace, 'json'
            )
            success, output, error = self._execute_ssh(cluster, cmd)

            if not success:
                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=False,
                    ready=False,
                    details={'error': error}
                )

            # 解析JSON输出
            resource_data = json.loads(output)
            status = resource_data.get('status', {})

            # 根据资源类型解析状态
            if resource_type.lower() in ['deployment', 'statefulset', 'replicaset']:
                replicas = status.get('replicas', 0)
                ready_replicas = status.get('readyReplicas', 0)

                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=replicas > 0,
                    ready=ready_replicas == replicas and replicas > 0,
                    replicas=replicas,
                    ready_replicas=ready_replicas,
                    details={
                        'conditions': status.get('conditions', []),
                        'availableReplicas': status.get('availableReplicas', 0)
                    }
                )
            else:
                # 其他资源类型
                return ServiceStatus(
                    service_name=service_name,
                    platform=self.platform,
                    running=True,
                    ready=True,
                    details={'raw_status': status}
                )

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse status JSON for {service_name}: {e}")
            return ServiceStatus(
                service_name=service_name,
                platform=self.platform,
                running=False,
                ready=False,
                details={'error': f'JSON parse error: {e}'}
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
        """执行K8S服务健康检查

        Args:
            service_name: 服务名称
            node_name: K8S集群名称
            check_config: 健康检查配置
            context: 上下文信息

        Returns:
            bool: 健康检查是否通过
        """
        context = context or {}
        namespace = context.get('namespace', 'default')
        check_type = check_config.get('type', 'deployment_ready')

        cluster = self._get_cluster(node_name)
        if not cluster:
            return False

        if check_type == 'deployment_ready':
            name = check_config.get('name', service_name)
            return self._check_deployment_ready(cluster, name, namespace)
        elif check_type == 'pod_ready':
            selector = check_config.get('selector', f'app={service_name}')
            min_ready = check_config.get('min_ready', 1)
            return self._check_pods_ready(cluster, selector, namespace, min_ready)
        elif check_type == 'resource_exists':
            kind = check_config.get('kind', 'deployment')
            name = check_config.get('name', service_name)
            return self._check_resource_exists(cluster, kind, name, namespace)
        else:
            self.logger.warning(f"Unknown health check type: {check_type}")
            return True

    def cleanup(self, scenario_path: Path, service_configs: List[Dict[str, Any]],
               node_name: str, force: bool = False) -> bool:
        """清理K8S服务资源

        Args:
            scenario_path: 场景配置目录
            service_configs: 服务配置列表
            node_name: K8S集群名称
            force: 是否强制清理

        Returns:
            bool: 清理是否成功
        """
        success = True

        for service_config in service_configs:
            service_name = service_config.get('name', 'unknown')

            try:
                # 停止服务
                if not self.stop_service(scenario_path, service_config, node_name):
                    success = False

                # 删除上传的manifest文件
                cluster = self._get_cluster(node_name)
                if cluster:
                    remote_dir = f"{cluster.kubectl_work_dir}/{service_name}"
                    cmd = f"rm -rf {remote_dir}"
                    self._execute_ssh(cluster, cmd)

            except Exception as e:
                self.logger.error(f"Cleanup failed for {service_name}: {e}")
                success = False

        return success

    def validate_config(self, service_config: Dict[str, Any]) -> Dict[str, List[str]]:
        """验证K8S服务配置

        Args:
            service_config: 服务配置

        Returns:
            Dict包含'errors'和'warnings'列表
        """
        errors = []
        warnings = []

        kubectl_config = service_config.get('kubectl', {})

        # 检查steps配置
        steps = kubectl_config.get('steps', [])
        if not steps:
            errors.append("Missing 'kubectl.steps' configuration")

        # 检查每个step
        for i, step in enumerate(steps):
            step_name = step.get('name', f'step_{i}')
            if not step.get('manifest'):
                errors.append(f"Step '{step_name}' missing 'manifest' field")

        # 警告：没有配置namespace
        if not kubectl_config.get('namespace'):
            warnings.append("No namespace specified, will use cluster default")

        return {'errors': errors, 'warnings': warnings}

    # ===== 私有方法 =====

    def _get_cluster(self, node_name: str) -> Optional[K8SCluster]:
        """获取K8S集群实例"""
        return self.node_manager.get_k8s_cluster(node_name)

    def _execute_ssh(self, cluster: K8SCluster, command: str,
                    timeout: int = 300) -> Tuple[bool, str, str]:
        """通过SSH执行命令

        Args:
            cluster: K8S集群实例
            command: 要执行的命令
            timeout: 超时时间

        Returns:
            Tuple[bool, str, str]: (成功, stdout, stderr)
        """
        try:
            client = cluster.get_ssh_client()
            with client.connection_context():
                exit_code, stdout, stderr = client.execute_command(command, timeout=timeout)
                return exit_code == 0, stdout, stderr
        except Exception as e:
            self.logger.error(f"SSH execution failed: {e}")
            return False, "", str(e)

    def _upload_manifests(self, cluster: K8SCluster, local_dir: Path,
                         remote_dir: str, steps: List[Dict]) -> bool:
        """上传manifest文件到远程节点

        Args:
            cluster: K8S集群实例
            local_dir: 本地manifest目录
            remote_dir: 远程目标目录
            steps: 步骤列表，用于确定需要上传哪些文件

        Returns:
            bool: 上传是否成功
        """
        try:
            client = cluster.get_ssh_client()

            with client.connection_context():
                # 创建远程目录
                mkdir_cmd = f"mkdir -p {remote_dir}"
                exit_code, _, stderr = client.execute_command(mkdir_cmd)
                if exit_code != 0:
                    self.logger.error(f"Failed to create remote directory: {stderr}")
                    return False

                # 收集需要上传的文件
                files_to_upload = set()
                for step in steps:
                    manifest = step.get('manifest')
                    if manifest:
                        files_to_upload.add(manifest)

                # 上传文件
                for filename in files_to_upload:
                    local_path = local_dir / filename
                    remote_path = f"{remote_dir}/{filename}"

                    if not local_path.exists():
                        self.logger.error(f"Manifest file not found: {local_path}")
                        return False

                    self.logger.debug(f"Uploading {local_path} to {remote_path}")
                    success = client.upload_file(str(local_path), remote_path)
                    if not success:
                        self.logger.error(f"Failed to upload {filename}")
                        return False

                self.logger.info(f"Uploaded {len(files_to_upload)} manifest files to {remote_dir}")
                return True

        except Exception as e:
            self.logger.error(f"Failed to upload manifests: {e}")
            return False

    def _run_step_check(self, cluster: K8SCluster, check_config: Dict,
                       namespace: str, timeout: int) -> bool:
        """执行步骤检查

        Args:
            cluster: K8S集群实例
            check_config: 检查配置
            namespace: 命名空间
            timeout: 超时时间

        Returns:
            bool: 检查是否通过
        """
        check_type = check_config.get('type')
        start_time = time.time()
        check_interval = 5

        while time.time() - start_time < timeout:
            result = False

            if check_type == 'resource_exists':
                kind = check_config.get('kind')
                name = check_config.get('name')
                result = self._check_resource_exists(cluster, kind, name, namespace)

            elif check_type == 'resource_ready':
                kind = check_config.get('kind')
                name = check_config.get('name')
                result = self._check_resource_ready(cluster, kind, name, namespace)

            elif check_type == 'deployment_ready':
                name = check_config.get('name')
                replicas = check_config.get('replicas')
                result = self._check_deployment_ready(cluster, name, namespace, replicas)

            elif check_type == 'pod_ready':
                selector = check_config.get('selector')
                min_ready = check_config.get('min_ready', 1)
                result = self._check_pods_ready(cluster, selector, namespace, min_ready)

            else:
                self.logger.warning(f"Unknown check type: {check_type}")
                return True

            if result:
                return True

            self.logger.debug(f"Check not passed yet, waiting {check_interval}s...")
            time.sleep(check_interval)

        self.logger.warning(f"Check timeout after {timeout}s")
        return False

    def _check_resource_exists(self, cluster: K8SCluster, kind: str,
                              name: str, namespace: str) -> bool:
        """检查资源是否存在"""
        cmd = cluster.build_kubectl_get_command(kind, name, namespace)
        success, _, _ = self._execute_ssh(cluster, cmd)
        return success

    def _check_resource_ready(self, cluster: K8SCluster, kind: str,
                             name: str, namespace: str) -> bool:
        """检查资源是否就绪"""
        if kind.lower() == 'persistentvolumeclaim':
            return self._check_pvc_bound(cluster, name, namespace)
        elif kind.lower() in ['deployment', 'statefulset']:
            return self._check_deployment_ready(cluster, name, namespace)
        else:
            # 默认只检查存在性
            return self._check_resource_exists(cluster, kind, name, namespace)

    def _check_pvc_bound(self, cluster: K8SCluster, name: str, namespace: str) -> bool:
        """检查PVC是否已绑定"""
        cmd = cluster.build_kubectl_get_command('pvc', name, namespace, 'json')
        success, output, _ = self._execute_ssh(cluster, cmd)

        if not success:
            return False

        try:
            pvc = json.loads(output)
            phase = pvc.get('status', {}).get('phase', '')
            return phase == 'Bound'
        except json.JSONDecodeError:
            return False

    def _check_deployment_ready(self, cluster: K8SCluster, name: str,
                               namespace: str, expected_replicas: int = None) -> bool:
        """检查Deployment是否就绪"""
        cmd = cluster.build_kubectl_get_command('deployment', name, namespace, 'json')
        success, output, _ = self._execute_ssh(cluster, cmd)

        if not success:
            return False

        try:
            dep = json.loads(output)
            status = dep.get('status', {})
            replicas = status.get('replicas', 0)
            ready_replicas = status.get('readyReplicas', 0)

            if expected_replicas is not None:
                return ready_replicas >= expected_replicas
            else:
                return replicas > 0 and ready_replicas == replicas
        except json.JSONDecodeError:
            return False

    def _check_pods_ready(self, cluster: K8SCluster, selector: str,
                         namespace: str, min_ready: int) -> bool:
        """检查Pod是否就绪"""
        cmd = f"kubectl -n {namespace} get pods -l {selector} -o json"
        if cluster.kubeconfig:
            cmd = f"kubectl --kubeconfig={cluster.kubeconfig} " + cmd[8:]

        success, output, _ = self._execute_ssh(cluster, cmd)

        if not success:
            return False

        try:
            pods_data = json.loads(output)
            pods = pods_data.get('items', [])

            ready_count = 0
            for pod in pods:
                conditions = pod.get('status', {}).get('conditions', [])
                for cond in conditions:
                    if cond.get('type') == 'Ready' and cond.get('status') == 'True':
                        ready_count += 1
                        break

            return ready_count >= min_ready
        except json.JSONDecodeError:
            return False
