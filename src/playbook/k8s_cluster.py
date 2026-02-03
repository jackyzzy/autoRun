"""Kubernetes集群配置类

封装K8S集群的配置信息和连接参数。与Node类类似，但专门用于K8S集群。
K8S集群也通过SSH连接到集群的一个节点上执行kubectl命令。

主要功能:
    - 封装K8S集群的SSH连接信息
    - 构建kubectl命令
    - 管理命名空间和kubeconfig配置

典型用法:
    >>> config = {
    ...     'host': '192.168.1.200',
    ...     'username': 'k8s-admin',
    ...     'password': 'secret',
    ...     'kubectl_work_dir': '/opt/k8s-manifests',
    ...     'default_namespace': 'default'
    ... }
    >>> cluster = K8SCluster('k8s_prod', config)
    >>> cmd = cluster.build_kubectl_command('apply', '/path/to/deployment.yaml')
    >>> print(cmd)
    'kubectl -n default apply -f /path/to/deployment.yaml'
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from ..utils.ssh_client import SSHClient, ssh_pool
from ..utils.common import setup_module_logger


class K8SCluster:
    """Kubernetes集群配置类

    封装K8S集群的配置信息，包括SSH连接参数和kubectl相关配置。
    K8S集群通过SSH连接到集群的一个节点上执行kubectl命令，
    这与Docker Compose节点的操作模式保持一致。

    Attributes:
        name: 集群名称，用作唯一标识符
        host: 集群节点主机地址或IP（用于SSH连接）
        username: SSH连接用户名
        password: SSH连接密码（可选）
        key_filename: SSH私钥文件路径（可选）
        port: SSH连接端口，默认22
        kubectl_work_dir: kubectl工作目录，用于存放上传的manifest文件
        kubeconfig: kubeconfig文件路径（可选，默认使用节点上的默认配置）
        context: K8S上下文名称（可选）
        default_namespace: 默认命名空间
        tags: 集群标签列表，用于分组和过滤
        enabled: 是否启用此集群

    Examples:
        >>> config = {
        ...     'host': '192.168.1.200',
        ...     'username': 'k8s-admin',
        ...     'password': 'secret',
        ...     'kubectl_work_dir': '/opt/k8s-manifests',
        ...     'default_namespace': 'default'
        ... }
        >>> cluster = K8SCluster('k8s_prod', config)
        >>> print(f"{cluster.name}: {cluster.host}")
        k8s_prod: 192.168.1.200
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        """初始化K8S集群配置

        Args:
            name: 集群名称
            config: 集群配置字典，包含SSH连接信息和kubectl配置
        """
        self.logger = setup_module_logger("playbook.k8s_cluster")

        self.name = name
        self.host = config['host']
        self.username = config['username']
        self.password = config.get('password')
        self.key_filename = config.get('key_filename')
        self.port = config.get('port', 22)

        # K8S特有配置
        self.kubectl_work_dir = config.get('kubectl_work_dir', '/opt/k8s-manifests')
        self.kubeconfig = config.get('kubeconfig')
        self.context = config.get('context')
        self.default_namespace = config.get('default_namespace', 'default')

        # 通用配置
        self.tags = config.get('tags', [])
        self.enabled = config.get('enabled', True)

        self.logger.debug(f"K8SCluster initialized: {name} ({self.host})")

    def get_ssh_client(self) -> SSHClient:
        """获取SSH客户端

        使用全局SSH连接池获取到K8S集群节点的连接。

        Returns:
            SSHClient: SSH客户端实例
        """
        return ssh_pool.get_connection(
            self.host, self.username, self.password,
            self.key_filename, self.port
        )

    def build_kubectl_command(self, action: str, manifest_path: str = None,
                             namespace: str = None,
                             extra_args: List[str] = None) -> str:
        """构建kubectl命令

        根据配置构建完整的kubectl命令字符串。

        Args:
            action: kubectl动作，如 'apply', 'delete', 'get' 等
            manifest_path: manifest文件路径（可选）
            namespace: 命名空间（可选，默认使用default_namespace）
            extra_args: 额外的命令行参数（可选）

        Returns:
            str: 完整的kubectl命令字符串

        Examples:
            >>> cluster.build_kubectl_command('apply', '/path/to/deploy.yaml')
            'kubectl -n default apply -f /path/to/deploy.yaml'

            >>> cluster.build_kubectl_command('get', namespace='kube-system',
            ...                               extra_args=['pods', '-o', 'wide'])
            'kubectl --kubeconfig=/path/to/config -n kube-system get pods -o wide'
        """
        cmd_parts = ['kubectl']

        # 添加kubeconfig（如果配置了）
        if self.kubeconfig:
            cmd_parts.append(f'--kubeconfig={self.kubeconfig}')

        # 添加context（如果配置了）
        if self.context:
            cmd_parts.append(f'--context={self.context}')

        # 添加namespace
        ns = namespace or self.default_namespace
        cmd_parts.append(f'-n {ns}')

        # 添加动作
        cmd_parts.append(action)

        # 添加manifest文件（如果有）
        if manifest_path:
            cmd_parts.append(f'-f {manifest_path}')

        # 添加额外参数
        if extra_args:
            cmd_parts.extend(extra_args)

        return ' '.join(cmd_parts)

    def build_kubectl_get_command(self, resource_type: str, resource_name: str = None,
                                  namespace: str = None, output_format: str = 'json') -> str:
        """构建kubectl get命令

        Args:
            resource_type: 资源类型，如 'deployment', 'pod', 'service'
            resource_name: 资源名称（可选）
            namespace: 命名空间（可选）
            output_format: 输出格式，默认'json'

        Returns:
            str: kubectl get命令字符串
        """
        extra_args = [resource_type]
        if resource_name:
            extra_args.append(resource_name)
        extra_args.extend(['-o', output_format])

        return self.build_kubectl_command('get', namespace=namespace, extra_args=extra_args)

    def build_kubectl_delete_command(self, manifest_path: str, namespace: str = None,
                                     ignore_not_found: bool = True) -> str:
        """构建kubectl delete命令

        Args:
            manifest_path: manifest文件路径
            namespace: 命名空间（可选）
            ignore_not_found: 是否忽略不存在的资源，默认True

        Returns:
            str: kubectl delete命令字符串
        """
        extra_args = []
        if ignore_not_found:
            extra_args.append('--ignore-not-found=true')

        cmd = self.build_kubectl_command('delete', manifest_path, namespace)
        if extra_args:
            cmd += ' ' + ' '.join(extra_args)

        return cmd

    def build_kubectl_logs_command(self, pod_name: str, container: str = None,
                                   namespace: str = None, tail: int = 100) -> str:
        """构建kubectl logs命令

        Args:
            pod_name: Pod名称
            container: 容器名称（可选）
            namespace: 命名空间（可选）
            tail: 尾部行数，默认100

        Returns:
            str: kubectl logs命令字符串
        """
        extra_args = [pod_name]
        if container:
            extra_args.extend(['-c', container])
        extra_args.extend(['--tail', str(tail)])

        return self.build_kubectl_command('logs', namespace=namespace, extra_args=extra_args)

    def build_kubectl_exec_command(self, pod_name: str, command: List[str],
                                   container: str = None, namespace: str = None) -> str:
        """构建kubectl exec命令

        Args:
            pod_name: Pod名称
            command: 要执行的命令列表
            container: 容器名称（可选）
            namespace: 命名空间（可选）

        Returns:
            str: kubectl exec命令字符串
        """
        extra_args = [pod_name]
        if container:
            extra_args.extend(['-c', container])
        extra_args.append('--')
        extra_args.extend(command)

        return self.build_kubectl_command('exec', namespace=namespace, extra_args=extra_args)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典

        Returns:
            Dict[str, Any]: 包含集群配置的字典
        """
        return {
            'name': self.name,
            'host': self.host,
            'username': self.username,
            'port': self.port,
            'kubectl_work_dir': self.kubectl_work_dir,
            'kubeconfig': self.kubeconfig,
            'context': self.context,
            'default_namespace': self.default_namespace,
            'tags': self.tags,
            'enabled': self.enabled
        }

    def __repr__(self) -> str:
        return f"K8SCluster(name='{self.name}', host='{self.host}', namespace='{self.default_namespace}')"
