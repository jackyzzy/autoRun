"""
结果传输器
负责处理测试结果文件的传输、下载和文件操作
"""

import logging
import shutil
import warnings
import time
import hashlib
from typing import List, Optional
from pathlib import Path
from datetime import datetime

from ..node_manager import NodeManager
from ..scenario_manager import Scenario
from .result_models import CollectionTask, CollectionSummary
from ...utils.exception_handler import result_collection
from ..exceptions import ResultCollectionError


class ResultTransporter:
    """结果文件传输器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.result_transporter")
        self.node_manager = node_manager

    def _ensure_writable_directory(self, dir_path: Path) -> bool:
        """确保目录存在且可写

        Args:
            dir_path: 目标目录路径

        Returns:
            bool: 目录创建并可写返回True，否则返回False
        """
        try:
            dir_path.mkdir(parents=True, exist_ok=True)

            # 验证写权限
            test_file = dir_path / ".write_test"
            test_file.touch()
            test_file.unlink()

            return True
        except (OSError, PermissionError) as e:
            self.logger.error(f"Cannot create writable directory {dir_path}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error creating directory {dir_path}: {e}")
            return False

    def _check_disk_space(self, target_dir: Path, required_bytes: int) -> bool:
        """检查磁盘空间是否足够

        Args:
            target_dir: 目标目录
            required_bytes: 需要的字节数

        Returns:
            bool: 空间足够返回True，否则返回False
        """
        try:
            stat = shutil.disk_usage(target_dir)
            available = stat.free
            buffer_ratio = 1.1  # 10%安全缓冲

            if available < required_bytes * buffer_ratio:
                self.logger.error(
                    f"Insufficient disk space: need {required_bytes / (1024*1024):.1f}MB, "
                    f"available {available / (1024*1024):.1f}MB"
                )
                return False

            return True
        except Exception as e:
            self.logger.warning(f"Cannot check disk space for {target_dir}: {e}")
            return True  # 如果无法检查，继续尝试

    def _verify_file_integrity(self, source: Path, target: Path) -> bool:
        """验证文件完整性

        Args:
            source: 源文件路径
            target: 目标文件路径

        Returns:
            bool: 文件完整返回True，否则返回False
        """
        try:
            # 首先检查文件大小
            source_size = source.stat().st_size
            target_size = target.stat().st_size

            if source_size != target_size:
                self.logger.error(
                    f"File size mismatch: source={source_size}, target={target_size} for {source.name}"
                )
                return False

            # 对于小文件(<10MB)进行MD5校验
            if source_size < 10 * 1024 * 1024:
                with open(source, 'rb') as f1, open(target, 'rb') as f2:
                    hash1 = hashlib.md5(f1.read()).hexdigest()
                    hash2 = hashlib.md5(f2.read()).hexdigest()
                    if hash1 != hash2:
                        self.logger.error(f"MD5 hash mismatch for {source.name}")
                        return False

            return True
        except Exception as e:
            self.logger.error(f"Integrity verification failed for {source.name}: {e}")
            return False

    @result_collection(return_on_error=CollectionSummary(collection_time=datetime.now().isoformat(), mode="artifacts"))
    def collect_artifacts_from_nodes(self, artifacts: List[str], node_names: List[str],
                                   local_base_dir: Path) -> CollectionSummary:
        """从节点收集测试artifacts

        .. deprecated::
            此方法已废弃，请使用 collect_artifacts_from_test_node() 代替。
            该方法存在逻辑错误：从所有参与节点收集artifacts，而实际应该只从测试执行节点收集。
        """
        warnings.warn(
            "collect_artifacts_from_nodes() is deprecated. "
            "Use collect_artifacts_from_test_node() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        collection_summary = CollectionSummary(
            collection_time=datetime.now().isoformat(),
            mode="artifacts"
        )
        
        for node_name in node_names:
            node_dir = local_base_dir / node_name
            node_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                collected_artifacts = self._download_artifacts_from_node(
                    node_name, artifacts, node_dir
                )
                
                # 更新统计
                collection_summary.total_files_collected += len(collected_artifacts)
                for artifact_file in collected_artifacts:
                    if Path(artifact_file).exists():
                        file_size = Path(artifact_file).stat().st_size
                        collection_summary.total_size_mb += file_size / (1024 * 1024)
                
                # 记录节点结果
                collection_summary.node_results[node_name] = {
                    'artifacts_collected': len(collected_artifacts),
                    'files': [Path(f).name for f in collected_artifacts]
                }
                
            except Exception as e:
                error_msg = f"Failed to collect artifacts from {node_name}: {e}"
                self.logger.error(error_msg)
                collection_summary.collection_errors.append(error_msg)
        
        return collection_summary

    @result_collection(return_on_error=CollectionSummary(collection_time=datetime.now().isoformat(), mode="artifacts"))
    def collect_artifacts_from_test_node(self, artifacts: List[str], test_node: str,
                                       artifacts_dir: Path) -> CollectionSummary:
        """从测试执行节点收集artifacts到artifacts目录

        Args:
            artifacts: 要收集的artifact文件路径列表
            test_node: 测试执行节点名称，"local" 表示本地节点
            artifacts_dir: artifacts存储目录

        Returns:
            CollectionSummary: 收集结果摘要
        """
        collection_summary = CollectionSummary(
            collection_time=datetime.now().isoformat(),
            mode="artifacts"
        )

        self.logger.info(f"Collecting {len(artifacts)} artifacts from test execution node: {test_node}")

        try:
            if test_node == "local":
                collected_artifacts = self._collect_artifacts_local(artifacts, artifacts_dir)
            else:
                collected_artifacts = self._collect_artifacts_remote(artifacts, test_node, artifacts_dir)

            # 更新统计
            collection_summary.total_files_collected = len(collected_artifacts)
            for artifact_file in collected_artifacts:
                if Path(artifact_file).exists():
                    file_size = Path(artifact_file).stat().st_size
                    collection_summary.total_size_mb += file_size / (1024 * 1024)

            # 记录结果
            collection_summary.node_results[test_node] = {
                'artifacts_collected': len(collected_artifacts),
                'files': [Path(f).name for f in collected_artifacts]
            }

        except Exception as e:
            error_msg = f"Failed to collect artifacts from test node {test_node}: {e}"
            self.logger.error(error_msg)
            collection_summary.collection_errors.append(error_msg)

        return collection_summary

    def _collect_artifacts_local(self, artifacts: List[str], artifacts_dir: Path) -> List[str]:
        """从本地节点收集artifacts（直接文件复制）

        Args:
            artifacts: 要收集的artifact文件路径列表
            artifacts_dir: artifacts存储目录

        Returns:
            List[str]: 成功收集的文件路径列表

        Raises:
            ResultCollectionError: 当目录创建失败或磁盘空间不足时
        """
        collected_files = []

        # 预检查目录权限
        if not self._ensure_writable_directory(artifacts_dir):
            raise ResultCollectionError("local", str(artifacts_dir), "Cannot create writable directory")

        for artifact_path in artifacts:
            start_time = time.time()
            try:
                source_path = Path(artifact_path)
                if not source_path.exists():
                    self.logger.debug(f"Local artifact not found: {artifact_path}")
                    continue

                # 预检查磁盘空间
                if source_path.is_file():
                    file_size = source_path.stat().st_size
                    if not self._check_disk_space(artifacts_dir, file_size):
                        raise ResultCollectionError("local", artifact_path, "Insufficient disk space")
                elif source_path.is_dir():
                    # 估算目录大小
                    dir_size = sum(f.stat().st_size for f in source_path.rglob('*') if f.is_file())
                    if not self._check_disk_space(artifacts_dir, dir_size):
                        raise ResultCollectionError("local", artifact_path, "Insufficient disk space for directory")

                # 计算目标文件路径
                artifact_name = source_path.name
                target_file = artifacts_dir / artifact_name

                # 执行复制操作
                if source_path.is_file():
                    shutil.copy2(source_path, target_file)
                elif source_path.is_dir():
                    shutil.copytree(source_path, target_file, dirs_exist_ok=True)
                elif source_path.is_symlink():
                    # 处理符号链接
                    if source_path.exists():  # 检查链接目标是否存在
                        shutil.copy2(source_path, target_file)
                        self.logger.debug(f"Copied symlink: {artifact_path} -> {target_file}")
                    else:
                        self.logger.warning(f"Broken symlink skipped: {artifact_path}")
                        continue
                else:
                    self.logger.warning(f"Unknown file type skipped: {artifact_path}")
                    continue

                # 验证完整性（仅对普通文件）
                if source_path.is_file() and not self._verify_file_integrity(source_path, target_file):
                    # 清理损坏的文件
                    target_file.unlink(missing_ok=True)
                    raise ResultCollectionError("local", str(source_path), "File integrity check failed")

                collected_files.append(str(target_file))

                # 记录详细日志
                duration = time.time() - start_time
                if source_path.is_file():
                    file_size = source_path.stat().st_size
                    self.logger.info(
                        f"Successfully collected: {artifact_path} -> {target_file} "
                        f"({file_size} bytes, {duration:.2f}s)"
                    )
                else:
                    self.logger.info(
                        f"Successfully collected directory: {artifact_path} -> {target_file} "
                        f"({duration:.2f}s)"
                    )

            except (FileNotFoundError, PermissionError) as e:
                self.logger.error(f"File access error for {artifact_path}: {e}")
            except shutil.Error as e:
                self.logger.error(f"Copy operation failed for {artifact_path}: {e}")
            except OSError as e:
                self.logger.error(f"System error collecting {artifact_path}: {e}")
            except ResultCollectionError:
                raise  # 重新抛出业务异常
            except Exception as e:
                self.logger.error(f"Unexpected error collecting {artifact_path}: {type(e).__name__}: {e}")

        return collected_files

    def _collect_artifacts_remote(self, artifacts: List[str], node_name: str,
                                artifacts_dir: Path) -> List[str]:
        """从远程节点收集artifacts（SSH下载）

        Args:
            artifacts: 要收集的artifact文件路径列表
            node_name: 远程节点名称
            artifacts_dir: artifacts存储目录

        Returns:
            List[str]: 成功收集的文件路径列表

        Raises:
            ResultCollectionError: 当目录创建失败、节点不存在或连接失败时
        """
        collected_files = []

        # 预检查目录权限
        if not self._ensure_writable_directory(artifacts_dir):
            raise ResultCollectionError(node_name, str(artifacts_dir), "Cannot create writable directory")

        node = self.node_manager.get_node(node_name)
        if not node:
            raise ResultCollectionError(node_name, "<node_lookup>", "Node not found")

        max_retries = 3
        retry_delay = 2  # 秒
        operation_start_time = time.time()

        for retry in range(max_retries):
            try:
                client = node.get_ssh_client()
                self.logger.debug(f"Attempting remote collection from {node_name} (attempt {retry + 1}/{max_retries})")

                with client.connection_context():
                    for artifact_path in artifacts:
                        start_time = time.time()
                        try:
                            # 检查远程文件是否存在
                            if not client.file_exists(artifact_path):
                                self.logger.debug(f"Remote artifact not found on {node_name}: {artifact_path}")
                                continue

                            # 计算本地文件路径
                            artifact_name = Path(artifact_path).name
                            local_file = artifacts_dir / artifact_name

                            # 下载文件
                            success = client.download_file(artifact_path, str(local_file))
                            if success:
                                collected_files.append(str(local_file))

                                # 记录详细日志
                                duration = time.time() - start_time
                                if local_file.exists():
                                    file_size = local_file.stat().st_size
                                    self.logger.info(
                                        f"Downloaded artifact: {artifact_path} -> {local_file} "
                                        f"({file_size} bytes, {duration:.2f}s)"
                                    )
                                else:
                                    self.logger.info(f"Downloaded artifact: {artifact_path} -> {local_file} ({duration:.2f}s)")
                            else:
                                self.logger.warning(f"Failed to download artifact: {artifact_path}")

                        except Exception as e:
                            self.logger.error(f"Error downloading artifact {artifact_path} from {node_name}: {e}")

                # 成功完成，退出重试循环
                operation_duration = time.time() - operation_start_time
                self.logger.info(f"Remote collection completed from {node_name} in {operation_duration:.2f}s")
                break

            except (ConnectionError, TimeoutError) as e:
                if retry < max_retries - 1:
                    delay = retry_delay * (retry + 1)  # 递增延迟
                    self.logger.warning(
                        f"Connection error collecting from {node_name} (retry {retry + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    operation_duration = time.time() - operation_start_time
                    raise ResultCollectionError(
                        node_name,
                        "<artifacts_collection>",
                        f"Failed after {max_retries} retries (total time: {operation_duration:.2f}s): {e}"
                    )
            except Exception as e:
                operation_duration = time.time() - operation_start_time
                raise ResultCollectionError(
                    node_name,
                    "<artifacts_collection>",
                    f"Collection error after {operation_duration:.2f}s: {e}"
                )

        return collected_files

    def collect_service_logs(self, node_names: List[str], local_base_dir: Path,
                           collection_summary: CollectionSummary, scenario: Optional[Scenario] = None):
        """收集服务日志（支持Docker节点和K8S集群）"""
        self.logger.info(f"Collecting service logs from {len(node_names)} nodes")

        # Docker compose服务日志配置
        compose_commands = [
            {"type": "logs", "params": {"lines": 2000}, "file_name": "compose_logs.txt"},
            {"type": "ps", "params": {}, "file_name": "compose_status.txt"}
        ]

        for node_name in node_names:
            node_dir = local_base_dir / "logs" / node_name
            node_dir.mkdir(parents=True, exist_ok=True)

            try:
                # 检查是否为K8S集群
                if self.node_manager.is_k8s_cluster(node_name):
                    # K8S集群：收集K8S日志
                    self._collect_k8s_logs(node_name, node_dir, scenario)
                    continue

                # 获取Docker节点
                node = self.node_manager.get_node(node_name)
                if not node:
                    self.logger.error(f"Node {node_name} not found (not a Docker node or K8S cluster)")
                    continue
                
                for cmd_config in compose_commands:
                    try:
                        # 获取 compose 文件信息
                        compose_file = "docker-compose.yml"  # 默认值
                        if scenario and scenario.metadata and scenario.metadata.services:
                            # 使用第一个服务的 compose_file，假设所有服务使用相同的 compose 文件
                            compose_file = scenario.metadata.services[0].compose_file

                        # 构建命令参数，包含必需的 file 参数
                        cmd_params = {
                            "file": compose_file,
                            **cmd_config["params"]
                        }

                        # 使用适配器构建版本兼容的命令
                        compose_cmd = self.node_manager.build_compose_command(
                            node_name,
                            cmd_config["type"],
                            **cmd_params
                        )

                        full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"

                        # 对于 logs 命令，临时调整相关日志级别以避免控制台输出
                        if cmd_config["type"] == "logs":
                            # 临时提高相关日志级别，抑制大量日志输出
                            ssh_logger = logging.getLogger(f"ssh.{node.host}")
                            node_logger = logging.getLogger("playbook.node_manager")

                            original_ssh_level = ssh_logger.level
                            original_node_level = node_logger.level

                            ssh_logger.setLevel(logging.ERROR)
                            node_logger.setLevel(logging.WARNING)

                            try:
                                results = self.node_manager.execute_command(
                                    full_cmd, [node_name], timeout=60
                                )
                            finally:
                                # 恢复原始日志级别
                                ssh_logger.setLevel(original_ssh_level)
                                node_logger.setLevel(original_node_level)
                        else:
                            results = self.node_manager.execute_command(
                                full_cmd, [node_name], timeout=60
                            )
                        node_result = results.get(node_name)

                        if node_result and node_result[0] == 0:
                            log_file = node_dir / cmd_config["file_name"]
                            with open(log_file, 'w', encoding='utf-8') as f:
                                f.write(f"# Service log: {cmd_config['file_name']} from {node_name}\\n")
                                f.write(f"# Command: {compose_cmd.full_cmd}\\n")
                                f.write(f"# Compose version: {compose_cmd.version.value}\\n")
                                f.write(f"# Collected at: {datetime.now().isoformat()}\\n\\n")
                                f.write(node_result[1])

                            # 更新统计
                            log_size = log_file.stat().st_size
                            collection_summary.total_files_collected += 1
                            collection_summary.total_size_mb += log_size / (1024 * 1024)

                    except Exception as e:
                        error_msg = f"Failed to collect {cmd_config['type']} from {node_name}: {e}"
                        self.logger.warning(error_msg)
                        collection_summary.collection_errors.append(error_msg)

            except Exception as e:
                error_msg = f"Failed to collect service logs from {node_name}: {e}"
                self.logger.warning(error_msg)
                collection_summary.collection_errors.append(error_msg)

    def _collect_k8s_logs(self, cluster_name: str, node_dir: Path, scenario: Optional[Scenario] = None):
        """收集K8S集群的Pod日志

        Args:
            cluster_name: K8S集群名称
            node_dir: 日志存储目录
            scenario: 场景对象（可选，用于获取服务配置）
        """
        try:
            cluster = self.node_manager.get_k8s_cluster(cluster_name)
            if not cluster:
                self.logger.warning(f"K8S cluster {cluster_name} not found for log collection")
                return

            ssh_client = cluster.get_ssh_client()

            # 获取服务配置中的selector（如果有）
            selector = None
            namespace = cluster.default_namespace
            if scenario and scenario.metadata and scenario.metadata.services:
                for service in scenario.metadata.services:
                    if hasattr(service, 'kubectl') and service.kubectl:
                        namespace = service.kubectl.get('namespace', namespace)
                        # 从健康检查配置中获取selector
                        if service.health_check and service.health_check.checks:
                            for check in service.health_check.checks:
                                if check.get('type') == 'pod_ready' and 'selector' in check:
                                    selector = check['selector']
                                    break

            # 1. 收集Pod列表和状态
            pod_list_cmd = f"kubectl get pods -n {namespace} -o wide"
            if selector:
                pod_list_cmd = f"kubectl get pods -l {selector} -n {namespace} -o wide"
            if cluster.kubeconfig:
                pod_list_cmd = f"kubectl --kubeconfig={cluster.kubeconfig} " + pod_list_cmd.replace("kubectl ", "")

            result = ssh_client.execute_command(pod_list_cmd)
            if result[0] == 0:
                pod_status_file = node_dir / "k8s_pod_status.txt"
                with open(pod_status_file, 'w', encoding='utf-8') as f:
                    f.write(f"# K8S Pod Status from cluster {cluster_name}\n")
                    f.write(f"# Namespace: {namespace}\n")
                    f.write(f"# Selector: {selector or 'all pods'}\n")
                    f.write(f"# Collected at: {datetime.now().isoformat()}\n\n")
                    f.write(result[1])
                self.logger.info(f"Collected K8S pod status from {cluster_name}")

            # 2. 收集Pod描述（events等详细信息）
            pod_describe_cmd = f"kubectl describe pods -n {namespace}"
            if selector:
                pod_describe_cmd = f"kubectl describe pods -l {selector} -n {namespace}"
            if cluster.kubeconfig:
                pod_describe_cmd = f"kubectl --kubeconfig={cluster.kubeconfig} " + pod_describe_cmd.replace("kubectl ", "")

            result = ssh_client.execute_command(pod_describe_cmd)
            if result[0] == 0:
                pod_describe_file = node_dir / "k8s_pod_describe.txt"
                with open(pod_describe_file, 'w', encoding='utf-8') as f:
                    f.write(f"# K8S Pod Describe from cluster {cluster_name}\n")
                    f.write(f"# Namespace: {namespace}\n")
                    f.write(f"# Collected at: {datetime.now().isoformat()}\n\n")
                    f.write(result[1])
                self.logger.info(f"Collected K8S pod describe from {cluster_name}")

            # 3. 收集Pod日志（最近1000行）
            # 首先获取Pod名称列表
            pod_names_cmd = f"kubectl get pods -n {namespace} -o jsonpath='{{.items[*].metadata.name}}'"
            if selector:
                pod_names_cmd = f"kubectl get pods -l {selector} -n {namespace} -o jsonpath='{{.items[*].metadata.name}}'"
            if cluster.kubeconfig:
                pod_names_cmd = f"kubectl --kubeconfig={cluster.kubeconfig} " + pod_names_cmd.replace("kubectl ", "")

            result = ssh_client.execute_command(pod_names_cmd)
            if result[0] == 0 and result[1].strip():
                pod_names = result[1].strip().split()
                for pod_name in pod_names[:10]:  # 限制最多收集10个Pod的日志
                    log_cmd = f"kubectl logs {pod_name} -n {namespace} --tail=1000"
                    if cluster.kubeconfig:
                        log_cmd = f"kubectl --kubeconfig={cluster.kubeconfig} logs {pod_name} -n {namespace} --tail=1000"

                    log_result = ssh_client.execute_command(log_cmd)
                    if log_result[0] == 0:
                        pod_log_file = node_dir / f"k8s_pod_log_{pod_name}.txt"
                        with open(pod_log_file, 'w', encoding='utf-8') as f:
                            f.write(f"# K8S Pod Log: {pod_name}\n")
                            f.write(f"# Cluster: {cluster_name}\n")
                            f.write(f"# Namespace: {namespace}\n")
                            f.write(f"# Collected at: {datetime.now().isoformat()}\n\n")
                            f.write(log_result[1])

            self.logger.info(f"K8S log collection completed for cluster {cluster_name}")

        except Exception as e:
            self.logger.warning(f"Failed to collect K8S logs from {cluster_name}: {e}")

    def collect_system_logs(self, node_names: List[str], local_base_dir: Path,
                          collection_summary: CollectionSummary):
        """收集系统日志（支持Docker节点和K8S集群）"""
        self.logger.info(f"Collecting system logs from {len(node_names)} nodes")

        # Docker节点的系统日志命令
        docker_system_log_commands = {
            "docker_info.txt": "docker info",
            "docker_images.txt": "docker images --format 'table {{.Repository}}\\t{{.Tag}}\\t{{.Size}}\\t{{.CreatedAt}}'",
            "docker_containers.txt": "docker ps -a --format 'table {{.Names}}\\t{{.Status}}\\t{{.Image}}\\t{{.CreatedAt}}'",
            "system_resources.txt": "free -h && echo '---' && df -h && echo '---' && top -b -n 1 | head -20"
        }

        # K8S集群的系统日志命令
        k8s_system_log_commands = {
            "k8s_nodes.txt": "kubectl get nodes -o wide",
            "k8s_cluster_info.txt": "kubectl cluster-info",
            "system_resources.txt": "free -h && echo '---' && df -h && echo '---' && top -b -n 1 | head -20"
        }

        for node_name in node_names:
            node_dir = local_base_dir / "logs" / node_name
            node_dir.mkdir(parents=True, exist_ok=True)

            # 判断是否为K8S集群
            is_k8s = self.node_manager.is_k8s_cluster(node_name)
            system_log_commands = k8s_system_log_commands if is_k8s else docker_system_log_commands

            try:
                for log_name, command in system_log_commands.items():
                    try:
                        # 对于K8S集群，需要添加kubeconfig参数
                        if is_k8s and command.startswith("kubectl"):
                            cluster = self.node_manager.get_k8s_cluster(node_name)
                            if cluster and cluster.kubeconfig:
                                command = command.replace("kubectl", f"kubectl --kubeconfig={cluster.kubeconfig}")

                        results = self.node_manager.execute_command(
                            command, [node_name], timeout=30
                        )
                        node_result = results.get(node_name)
                        
                        if node_result and node_result[0] == 0:
                            log_file = node_dir / log_name
                            with open(log_file, 'w', encoding='utf-8') as f:
                                f.write(f"# System log: {log_name} from {node_name}\\n")
                                f.write(f"# Command: {command}\\n")
                                f.write(f"# Collected at: {datetime.now().isoformat()}\\n\\n")
                                f.write(node_result[1])
                            
                            # 更新统计
                            log_size = log_file.stat().st_size
                            collection_summary.total_files_collected += 1
                            collection_summary.total_size_mb += log_size / (1024 * 1024)
                    
                    except Exception as e:
                        error_msg = f"Failed to collect {log_name} from {node_name}: {e}"
                        self.logger.warning(error_msg)
                        collection_summary.collection_errors.append(error_msg)
                
                # 标记该节点的系统日志已收集
                if node_name not in collection_summary.node_results:
                    collection_summary.node_results[node_name] = {
                        'services_collected': [],
                        'system_logs_collected': False,
                        'collection_errors': []
                    }
                collection_summary.node_results[node_name]['system_logs_collected'] = True
            
            except Exception as e:
                error_msg = f"Error collecting system logs from {node_name}: {e}"
                self.logger.error(error_msg)
                collection_summary.collection_errors.append(error_msg)
    
    def _download_artifacts_from_node(self, node_name: str, artifacts: List[str], 
                                    local_dir: Path) -> List[str]:
        """从单个节点下载artifacts"""
        downloaded_files = []
        
        node = self.node_manager.get_node(node_name)
        if not node:
            self.logger.error(f"Node not found: {node_name}")
            return []
        
        client = node.get_ssh_client()
        
        with client.connection_context():
            for artifact_path in artifacts:
                try:
                    # 检查远程文件是否存在
                    if not client.file_exists(artifact_path):
                        self.logger.debug(f"Artifact not found on {node_name}: {artifact_path}")
                        continue
                    
                    # 计算本地文件路径
                    artifact_name = Path(artifact_path).name
                    local_file = local_dir / artifact_name
                    
                    # 下载文件
                    success = client.download_file(artifact_path, str(local_file))
                    if success:
                        downloaded_files.append(str(local_file))
                        self.logger.debug(f"Downloaded artifact: {artifact_path} -> {local_file}")
                    else:
                        self.logger.warning(f"Failed to download artifact: {artifact_path}")
                
                except Exception as e:
                    self.logger.error(f"Error downloading artifact {artifact_path} from {node_name}: {e}")
        
        return downloaded_files
    
    def aggregate_files_info(self, task: CollectionTask) -> List[str]:
        """汇总文件信息 - 用于测试套件汇总"""
        collected_files = []
        
        for node_name in task.node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                self.logger.error(f"Node not found: {node_name}")
                continue
            
            # 为每个节点创建子目录
            node_result_dir = Path(task.local_base_path) / node_name
            node_result_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                self.logger.warning(f"Legacy collection method called for {node_name}, using empty result")
                node_files = []
                collected_files.extend(node_files)
                
                self.logger.info(f"Collected {len(node_files)} files from node {node_name}")
            
            except Exception as e:
                self.logger.error(f"Failed to collect files from node {node_name}: {e}")
        
        return collected_files
    
