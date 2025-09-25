"""
结果传输器
负责处理测试结果文件的传输、下载和文件操作
"""

import os
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

from ..node_manager import NodeManager
from .result_models import CollectionTask, CollectionSummary
from ...utils.exception_handler import with_exception_handling, result_collection


class ResultTransporter:
    """结果文件传输器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.result_transporter")
        self.node_manager = node_manager
    
    @result_collection(return_on_error=CollectionSummary(collection_time=datetime.now().isoformat(), mode="artifacts"))
    def collect_artifacts_from_nodes(self, artifacts: List[str], node_names: List[str], 
                                   local_base_dir: Path) -> CollectionSummary:
        """从节点收集测试artifacts"""
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
    
    def collect_service_logs(self, node_names: List[str], local_base_dir: Path,
                           collection_summary: CollectionSummary):
        """收集服务日志"""
        self.logger.info(f"Collecting service logs from {len(node_names)} nodes")
        
        # Docker compose服务日志
        compose_logs = [
            "docker compose logs --no-color",
            "docker compose ps -a"
        ]
        
        for node_name in node_names:
            node_dir = local_base_dir / node_name
            node_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                for i, log_cmd in enumerate(compose_logs):
                    log_name = f"compose_{'logs' if i == 0 else 'status'}.txt"
                    
                    results = self.node_manager.execute_command(
                        log_cmd, [node_name], timeout=60
                    )
                    node_result = results.get(node_name)
                    
                    if node_result and node_result[0] == 0:
                        log_file = node_dir / log_name
                        with open(log_file, 'w', encoding='utf-8') as f:
                            f.write(f"# Service log: {log_name} from {node_name}\\n")
                            f.write(f"# Command: {log_cmd}\\n")
                            f.write(f"# Collected at: {datetime.now().isoformat()}\\n\\n")
                            f.write(node_result[1])
                        
                        # 更新统计
                        log_size = log_file.stat().st_size
                        collection_summary.total_files_collected += 1
                        collection_summary.total_size_mb += log_size / (1024 * 1024)
            
            except Exception as e:
                error_msg = f"Failed to collect service logs from {node_name}: {e}"
                self.logger.warning(error_msg)
                collection_summary.collection_errors.append(error_msg)
    
    def collect_system_logs(self, node_names: List[str], local_base_dir: Path,
                          collection_summary: CollectionSummary):
        """收集系统日志"""
        self.logger.info(f"Collecting system logs from {len(node_names)} nodes")
        
        # 系统日志命令
        system_log_commands = {
            "docker_info.txt": "docker info",
            "docker_images.txt": "docker images --format 'table {{.Repository}}\\t{{.Tag}}\\t{{.Size}}\\t{{.CreatedAt}}'",
            "docker_containers.txt": "docker ps -a --format 'table {{.Names}}\\t{{.Status}}\\t{{.Image}}\\t{{.CreatedAt}}'",
            "system_resources.txt": "free -h && echo '---' && df -h && echo '---' && top -b -n 1 | head -20"
        }
        
        for node_name in node_names:
            node_dir = local_base_dir / node_name
            node_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                for log_name, command in system_log_commands.items():
                    try:
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
    
    def collect_files_from_nodes_legacy(self, task: CollectionTask) -> List[str]:
        """从节点收集文件 - 兼容旧版本的方法"""
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
    
    def collect_files_from_single_node(self, node, remote_path: str, local_dir: str) -> List[str]:
        """从单个节点收集文件"""
        collected_files = []
        
        client = node.get_ssh_client()
        
        with client.connection_context():
            # 检查远程路径是否存在
            if not client.directory_exists(remote_path):
                self.logger.warning(f"Remote path does not exist on {node.name}: {remote_path}")
                return []
            
            # 列出远程文件
            list_cmd = f"find {remote_path} -type f -name '*.json' -o -name '*.csv' -o -name '*.log' -o -name '*.txt' | head -100"
            exit_code, stdout, stderr = client.execute_command(list_cmd, timeout=60, check_exit_code=False)
            
            if exit_code != 0:
                self.logger.warning(f"Failed to list files on {node.name}: {stderr}")
                return []
            
            remote_files = [f.strip() for f in stdout.strip().split('\\n') if f.strip()]
            
            # 下载每个文件
            for remote_file in remote_files:
                try:
                    # 计算本地文件路径
                    relative_path = os.path.relpath(remote_file, remote_path)
                    local_file = Path(local_dir) / relative_path
                    local_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 下载文件
                    success = client.download_file(remote_file, str(local_file))
                    if success:
                        collected_files.append(str(local_file))
                        self.logger.debug(f"Downloaded: {remote_file} -> {local_file}")
                    else:
                        self.logger.warning(f"Failed to download: {remote_file}")
                
                except Exception as e:
                    self.logger.error(f"Error downloading {remote_file}: {e}")
        
        return collected_files