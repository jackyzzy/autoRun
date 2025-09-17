"""
Docker Compose管理器
管理分布式节点上的Docker Compose服务
"""

import logging
import time
import re
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from .node_manager import NodeManager, Node


class ServiceStatus(Enum):
    """服务状态枚举"""
    RUNNING = "running"
    STOPPED = "stopped"
    PAUSED = "paused"
    RESTARTING = "restarting"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ServiceInfo:
    """服务信息"""
    name: str
    image: str
    status: ServiceStatus
    ports: List[str]
    created: str
    node_name: str
    container_id: str = ""
    
    @classmethod
    def from_docker_ps_line(cls, line: str, node_name: str) -> 'ServiceInfo':
        """从docker ps输出行创建ServiceInfo"""
        # 解析docker ps输出格式
        parts = line.strip().split('\t')
        if len(parts) >= 4:
            return cls(
                name=parts[0],
                image=parts[1] if len(parts) > 1 else "",
                status=cls._parse_status(parts[2] if len(parts) > 2 else ""),
                ports=parts[3].split(',') if len(parts) > 3 else [],
                created=parts[4] if len(parts) > 4 else "",
                node_name=node_name
            )
        
        return cls(
            name=line.strip(),
            image="",
            status=ServiceStatus.UNKNOWN,
            ports=[],
            created="",
            node_name=node_name
        )
    
    @staticmethod
    def _parse_status(status_str: str) -> ServiceStatus:
        """解析状态字符串"""
        status_lower = status_str.lower()
        if 'up' in status_lower:
            if 'unhealthy' in status_lower:
                return ServiceStatus.UNHEALTHY
            else:
                return ServiceStatus.RUNNING
        elif 'exited' in status_lower:
            return ServiceStatus.STOPPED
        elif 'paused' in status_lower:
            return ServiceStatus.PAUSED
        elif 'restarting' in status_lower:
            return ServiceStatus.RESTARTING
        else:
            return ServiceStatus.UNKNOWN


class DockerComposeManager:
    """Docker Compose管理器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.docker_manager")
        self.node_manager = node_manager
        
    def start_services(self, compose_file: str, services: List[str] = None,
                      node_names: List[str] = None, detach: bool = True,
                      timeout: int = 300) -> Dict[str, bool]:
        """
        启动Docker Compose服务
        
        Args:
            compose_file: docker-compose文件路径
            services: 要启动的服务列表，为空则启动所有服务
            node_names: 目标节点列表，为空则使用所有启用的节点
            detach: 是否后台运行
            timeout: 超时时间
            
        Returns:
            Dict[node_name, success]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Starting services on {len(node_names)} nodes")

        results = {}
        for node_name in node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                results[node_name] = False
                continue

            try:
                # 使用适配器构建compose命令
                compose_cmd = self.node_manager.build_compose_command(
                    node_name=node_name,
                    command_type="up",
                    file=compose_file,
                    services=services or []
                )

                # 添加detach参数处理
                if detach:
                    # 适配器已经包含-d参数，这里无需额外处理
                    pass

                # 切换到工作目录并执行命令
                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"

                if services:
                    self.logger.info(f"Starting specific services on {node_name}: {services}")
                else:
                    self.logger.info(f"Starting all services on {node_name}")

                self.logger.info(f"Executing on {node_name}: {compose_cmd.full_cmd}")
                cmd_results = self.node_manager.execute_command(
                    full_cmd, [node_name], timeout=timeout
                )
                
                node_result = cmd_results.get(node_name)
                if node_result and node_result[0] == 0:
                    results[node_name] = True
                    self.logger.info(f"Services started successfully on {node_name}")
                else:
                    results[node_name] = False
                    error_msg = node_result[2] if node_result else "Unknown error"
                    self.logger.error(f"Failed to start services on {node_name}: {error_msg}")
                    
            except Exception as e:
                results[node_name] = False
                self.logger.error(f"Exception starting services on {node_name}: {e}")
        
        success_count = sum(results.values())
        self.logger.info(f"Services started on {success_count}/{len(node_names)} nodes")
        
        return results
    
    def stop_services(self, compose_file: str, services: List[str] = None,
                     node_names: List[str] = None, timeout: int = 120,
                     remove_volumes: bool = False) -> Dict[str, bool]:
        """
        停止Docker Compose服务
        
        Args:
            compose_file: docker-compose文件路径
            services: 要停止的服务列表，为空则停止所有服务
            node_names: 目标节点列表
            timeout: 超时时间
            remove_volumes: 是否删除卷
            
        Returns:
            Dict[node_name, success]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Stopping services on {len(node_names)} nodes")
        
        results = {}
        for node_name in node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                results[node_name] = False
                continue

            try:
                if services:
                    # 停止特定服务
                    compose_cmd = self.node_manager.build_compose_command(
                        node_name=node_name,
                        command_type="stop",
                        file=compose_file,
                        services=services
                    )
                    self.logger.info(f"Stopping specific services on {node_name}: {services}")
                else:
                    # 停止所有服务 - 使用down命令
                    compose_cmd = self.node_manager.build_compose_command(
                        node_name=node_name,
                        command_type="down",
                        file=compose_file
                    )
                    # 如果需要删除卷，修改命令
                    if remove_volumes:
                        compose_cmd.full_cmd += " -v"
                    self.logger.info(f"Stopping all services on {node_name}")

                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"
                
                self.logger.info(f"Executing on {node_name}: {compose_cmd}")
                cmd_results = self.node_manager.execute_command(
                    full_cmd, [node_name], timeout=timeout
                )
                
                node_result = cmd_results.get(node_name)
                if node_result and node_result[0] == 0:
                    results[node_name] = True
                    self.logger.info(f"Services stopped successfully on {node_name}")
                else:
                    results[node_name] = False
                    error_msg = node_result[2] if node_result else "Unknown error"
                    self.logger.error(f"Failed to stop services on {node_name}: {error_msg}")
                    
            except Exception as e:
                results[node_name] = False
                self.logger.error(f"Exception stopping services on {node_name}: {e}")
        
        success_count = sum(results.values())
        self.logger.info(f"Services stopped on {success_count}/{len(node_names)} nodes")
        
        return results
    
    def restart_services(self, compose_file: str, services: List[str] = None,
                        node_names: List[str] = None, timeout: int = 300) -> Dict[str, bool]:
        """重启Docker Compose服务"""
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Restarting services on {len(node_names)} nodes")
        
        # 先停止再启动
        stop_results = self.stop_services(compose_file, services, node_names, timeout//2)
        
        # 等待一段时间
        time.sleep(5)
        
        start_results = self.start_services(compose_file, services, node_names, timeout=timeout//2)
        
        # 合并结果
        results = {}
        for node_name in node_names:
            results[node_name] = (stop_results.get(node_name, False) and 
                                start_results.get(node_name, False))
        
        return results
    
    def get_service_status(self, node_names: List[str] = None) -> Dict[str, List[ServiceInfo]]:
        """
        获取服务状态
        
        Returns:
            Dict[node_name, List[ServiceInfo]]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        # Docker ps命令格式化输出
        ps_cmd = "docker ps -a --format 'table {{.Names}}\\t{{.Image}}\\t{{.Status}}\\t{{.Ports}}\\t{{.CreatedAt}}'"
        
        cmd_results = self.node_manager.execute_command(ps_cmd, node_names, timeout=30)
        
        results = {}
        for node_name in node_names:
            services = []
            node_result = cmd_results.get(node_name)
            
            if node_result and node_result[0] == 0:
                output_lines = node_result[1].strip().split('\n')
                
                # 跳过表头
                for line in output_lines[1:]:
                    if line.strip():
                        try:
                            service = ServiceInfo.from_docker_ps_line(line, node_name)
                            services.append(service)
                        except Exception as e:
                            self.logger.warning(f"Failed to parse service info: {line}, error: {e}")
            else:
                error_msg = node_result[2] if node_result else "Unknown error"
                self.logger.error(f"Failed to get service status on {node_name}: {error_msg}")
            
            results[node_name] = services
        
        return results
    
    def get_service_logs(self, service_name: str, compose_file: str,
                        node_names: List[str] = None, lines: int = 100,
                        follow: bool = False) -> Dict[str, str]:
        """
        获取服务日志
        
        Args:
            service_name: 服务名称
            compose_file: docker-compose文件路径
            node_names: 目标节点列表
            lines: 日志行数
            follow: 是否持续跟踪
            
        Returns:
            Dict[node_name, logs]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        results = {}
        for node_name in node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                results[node_name] = ""
                continue

            try:
                # 使用适配器构建日志命令
                compose_cmd = self.node_manager.build_compose_command(
                    node_name=node_name,
                    command_type="logs",
                    file=compose_file,
                    lines=str(lines),
                    services=[service_name]
                )

                # 添加follow参数处理
                if follow:
                    compose_cmd.full_cmd += " -f"

                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"
                cmd_results = self.node_manager.execute_command(
                    full_cmd, [node_name], timeout=60
                )
                
                node_result = cmd_results.get(node_name)
                if node_result and node_result[0] == 0:
                    results[node_name] = node_result[1]
                else:
                    error_msg = node_result[2] if node_result else "Unknown error"
                    results[node_name] = f"Error getting logs: {error_msg}"
                    
            except Exception as e:
                results[node_name] = f"Exception getting logs: {str(e)}"
        
        return results
    
    def scale_service(self, service_name: str, replicas: int, compose_file: str,
                     node_names: List[str] = None) -> Dict[str, bool]:
        """
        扩缩容服务
        
        Args:
            service_name: 服务名称
            replicas: 副本数
            compose_file: docker-compose文件路径
            node_names: 目标节点列表
            
        Returns:
            Dict[node_name, success]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Scaling service {service_name} to {replicas} replicas on {len(node_names)} nodes")

        results = {}
        for node_name in node_names:
            node = self.node_manager.get_node(node_name)
            if not node:
                results[node_name] = False
                continue

            try:
                # 使用适配器构建扩缩容命令
                compose_cmd = self.node_manager.build_compose_command(
                    node_name=node_name,
                    command_type="scale",
                    file=compose_file,
                    service=service_name,
                    replicas=str(replicas)
                )

                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"
                cmd_results = self.node_manager.execute_command(
                    full_cmd, [node_name], timeout=180
                )
                
                node_result = cmd_results.get(node_name)
                if node_result and node_result[0] == 0:
                    results[node_name] = True
                    self.logger.info(f"Service scaled successfully on {node_name}")
                else:
                    results[node_name] = False
                    error_msg = node_result[2] if node_result else "Unknown error"
                    self.logger.error(f"Failed to scale service on {node_name}: {error_msg}")
                    
            except Exception as e:
                results[node_name] = False
                self.logger.error(f"Exception scaling service on {node_name}: {e}")
        
        return results
    
    def wait_for_services_healthy(self, compose_file: str, services: List[str] = None,
                                 node_names: List[str] = None, timeout: int = 300,
                                 check_interval: int = 10) -> Dict[str, bool]:
        """
        等待服务变为健康状态
        
        Args:
            compose_file: docker-compose文件路径
            services: 要检查的服务列表
            node_names: 目标节点列表
            timeout: 超时时间
            check_interval: 检查间隔
            
        Returns:
            Dict[node_name, all_healthy]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Waiting for services to become healthy on {len(node_names)} nodes")
        
        start_time = time.time()
        results = {name: False for name in node_names}
        
        while time.time() - start_time < timeout:
            all_ready = True
            service_status = self.get_service_status(node_names)
            
            for node_name in node_names:
                if results[node_name]:  # 已经健康
                    continue
                
                node_services = service_status.get(node_name, [])
                
                # 检查指定服务是否健康
                if services:
                    target_services = [s for s in node_services if s.name in services]
                else:
                    target_services = node_services
                
                # 检查服务状态
                node_healthy = True
                for service in target_services:
                    if service.status not in [ServiceStatus.RUNNING]:
                        node_healthy = False
                        break
                
                if node_healthy and target_services:  # 有服务且都健康
                    results[node_name] = True
                    self.logger.info(f"Services are healthy on {node_name}")
                else:
                    all_ready = False
            
            if all_ready:
                self.logger.info("All services are healthy on all nodes")
                break
            
            self.logger.info(f"Services not ready yet, waiting {check_interval} seconds...")
            time.sleep(check_interval)
        
        if time.time() - start_time >= timeout:
            self.logger.warning("Timeout waiting for services to become healthy")
        
        return results
    
    def cleanup_containers(self, node_names: List[str] = None,
                          remove_images: bool = False) -> Dict[str, bool]:
        """
        清理Docker容器和资源
        
        Args:
            node_names: 目标节点列表
            remove_images: 是否删除镜像
            
        Returns:
            Dict[node_name, success]
        """
        if node_names is None:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
        
        self.logger.info(f"Cleaning up Docker resources on {len(node_names)} nodes")
        
        cleanup_commands = [
            "docker container prune -f",  # 清理停止的容器
            "docker network prune -f",    # 清理未使用的网络
            "docker volume prune -f"      # 清理未使用的卷
        ]
        
        if remove_images:
            cleanup_commands.append("docker image prune -a -f")  # 清理未使用的镜像
        
        results = {}
        for node_name in node_names:
            node_success = True
            
            for cmd in cleanup_commands:
                try:
                    cmd_results = self.node_manager.execute_command(cmd, [node_name], timeout=120)
                    node_result = cmd_results.get(node_name)
                    
                    if not node_result or node_result[0] != 0:
                        node_success = False
                        error_msg = node_result[2] if node_result else "Unknown error"
                        self.logger.warning(f"Cleanup command failed on {node_name}: {cmd}, error: {error_msg}")
                        
                except Exception as e:
                    node_success = False
                    self.logger.error(f"Exception during cleanup on {node_name}: {e}")
            
            results[node_name] = node_success
            
            if node_success:
                self.logger.info(f"Cleanup completed successfully on {node_name}")
            else:
                self.logger.warning(f"Cleanup completed with errors on {node_name}")
        
        return results
    
    def get_compose_services(self, compose_file: str) -> List[str]:
        """
        从docker-compose文件获取服务列表
        
        Args:
            compose_file: docker-compose文件路径
            
        Returns:
            服务名称列表
        """
        try:
            import yaml
            
            with open(compose_file, 'r', encoding='utf-8') as f:
                compose_config = yaml.safe_load(f)
            
            services = list(compose_config.get('services', {}).keys())
            self.logger.info(f"Found {len(services)} services in {compose_file}: {services}")
            
            return services
            
        except Exception as e:
            self.logger.error(f"Failed to parse compose file {compose_file}: {e}")
            return []
    
    def get_summary(self) -> Dict[str, Any]:
        """获取Docker管理器摘要信息"""
        nodes = self.node_manager.get_nodes(enabled_only=True)
        node_names = [node.name for node in nodes]
        
        # 获取所有服务状态
        service_status = self.get_service_status(node_names)
        
        total_services = 0
        running_services = 0
        
        for services in service_status.values():
            total_services += len(services)
            running_services += sum(1 for s in services if s.status == ServiceStatus.RUNNING)
        
        return {
            'total_nodes': len(node_names),
            'total_services': total_services,
            'running_services': running_services,
            'stopped_services': total_services - running_services,
            'service_status_by_node': {
                node_name: len(services) for node_name, services in service_status.items()
            }
        }