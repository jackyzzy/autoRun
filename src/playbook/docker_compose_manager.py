"""
Docker Compose管理器
处理多compose文件和选择性服务部署
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
import tempfile
import shutil

from .node_manager import NodeManager
from .scenario_manager import ServiceDeployment
from .docker_container_service import DockerContainerService


class DockerComposeService:
    """Docker Compose服务定义"""
    
    def __init__(self, name: str, definition: Dict[str, Any]):
        self.name = name
        self.definition = definition
        self.depends_on = definition.get('depends_on', [])
        
    def to_dict(self) -> Dict[str, Any]:
        return {self.name: self.definition}


class DockerComposeFile:
    """Docker Compose文件管理"""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.services: Dict[str, DockerComposeService] = {}
        self.compose_data: Dict[str, Any] = {}
        self.load()
        
    def load(self):
        """加载compose文件"""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Docker Compose file not found: {self.file_path}")
            
        with open(self.file_path, 'r', encoding='utf-8') as f:
            self.compose_data = yaml.safe_load(f)
            
        # 解析服务定义
        services_data = self.compose_data.get('services', {})
        for service_name, service_def in services_data.items():
            self.services[service_name] = DockerComposeService(service_name, service_def)
    
    def get_service_names(self) -> List[str]:
        """获取所有服务名称"""
        return list(self.services.keys())
    
    def has_service(self, service_name: str) -> bool:
        """检查是否包含指定服务"""
        return service_name in self.services
    
    def create_filtered_compose(self, service_names: List[str], output_path: str) -> bool:
        """创建包含指定服务的过滤版本compose文件"""
        try:
            # 过滤服务
            filtered_services = {}
            for service_name in service_names:
                if service_name in self.services:
                    filtered_services.update(self.services[service_name].to_dict())
            
            if not filtered_services:
                return False
                
            # 创建新的compose数据
            filtered_compose = self.compose_data.copy()
            filtered_compose['services'] = filtered_services
            
            # 写入文件
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(filtered_compose, f, default_flow_style=False, allow_unicode=True)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to create filtered compose file: {e}")
            return False


class DockerComposeManager:
    """Docker Compose管理器"""
    
    def __init__(self, node_manager: NodeManager):
        self.logger = logging.getLogger("playbook.docker_compose_manager")
        self.node_manager = node_manager
        self.compose_files: Dict[str, DockerComposeFile] = {}
        self.container_service = DockerContainerService(node_manager)
        
    def discover_compose_files(self, scenario_path: Path) -> List[str]:
        """发现场景目录下的所有compose文件"""
        compose_files = []
        
        # 查找所有docker-compose*.yml和docker-compose*.yaml文件
        patterns = ["docker-compose*.yml", "docker-compose*.yaml"]
        
        for pattern in patterns:
            for file_path in scenario_path.glob(pattern):
                compose_files.append(str(file_path))
                
        self.logger.info(f"Discovered {len(compose_files)} compose files in {scenario_path}")
        return sorted(compose_files)
    
    def load_compose_file(self, file_path: str) -> DockerComposeFile:
        """加载compose文件"""
        if file_path not in self.compose_files:
            self.compose_files[file_path] = DockerComposeFile(file_path)
        return self.compose_files[file_path]
    
    def validate_service_deployment(self, scenario_path: Path, 
                                  services: List[ServiceDeployment]) -> Dict[str, List[str]]:
        """验证服务部署配置"""
        results = {
            'valid_services': [],
            'invalid_services': [],
            'missing_files': [],
            'missing_nodes': []
        }
        
        # 发现可用的compose文件
        available_files = self.discover_compose_files(scenario_path)
        
        # 获取可用节点
        available_nodes = {node.name for node in self.node_manager.get_nodes()}
        
        for service in services:
            compose_file_path = scenario_path / service.compose_file
            
            # 检查compose文件是否存在
            if not compose_file_path.exists():
                results['missing_files'].append(f"{service.name}: {service.compose_file}")
                continue
            
            try:
                # 加载compose文件
                compose_file = self.load_compose_file(str(compose_file_path))
                
                # 检查服务是否存在于compose文件中
                if not compose_file.has_service(service.name):
                    results['invalid_services'].append(
                        f"{service.name}: not found in {service.compose_file}"
                    )
                    continue
                
                # 检查节点是否存在
                missing_nodes = set(service.nodes) - available_nodes
                if missing_nodes:
                    results['missing_nodes'].extend([
                        f"{service.name}: nodes {list(missing_nodes)} not available"
                    ])
                    continue
                
                results['valid_services'].append(service.name)
                
            except Exception as e:
                results['invalid_services'].append(f"{service.name}: {str(e)}")
        
        return results
    
    def deploy_service(self, scenario_path: Path, service: ServiceDeployment,
                      node_name: str, timeout: int = 300) -> bool:
        """在指定节点上部署服务"""
        try:
            compose_file_path = scenario_path / service.compose_file
            compose_file = self.load_compose_file(str(compose_file_path))
            
            # 检查服务是否存在
            if not compose_file.has_service(service.name):
                self.logger.error(f"Service {service.name} not found in {service.compose_file}")
                return False
            
            # 获取节点
            node = self.node_manager.get_node(node_name)
            if not node:
                self.logger.error(f"Node {node_name} not found")
                return False
            
            # 创建临时的过滤版本compose文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                temp_compose_path = f.name
            
            try:
                # 创建只包含目标服务的compose文件
                if not compose_file.create_filtered_compose([service.name], temp_compose_path):
                    self.logger.error(f"Failed to create filtered compose for {service.name}")
                    return False
                
                # 上传compose文件到节点
                remote_compose_path = f"{node.docker_compose_path}/{service.compose_file}"
                upload_success = self.node_manager.upload_file(
                    temp_compose_path, remote_compose_path, [node_name]
                )
                
                if not upload_success.get(node_name, False):
                    self.logger.error(f"Failed to upload compose file to {node_name}")
                    return False
                
                # 在节点上启动服务 - 使用适配器构建命令
                compose_cmd = self.node_manager.build_compose_command(
                    node_name=node_name,
                    command_type="up",
                    file=service.compose_file,
                    services=[service.name]
                )

                full_cmd = f"cd {node.docker_compose_path} && {compose_cmd.full_cmd}"
                self.logger.info(f"Deploying {service.name} on {node_name} using {compose_cmd.version.value} with command: {compose_cmd.full_cmd}")

                results = self.node_manager.execute_command(full_cmd, [node_name], timeout=timeout)

                node_result = results.get(node_name)
                if node_result and node_result[0] == 0:
                    self.logger.info(f"Successfully deployed {service.name} on {node_name}")
                    return True
                else:
                    # 构建详细的错误信息
                    exit_code = node_result[0] if node_result else "unknown"
                    stdout_data = node_result[1] if node_result and len(node_result) > 1 else ""
                    stderr_data = node_result[2] if node_result and len(node_result) > 2 else ""

                    error_parts = [f"Failed to deploy {service.name} on {node_name} (exit code: {exit_code})"]

                    if stderr_data.strip():
                        error_parts.append(f"stderr: {stderr_data.strip()}")
                    if stdout_data.strip():
                        error_parts.append(f"stdout: {stdout_data.strip()}")

                    error_msg = "; ".join(error_parts)
                    self.logger.error(error_msg)
                    return False
                    
            finally:
                # 清理临时文件
                if os.path.exists(temp_compose_path):
                    os.unlink(temp_compose_path)
                
        except Exception as e:
            self.logger.error(f"Error deploying service {service.name} on {node_name}: {e}")
            return False
    
    def stop_service(self, scenario_path: Path, service: ServiceDeployment,
                    node_name: str, timeout: int = 120) -> bool:
        """在指定节点上停止服务"""
        try:
            node = self.node_manager.get_node(node_name)
            if not node:
                self.logger.error(f"Node {node_name} not found")
                return False
            
            # 停止服务 - 使用适配器构建命令
            stop_cmd = self.node_manager.build_compose_command(
                node_name=node_name,
                command_type="stop",
                file=service.compose_file,
                services=[service.name]
            )

            rm_cmd = self.node_manager.build_compose_command(
                node_name=node_name,
                command_type="rm",
                file=service.compose_file,
                services=[service.name]
            )

            full_cmd = f"cd {node.docker_compose_path} && {stop_cmd.full_cmd} && {rm_cmd.full_cmd}"
            results = self.node_manager.execute_command(full_cmd, [node_name], timeout=timeout)
            
            node_result = results.get(node_name)
            if node_result and node_result[0] == 0:
                self.logger.info(f"Successfully stopped {service.name} on {node_name}")
                return True
            else:
                error_msg = node_result[2] if node_result else "Unknown error"
                self.logger.warning(f"Failed to stop {service.name} on {node_name}: {error_msg}")
                # 不返回False，允许继续清理
                return True
                
        except Exception as e:
            self.logger.error(f"Error stopping service {service.name} on {node_name}: {e}")
            return False
    
    def get_service_status(self, service_name: str, node_name: str) -> Dict[str, Any]:
        """获取服务状态"""
        try:
            # 使用统一的容器服务查找容器（包含停止的容器）
            result = self.container_service.find_containers_for_service(
                service_name=service_name,
                node_name=node_name,
                include_stopped=True  # docker_compose_manager 需要查看所有容器状态
            )
            
            if not result.success:
                return {
                    'node': node_name,
                    'service': service_name,
                    'running': False,
                    'error': result.error or "Container not found with any matching strategy"
                }
            
            # 检查运行中的容器
            running_containers = result.get_running_containers()
            has_running = len(running_containers) > 0
            
            # 构建返回格式（保持向后兼容）
            status_info = {
                'node': node_name,
                'service': service_name,
                'running': has_running,
                'containers': [],
                'strategy': result.strategy_used,
                'details': f"Found {len(result.containers)} container(s) using {result.strategy_used}"
            }
            
            # 转换容器信息为旧格式
            for container in result.containers:
                status_info['containers'].append({
                    'name': container.name,
                    'status': container.status,
                    'ports': container.ports,
                    'running': container.is_running()
                })
            
            self.logger.info(f"服务状态检查完成 - {service_name}@{node_name}: {len(result.containers)}个容器, {len(running_containers)}个运行中, 策略: {result.strategy_used}")
            
            return status_info
                
        except Exception as e:
            self.logger.error(f"获取服务状态失败 - {service_name}@{node_name}: {e}")
            return {
                'node': node_name,
                'service': service_name,
                'running': False,
                'error': str(e)
            }
    
    def cleanup_node_compose_files(self, node_names: List[str]):
        """清理节点上的compose文件"""
        # 使用第一个节点构建down命令（假设所有节点版本相同，或者使用通用命令）
        if node_names:
            first_node = node_names[0]
            try:
                down_cmd = self.node_manager.build_compose_command(
                    node_name=first_node,
                    command_type="down",
                    file="docker-compose.yml"  # 使用通用文件名
                )
                cleanup_commands = [
                    down_cmd.full_cmd.replace("docker-compose.yml", "*compose*.yml") + " --remove-orphans || true",
                    "docker system prune -f"
                ]
            except Exception as e:
                self.logger.warning(f"Failed to build adaptive down command, using default: {e}")
                cleanup_commands = [
                    "docker compose down --remove-orphans || docker-compose down --remove-orphans || true",
                    "docker system prune -f"
                ]
        else:
            cleanup_commands = [
                "docker compose down --remove-orphans || docker-compose down --remove-orphans || true",
                "docker system prune -f"
            ]
        
        for cmd in cleanup_commands:
            try:
                results = self.node_manager.execute_command(cmd, node_names, timeout=120)
                success_count = sum(1 for r in results.values() if r[0] == 0)
                self.logger.info(f"Cleanup command '{cmd}' executed on {success_count}/{len(node_names)} nodes")
            except Exception as e:
                self.logger.warning(f"Cleanup command failed: {e}")