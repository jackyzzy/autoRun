"""
服务依赖解析器
处理服务启动顺序和依赖关系
"""

import logging
import time
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

from .scenario_manager import ServiceDeployment
from .docker_compose_manager import DockerComposeManager


class DependencyStatus(Enum):
    """依赖状态"""
    PENDING = "pending"
    RESOLVING = "resolving"
    RESOLVED = "resolved"
    FAILED = "failed"


@dataclass
class ServiceNode:
    """服务节点，用于依赖图"""
    service: ServiceDeployment
    dependencies: Set[str]  # 依赖的服务名称
    dependents: Set[str]   # 依赖此服务的服务名称
    status: DependencyStatus = DependencyStatus.PENDING
    deploy_nodes: List[str] = None  # 部署节点列表
    
    def __post_init__(self):
        if self.deploy_nodes is None:
            self.deploy_nodes = self.service.nodes.copy()


class ServiceDependencyResolver:
    """服务依赖解析器"""
    
    def __init__(self, docker_manager: DockerComposeManager):
        self.logger = logging.getLogger("playbook.dependency_resolver")
        self.docker_manager = docker_manager
        self.service_nodes: Dict[str, ServiceNode] = {}
        self.deployment_batches: List[List[ServiceNode]] = []
        
    def build_dependency_graph(self, services: List[ServiceDeployment]) -> bool:
        """构建服务依赖图"""
        self.service_nodes.clear()
        
        # 创建服务节点
        for service in services:
            dependencies = set(service.depends_on)
            self.service_nodes[service.name] = ServiceNode(
                service=service,
                dependencies=dependencies,
                dependents=set()
            )
        
        # 建立反向依赖关系
        for service_name, node in self.service_nodes.items():
            for dep_name in node.dependencies:
                if dep_name in self.service_nodes:
                    self.service_nodes[dep_name].dependents.add(service_name)
                else:
                    self.logger.warning(f"Service {service_name} depends on unknown service {dep_name}")
        
        # 检查循环依赖
        if self._has_circular_dependency():
            self.logger.error("Circular dependency detected")
            return False
        
        self.logger.info(f"Built dependency graph with {len(self.service_nodes)} services")
        return True
    
    def _has_circular_dependency(self) -> bool:
        """检查是否存在循环依赖"""
        visited = set()
        rec_stack = set()
        
        def has_cycle(node_name: str) -> bool:
            visited.add(node_name)
            rec_stack.add(node_name)
            
            node = self.service_nodes.get(node_name)
            if not node:
                return False
            
            for dep_name in node.dependencies:
                if dep_name not in visited:
                    if has_cycle(dep_name):
                        return True
                elif dep_name in rec_stack:
                    return True
            
            rec_stack.remove(node_name)
            return False
        
        for service_name in self.service_nodes:
            if service_name not in visited:
                if has_cycle(service_name):
                    return True
        
        return False
    
    def get_deployment_batches(self) -> List[List[ServiceNode]]:
        """计算服务部署批次，使用拓扑排序"""
        if not self.service_nodes:
            return []
        
        # 拓扑排序计算部署顺序
        in_degree = {}
        for service_name, node in self.service_nodes.items():
            in_degree[service_name] = len(node.dependencies)
        
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        batches = []
        
        while queue:
            # 当前批次：所有入度为0的节点可以并行部署
            current_batch = []
            batch_size = len(queue)
            
            for _ in range(batch_size):
                service_name = queue.popleft()
                current_batch.append(self.service_nodes[service_name])
                
                # 更新依赖此服务的其他服务的入度
                for dependent_name in self.service_nodes[service_name].dependents:
                    in_degree[dependent_name] -= 1
                    if in_degree[dependent_name] == 0:
                        queue.append(dependent_name)
            
            if current_batch:
                batches.append(current_batch)
        
        # 检查是否所有服务都被处理
        processed_count = sum(len(batch) for batch in batches)
        if processed_count != len(self.service_nodes):
            remaining = set(self.service_nodes.keys()) - {
                node.service.name for batch in batches for node in batch
            }
            self.logger.error(f"Failed to resolve dependencies for services: {remaining}")
            return []
        
        self.deployment_batches = batches
        self.logger.info(f"Calculated {len(batches)} deployment batches")
        return batches
    
    def wait_for_service_ready(self, service_name: str, node_names: List[str],
                              timeout: int = 300) -> bool:
        """等待服务在指定节点上就绪"""
        start_time = time.time()
        check_interval = 10
        
        while time.time() - start_time < timeout:
            all_ready = True
            
            for node_name in node_names:
                status = self.docker_manager.get_service_status(service_name, node_name)
                
                if not status.get('running', False):
                    all_ready = False
                    break
            
            if all_ready:
                self.logger.info(f"Service {service_name} is ready on all nodes: {node_names}")
                return True
            
            self.logger.debug(f"Service {service_name} not ready yet, waiting {check_interval}s...")
            time.sleep(check_interval)
        
        self.logger.error(f"Service {service_name} did not become ready within {timeout}s")
        return False
    
    def wait_for_dependencies(self, service: ServiceDeployment, timeout: int = 600) -> bool:
        """等待服务的所有依赖就绪"""
        if not service.depends_on:
            return True
        
        self.logger.info(f"Waiting for dependencies of {service.name}: {service.depends_on}")
        
        start_time = time.time()
        
        for dep_service_name in service.depends_on:
            if dep_service_name not in self.service_nodes:
                self.logger.error(f"Dependency {dep_service_name} not found")
                return False
            
            dep_node = self.service_nodes[dep_service_name]
            remaining_timeout = int(timeout - (time.time() - start_time))
            
            if remaining_timeout <= 0:
                self.logger.error(f"Timeout waiting for dependency {dep_service_name}")
                return False
            
            # 等待依赖服务在其所有部署节点上就绪
            if not self.wait_for_service_ready(
                dep_service_name, 
                dep_node.deploy_nodes, 
                remaining_timeout
            ):
                return False
        
        self.logger.info(f"All dependencies ready for {service.name}")
        return True
    
    def deploy_batch(self, batch: List[ServiceNode], scenario_path, timeout: int = 300) -> Dict[str, bool]:
        """部署一批服务"""
        results = {}
        
        self.logger.info(f"Deploying batch with {len(batch)} services")
        
        # 并行部署批次中的所有服务
        deployment_tasks = []
        
        for service_node in batch:
            service = service_node.service
            
            # 等待依赖
            if not self.wait_for_dependencies(service, timeout):
                results[service.name] = False
                continue
            
            # 在每个指定节点上部署服务
            service_success = True
            for node_name in service.nodes:
                success = self.docker_manager.deploy_service(
                    scenario_path, service, node_name, timeout
                )
                if not success:
                    service_success = False
                    self.logger.error(f"Failed to deploy {service.name} on {node_name}")
                    break
            
            results[service.name] = service_success
            
            # 更新节点状态
            if service_success:
                service_node.status = DependencyStatus.RESOLVED
            else:
                service_node.status = DependencyStatus.FAILED
        
        # 等待本批次所有服务就绪
        for service_node in batch:
            if results.get(service_node.service.name, False):
                # 等待服务真正就绪
                if not self.wait_for_service_ready(
                    service_node.service.name,
                    service_node.service.nodes,
                    timeout
                ):
                    results[service_node.service.name] = False
                    service_node.status = DependencyStatus.FAILED
        
        success_count = sum(1 for success in results.values() if success)
        self.logger.info(f"Batch deployment completed: {success_count}/{len(batch)} successful")
        
        return results
    
    def get_deployment_summary(self) -> Dict[str, any]:
        """获取部署摘要"""
        if not self.service_nodes:
            return {'total': 0, 'batches': 0, 'services': []}
        
        services_info = []
        for service_name, node in self.service_nodes.items():
            services_info.append({
                'name': service_name,
                'status': node.status.value,
                'dependencies': list(node.dependencies),
                'dependents': list(node.dependents),
                'nodes': node.deploy_nodes
            })
        
        status_counts = defaultdict(int)
        for node in self.service_nodes.values():
            status_counts[node.status.value] += 1
        
        return {
            'total': len(self.service_nodes),
            'batches': len(self.deployment_batches),
            'status_counts': dict(status_counts),
            'services': services_info
        }
    
    def cleanup_failed_services(self, scenario_path):
        """清理失败的服务"""
        failed_services = [
            node for node in self.service_nodes.values() 
            if node.status == DependencyStatus.FAILED
        ]
        
        if not failed_services:
            return
        
        self.logger.info(f"Cleaning up {len(failed_services)} failed services")
        
        for service_node in failed_services:
            for node_name in service_node.service.nodes:
                try:
                    self.docker_manager.stop_service(
                        scenario_path, service_node.service, node_name
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup {service_node.service.name} on {node_name}: {e}")