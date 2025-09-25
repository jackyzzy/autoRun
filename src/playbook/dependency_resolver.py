"""
服务依赖解析器
处理服务启动顺序和依赖关系
"""

import logging
import time
import asyncio
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

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
        """计算服务部署批次，使用拓扑排序

        🎯 核心算法：基于Kahn算法的拓扑排序
        📊 工作原理：
            1. 计算每个服务的入度（依赖数量）
            2. 找到所有入度为0的服务作为当前批次
            3. 部署当前批次后，更新依赖这些服务的其他服务的入度
            4. 重复直到所有服务都被分配到批次中

        🚀 并发优势：
            - 同批次内的服务可以并发部署
            - 最大化部署并发度，最小化总部署时间
            - 保证依赖关系绝对不被违反

        📈 性能示例：
            无依赖场景: [S1,S2,S3] → 1个批次 → 并发部署
            链式依赖: S1→S2→S3 → 3个批次 → 串行部署
            分层依赖: [S1,S2]→[S3,S4] → 2个批次 → 部分并发

        Returns:
            List[List[ServiceNode]]: 按依赖顺序排列的服务批次列表
                                   每个批次内的服务可以并发部署
        """
        if not self.service_nodes:
            return []
        
        # 拓扑排序计算部署顺序
        in_degree = {}
        for service_name, node in self.service_nodes.items():
            in_degree[service_name] = len(node.dependencies)
        
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        batches = []
        
        while queue:
            # 🔄 核心循环：处理当前入度为0的所有服务
            current_batch = []
            batch_size = len(queue)  # 🔒 锁定当前批次大小，避免在循环中添加的服务影响当前批次

            # 📦 构建当前批次：所有入度为0的服务可以并发部署
            for _ in range(batch_size):
                service_name = queue.popleft()
                current_batch.append(self.service_nodes[service_name])

                # 🔄 依赖解析：当前服务部署完成后，更新依赖它的服务的入度
                for dependent_name in self.service_nodes[service_name].dependents:
                    in_degree[dependent_name] -= 1
                    # 📍 新的可部署服务：入度降为0表示所有依赖都已就绪
                    if in_degree[dependent_name] == 0:
                        queue.append(dependent_name)  # 🔮 加入下一批次
            
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
                              timeout: int = 200) -> bool:
        """🏥 等待服务在指定节点上就绪 - 智能健康检查

        🎯 改进机制：
        1. 动态调整检查间隔
        2. 提前检测失败状态
        3. 详细的状态日志
        4. 智能退出条件

        Args:
            service_name: 服务名称
            node_names: 节点名称列表
            timeout: 超时时间（秒）

        Returns:
            bool: 是否所有节点上的服务都就绪
        """
        start_time = time.time()
        check_interval = 5  # 🔧 减少初始检查间隔，更快发现问题
        max_check_interval = 30  # 最大检查间隔
        consecutive_failures = 0
        max_consecutive_failures = 3  # 连续失败次数阈值

        self.logger.info(f"Waiting for service {service_name} to be ready on {len(node_names)} nodes")

        while time.time() - start_time < timeout:
            all_ready = True
            ready_nodes = []
            failed_nodes = []

            for node_name in node_names:
                try:
                    status = self.docker_manager.get_service_status(service_name, node_name)

                    if status.get('running', False):
                        ready_nodes.append(node_name)
                    else:
                        all_ready = False
                        failed_nodes.append(node_name)

                        # 🚨 检查是否是永久性失败
                        error_msg = status.get('error', '')
                        if self._is_permanent_failure(error_msg):
                            self.logger.error(f"Service {service_name} on {node_name} has permanent failure: {error_msg}")
                            return False

                except Exception as e:
                    self.logger.warning(f"Failed to check status for {service_name} on {node_name}: {e}")
                    all_ready = False
                    failed_nodes.append(node_name)

            if all_ready:
                self.logger.info(f"Service {service_name} is ready on all {len(node_names)} nodes")
                return True

            # 📊 提供详细的状态信息
            elapsed = int(time.time() - start_time)
            self.logger.info(f"Service {service_name} status ({elapsed}s/{timeout}s): {len(ready_nodes)} ready, {len(failed_nodes)} not ready")

            if ready_nodes:
                self.logger.debug(f"Ready nodes: {ready_nodes}")
            if failed_nodes:
                self.logger.debug(f"Not ready nodes: {failed_nodes}")

            # 🔧 动态调整检查间隔
            if failed_nodes:
                consecutive_failures += 1
                # 随着失败次数增加，延长检查间隔
                check_interval = min(max_check_interval, 5 + consecutive_failures * 2)
            else:
                consecutive_failures = 0
                check_interval = 5

            # 🚨 如果连续失败太多次，可能有问题
            if consecutive_failures >= max_consecutive_failures:
                self.logger.warning(f"Service {service_name} has {consecutive_failures} consecutive check failures")

            self.logger.debug(f"Service {service_name} not ready yet, waiting {check_interval}s... (attempt {consecutive_failures + 1})")
            time.sleep(check_interval)

        self.logger.error(f"Service {service_name} did not become ready within {timeout}s (ready: {len(ready_nodes)}/{len(node_names)})")
        return False

    def _is_permanent_failure(self, error_msg: str) -> bool:
        """🚨 检查是否是永久性失败 - 不会自动恢复的错误

        Args:
            error_msg: 错误信息

        Returns:
            bool: 是否是永久性失败
        """
        if not error_msg:
            return False

        error_lower = error_msg.lower()

        # 永久性失败的错误模式
        permanent_failure_patterns = [
            "image not found",
            "manifest not found",
            "no such file or directory",
            "permission denied",
            "invalid configuration",
            "container failed to start",
            "exited with code",
            "cannot allocate memory"
        ]

        for pattern in permanent_failure_patterns:
            if pattern in error_lower:
                return True

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
            
            # 在所有指定节点上部署服务
            service_success = self._deploy_service_on_nodes(scenario_path, service, timeout)
            results[service.name] = service_success

            # 更新节点状态
            self._update_service_status(service_node, service_success)
        
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
    
    def _deploy_service_on_nodes(self, scenario_path: Path, service: ServiceDeployment, timeout: int) -> bool:
        """在所有指定节点上部署服务（同步版本）"""
        for node_name in service.nodes:
            success = self.docker_manager.deploy_service(
                scenario_path, service, node_name, timeout
            )
            if not success:
                self.logger.error(f"Failed to deploy {service.name} on {node_name}")
                return False
        return True

    def _update_service_status(self, service_node: ServiceNode, success: bool):
        """更新服务节点状态"""
        if success:
            service_node.status = DependencyStatus.RESOLVED
            self.logger.info(f"Successfully deployed {service_node.service.name}")
        else:
            service_node.status = DependencyStatus.FAILED

    async def _deploy_service_on_nodes_async(self, scenario_path: Path, service: ServiceDeployment, timeout: int) -> bool:
        """在所有指定节点上部署服务（异步版本）"""
        for node_name in service.nodes:
            success = await self._deploy_service_on_node_async(
                scenario_path, service, node_name, timeout
            )
            if not success:
                self.logger.error(f"Failed to deploy {service.name} on {node_name}")
                return False
        return True

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

    async def deploy_batch_concurrent(self, batch: List[ServiceNode], scenario_path,
                                    max_concurrent: int = 5, timeout: int = 300) -> Dict[str, bool]:
        """并发部署一批服务"""

        self.logger.info(f"Deploying batch with {len(batch)} services concurrently (max_concurrent={max_concurrent})")

        # 使用信号量控制并发度
        semaphore = asyncio.Semaphore(max_concurrent)

        async def deploy_single_service(service_node: ServiceNode) -> Tuple[str, bool]:
            """部署单个服务"""
            async with semaphore:
                service = service_node.service

                # 等待依赖
                if not await self._wait_for_dependencies_async(service, timeout):
                    self.logger.error(f"Dependencies not ready for {service.name}")
                    return service.name, False

                # 在所有指定节点上部署服务
                service_success = await self._deploy_service_on_nodes_async(scenario_path, service, timeout)

                # 更新节点状态
                self._update_service_status(service_node, service_success)

                return service.name, service_success

        # 并发执行所有服务的部署
        tasks = [deploy_single_service(service_node) for service_node in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        deployment_results = {}
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Service deployment task failed: {result}")
                continue

            service_name, success = result
            deployment_results[service_name] = success

        # 等待本批次所有成功的服务就绪
        await self._wait_batch_services_ready(batch, deployment_results, timeout)

        success_count = sum(1 for success in deployment_results.values() if success)
        self.logger.info(f"Batch deployment completed: {success_count}/{len(batch)} successful")

        return deployment_results

    async def _wait_for_dependencies_async(self, service: ServiceDeployment, timeout: int = 600) -> bool:
        """异步等待服务的所有依赖就绪"""
        if not service.depends_on:
            return True

        try:
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(
                    None, self.wait_for_dependencies, service, timeout
                ),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.error(f"Dependency wait timeout for service {service.name} after {timeout}s")
            return False
        except Exception as e:
            self.logger.error(f"Dependency wait failed for service {service.name}: {str(e)}")
            return False

    async def _deploy_service_on_node_async(self, scenario_path, service: ServiceDeployment,
                                          node_name: str, timeout: int) -> bool:
        """在指定节点上异步部署服务"""
        try:
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(
                    None, self.docker_manager.deploy_service, scenario_path, service, node_name, timeout
                ),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            self.logger.error(f"Service {service.name} deployment timeout on node {node_name} after {timeout}s")
            return False
        except Exception as e:
            self.logger.error(f"Service {service.name} deployment failed on node {node_name}: {str(e)}")
            return False

    async def _wait_batch_services_ready(self, batch: List[ServiceNode],
                                       deployment_results: Dict[str, bool], timeout: int):
        """等待批次中所有成功部署的服务就绪"""
        async def wait_single_service_ready(service_node: ServiceNode) -> bool:
            """等待单个服务就绪"""
            service = service_node.service
            if not deployment_results.get(service.name, False):
                return False

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self.wait_for_service_ready, service.name, service.nodes, timeout
            )

        # 只等待部署成功的服务
        ready_tasks = []
        for service_node in batch:
            if deployment_results.get(service_node.service.name, False):
                ready_tasks.append(wait_single_service_ready(service_node))

        if ready_tasks:
            ready_results = await asyncio.gather(*ready_tasks, return_exceptions=True)

            # 更新未就绪服务的状态
            task_index = 0
            for service_node in batch:
                if deployment_results.get(service_node.service.name, False):
                    if task_index < len(ready_results):
                        result = ready_results[task_index]
                        if isinstance(result, Exception) or not result:
                            deployment_results[service_node.service.name] = False
                            service_node.status = DependencyStatus.FAILED
                            self.logger.error(f"Service {service_node.service.name} did not become ready")
                    task_index += 1