"""
健康检查器
负责检查系统各组件的健康状态和进行容错处理
"""

import logging
import time
import json
import os
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from .node_manager import NodeManager
from .docker_manager import DockerComposeManager, ServiceStatus
from ..utils.exception_handler import with_exception_handling, health_check


class HealthStatus(Enum):
    """健康状态枚举"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """健康检查项"""
    name: str
    description: str
    check_func: Callable[[], bool]
    timeout: int = 30
    retry_count: int = 2
    critical: bool = True  # 是否为关键检查项
    
    def __post_init__(self):
        self.last_check_time: Optional[datetime] = None
        self.last_result: bool = False
        self.last_error: str = ""
        self.consecutive_failures: int = 0


@dataclass
class ComponentHealth:
    """组件健康状态"""
    component_name: str
    status: HealthStatus
    checks: Dict[str, bool] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)
    last_check_time: Optional[datetime] = None
    error_messages: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'component_name': self.component_name,
            'status': self.status.value,
            'checks': self.checks,
            'details': self.details,
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'error_messages': self.error_messages
        }


class HealthChecker:
    """健康检查器"""
    
    def __init__(self, node_manager: NodeManager, docker_manager: DockerComposeManager):
        self.logger = logging.getLogger("playbook.health_checker")
        self.node_manager = node_manager
        self.docker_manager = docker_manager
        
        # 健康检查项
        self.checks: Dict[str, HealthCheck] = {}
        self.component_status: Dict[str, ComponentHealth] = {}
        
        # 检查配置
        self.check_interval = 60  # 默认检查间隔（秒）
        self.max_failure_threshold = 3  # 最大失败次数阈值
        
        # 自动恢复配置
        self.enable_auto_recovery = False
        self.recovery_actions: Dict[str, Callable] = {}
        
        # 初始化默认检查项
        self._init_default_checks()
    
    def _init_default_checks(self):
        """初始化默认健康检查项"""
        # 节点连接检查
        self.add_check(
            "node_connectivity",
            "Check SSH connectivity to all nodes",
            self._check_node_connectivity,
            timeout=30,
            critical=True
        )
        
        # 系统资源检查
        self.add_check(
            "system_resources",
            "Check system resources (CPU, Memory, Disk)",
            self._check_system_resources,
            timeout=45,
            critical=False
        )
        
        # Docker服务检查
        self.add_check(
            "docker_services",
            "Check Docker daemon and services status",
            self._check_docker_services,
            timeout=60,
            critical=True
        )
        
        # GPU资源检查
        self.add_check(
            "gpu_resources",
            "Check GPU availability and status",
            self._check_gpu_resources,
            timeout=30,
            critical=False
        )
        
        # 网络连通性检查
        self.add_check(
            "network_connectivity",
            "Check network connectivity between nodes",
            self._check_network_connectivity,
            timeout=45,
            critical=False
        )
        
        # 存储空间检查
        self.add_check(
            "storage_space",
            "Check available storage space",
            self._check_storage_space,
            timeout=30,
            critical=True
        )
    
    def add_check(self, name: str, description: str, check_func: Callable[[], bool],
                  timeout: int = 30, retry_count: int = 2, critical: bool = True):
        """添加健康检查项"""
        self.checks[name] = HealthCheck(
            name=name,
            description=description,
            check_func=check_func,
            timeout=timeout,
            retry_count=retry_count,
            critical=critical
        )
        
        self.logger.info(f"Added health check: {name}")
    
    def remove_check(self, name: str) -> bool:
        """移除健康检查项"""
        if name in self.checks:
            del self.checks[name]
            if name in self.component_status:
                del self.component_status[name]
            self.logger.info(f"Removed health check: {name}")
            return True
        return False
    
    def run_single_check(self, check_name: str) -> bool:
        """运行单个健康检查"""
        if check_name not in self.checks:
            self.logger.error(f"Health check not found: {check_name}")
            return False
        
        check = self.checks[check_name]
        self.logger.info(f"Running health check: {check_name}")
        
        # 执行检查
        for attempt in range(check.retry_count + 1):
            try:
                start_time = time.time()
                result = check.check_func()
                duration = time.time() - start_time
                
                check.last_check_time = datetime.now()
                check.last_result = result
                check.last_error = ""
                
                if result:
                    check.consecutive_failures = 0
                    self.logger.info(f"Health check passed: {check_name} (took {duration:.2f}s)")
                    return True
                else:
                    check.consecutive_failures += 1
                    if attempt < check.retry_count:
                        self.logger.warning(f"Health check failed, retrying: {check_name} (attempt {attempt + 1})")
                        time.sleep(5)  # 重试前等待
                        continue
                    else:
                        self.logger.error(f"Health check failed after {attempt + 1} attempts: {check_name}")
                        return False
                        
            except Exception as e:
                check.last_error = str(e)
                check.consecutive_failures += 1
                
                if attempt < check.retry_count:
                    self.logger.warning(f"Health check error, retrying: {check_name}, error: {e}")
                    time.sleep(5)
                    continue
                else:
                    self.logger.error(f"Health check error after {attempt + 1} attempts: {check_name}, error: {e}")
                    return False
        
        return False
    
    def run_all_checks(self) -> Dict[str, ComponentHealth]:
        """运行所有健康检查"""
        self.logger.info(f"Running {len(self.checks)} health checks")
        
        results = {}
        
        for check_name, check in self.checks.items():
            component_health = ComponentHealth(
                component_name=check_name,
                status=HealthStatus.UNKNOWN,
                last_check_time=datetime.now()
            )
            
            try:
                check_result = self.run_single_check(check_name)
                component_health.checks[check_name] = check_result
                
                if check_result:
                    component_health.status = HealthStatus.HEALTHY
                else:
                    # 根据失败次数确定状态
                    if check.consecutive_failures >= self.max_failure_threshold:
                        component_health.status = HealthStatus.UNHEALTHY
                    else:
                        component_health.status = HealthStatus.DEGRADED
                    
                    component_health.error_messages.append(check.last_error or "Check failed")
                
            except Exception as e:
                component_health.status = HealthStatus.UNHEALTHY
                component_health.error_messages.append(str(e))
                self.logger.error(f"Error running health check {check_name}: {e}")
            
            results[check_name] = component_health
            self.component_status[check_name] = component_health
        
        # 执行自动恢复（如果启用）
        if self.enable_auto_recovery:
            self._attempt_auto_recovery(results)
        
        self._log_health_summary(results)
        return results
    
    @health_check(return_on_error=False)
    def _check_node_connectivity(self) -> bool:
        """检查节点连接性"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            if not nodes:
                self.logger.error("No enabled nodes found")
                return False
            
            node_names = [node.name for node in nodes]
            connectivity = self.node_manager.test_connectivity(node_names, timeout=15)
            
            failed_nodes = [name for name, connected in connectivity.items() if not connected]
            
            if failed_nodes:
                self.logger.error(f"Failed to connect to nodes: {failed_nodes}")
                return len(failed_nodes) == 0  # 只有全部连接成功才返回True
            
            self.logger.info(f"All {len(nodes)} nodes are connected")
            return True
            
        except Exception as e:
            self.logger.error(f"Node connectivity check failed: {e}")
            return False
    
    def _check_system_resources(self) -> bool:
        """检查系统资源"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
            
            # 检查系统资源的命令
            resource_commands = {
                'cpu_usage': "top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | sed 's/%us,//'",
                'memory_usage': "free | grep Mem | awk '{printf \"%.1f\", $3/$2 * 100.0}'",
                'disk_usage': "df -h / | awk 'NR==2{print $5}' | sed 's/%//'"
            }
            
            all_healthy = True
            resource_issues = []
            
            for node_name in node_names:
                node_resources = {}
                
                for resource_name, command in resource_commands.items():
                    try:
                        results = self.node_manager.execute_command(command, [node_name], timeout=10)
                        result = results.get(node_name)
                        
                        if result and result[0] == 0:
                            value = float(result[1].strip())
                            node_resources[resource_name] = value
                            
                            # 检查资源使用率阈值
                            if resource_name == 'cpu_usage' and value > 90:
                                resource_issues.append(f"{node_name}: High CPU usage {value}%")
                                all_healthy = False
                            elif resource_name == 'memory_usage' and value > 90:
                                resource_issues.append(f"{node_name}: High memory usage {value}%")
                                all_healthy = False
                            elif resource_name == 'disk_usage' and value > 85:
                                resource_issues.append(f"{node_name}: High disk usage {value}%")
                                all_healthy = False
                        else:
                            self.logger.warning(f"Failed to get {resource_name} for {node_name}")
                            
                    except Exception as e:
                        self.logger.warning(f"Error checking {resource_name} on {node_name}: {e}")
            
            if resource_issues:
                self.logger.warning(f"Resource issues found: {resource_issues}")
            
            return all_healthy
            
        except Exception as e:
            self.logger.error(f"System resource check failed: {e}")
            return False
    
    def _check_docker_services(self) -> bool:
        """检查Docker服务"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
            
            # 检查Docker守护进程
            docker_check_cmd = "docker info >/dev/null 2>&1 && echo 'ok' || echo 'failed'"
            docker_results = self.node_manager.execute_command(docker_check_cmd, node_names, timeout=15)
            
            failed_nodes = []
            for node_name, result in docker_results.items():
                if not result or result[0] != 0 or 'ok' not in result[1]:
                    failed_nodes.append(node_name)
            
            if failed_nodes:
                self.logger.error(f"Docker daemon not running on nodes: {failed_nodes}")
                return False
            
            # 检查运行中的服务状态
            service_status = self.docker_manager.get_service_status(node_names)
            
            unhealthy_services = []
            for node_name, services in service_status.items():
                for service in services:
                    if service.status == ServiceStatus.UNHEALTHY:
                        unhealthy_services.append(f"{node_name}:{service.name}")
            
            if unhealthy_services:
                self.logger.warning(f"Unhealthy services found: {unhealthy_services}")
                # 不健康的服务不一定意味着整体失败，可能是正常的状态
            
            self.logger.info(f"Docker services check passed for {len(node_names)} nodes")
            return True
            
        except Exception as e:
            self.logger.error(f"Docker services check failed: {e}")
            return False
    
    def _check_gpu_resources(self) -> bool:
        """检查GPU资源"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
            
            gpu_check_cmd = "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits"
            gpu_results = self.node_manager.execute_command(gpu_check_cmd, node_names, timeout=20)
            
            gpu_issues = []
            nodes_with_gpu = 0
            
            for node_name, result in gpu_results.items():
                if result and result[0] == 0 and result[1].strip():
                    nodes_with_gpu += 1
                    
                    # 解析GPU信息
                    gpu_lines = result[1].strip().split('\n')
                    for i, line in enumerate(gpu_lines):
                        if line.strip():
                            try:
                                parts = line.split(',')
                                if len(parts) >= 3:
                                    util = float(parts[0].strip())
                                    mem_used = float(parts[1].strip())
                                    mem_total = float(parts[2].strip())
                                    mem_usage = (mem_used / mem_total) * 100 if mem_total > 0 else 0
                                    
                                    # 检查GPU使用率（可选告警）
                                    if mem_usage > 95:
                                        gpu_issues.append(f"{node_name} GPU{i}: High memory usage {mem_usage:.1f}%")
                            except:
                                pass
                else:
                    # 没有GPU或nvidia-smi命令失败
                    self.logger.debug(f"No GPU or nvidia-smi failed on {node_name}")
            
            if gpu_issues:
                self.logger.warning(f"GPU issues found: {gpu_issues}")
            
            # 如果有GPU的节点，检查通过；如果没有GPU，也算通过（因为GPU不是必须的）
            self.logger.info(f"GPU check completed: {nodes_with_gpu} nodes with GPU detected")
            return True
            
        except Exception as e:
            self.logger.error(f"GPU resources check failed: {e}")
            return False
    
    def _check_network_connectivity(self) -> bool:
        """检查网络连通性"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            if len(nodes) < 2:
                self.logger.info("Less than 2 nodes, skipping network connectivity check")
                return True
            
            # 检查节点间的网络连通性
            connectivity_issues = []
            
            for i, node1 in enumerate(nodes):
                for node2 in nodes[i+1:]:
                    try:
                        # 从node1 ping node2
                        ping_cmd = f"ping -c 3 -W 5 {node2.host} >/dev/null 2>&1 && echo 'ok' || echo 'failed'"
                        results = self.node_manager.execute_command(ping_cmd, [node1.name], timeout=15)
                        
                        result = results.get(node1.name)
                        if not result or result[0] != 0 or 'ok' not in result[1]:
                            connectivity_issues.append(f"{node1.name} -> {node2.name}")
                            
                    except Exception as e:
                        connectivity_issues.append(f"{node1.name} -> {node2.name} (error: {e})")
            
            if connectivity_issues:
                self.logger.warning(f"Network connectivity issues: {connectivity_issues}")
                return len(connectivity_issues) == 0  # 有问题就返回False
            
            self.logger.info("Network connectivity check passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Network connectivity check failed: {e}")
            return False
    
    def _check_storage_space(self) -> bool:
        """检查存储空间"""
        try:
            nodes = self.node_manager.get_nodes(enabled_only=True)
            node_names = [node.name for node in nodes]
            
            # 检查关键目录的存储空间
            storage_paths = [
                "/",  # 根目录
                "/tmp",  # 临时目录
                "/var/lib/docker"  # Docker数据目录
            ]
            
            storage_issues = []
            
            for node_name in node_names:
                node = self.node_manager.get_node(node_name)
                if not node:
                    continue
                
                # 检查工作目录
                if node.work_dir:
                    storage_paths.append(node.work_dir)
                
                for path in storage_paths:
                    try:
                        # 检查磁盘使用率
                        df_cmd = f"df {path} | awk 'NR==2{{print $5}}' | sed 's/%//'"
                        results = self.node_manager.execute_command(df_cmd, [node_name], timeout=10)
                        
                        result = results.get(node_name)
                        if result and result[0] == 0:
                            usage = int(result[1].strip())
                            
                            # 检查使用率阈值
                            if usage > 90:
                                storage_issues.append(f"{node_name}:{path} {usage}%")
                                
                    except Exception as e:
                        self.logger.warning(f"Failed to check storage for {node_name}:{path}: {e}")
            
            if storage_issues:
                self.logger.error(f"Storage space issues: {storage_issues}")
                return False
            
            self.logger.info("Storage space check passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Storage space check failed: {e}")
            return False
    
    def _attempt_auto_recovery(self, health_results: Dict[str, ComponentHealth]):
        """尝试自动恢复"""
        if not self.enable_auto_recovery:
            return
        
        for component_name, health in health_results.items():
            if health.status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY]:
                if component_name in self.recovery_actions:
                    try:
                        self.logger.info(f"Attempting auto recovery for {component_name}")
                        recovery_func = self.recovery_actions[component_name]
                        recovery_func()
                    except Exception as e:
                        self.logger.error(f"Auto recovery failed for {component_name}: {e}")
    
    def add_recovery_action(self, component_name: str, recovery_func: Callable):
        """添加自动恢复动作"""
        self.recovery_actions[component_name] = recovery_func
        self.logger.info(f"Added recovery action for {component_name}")
    
    def _log_health_summary(self, results: Dict[str, ComponentHealth]):
        """记录健康检查摘要"""
        healthy_count = sum(1 for h in results.values() if h.status == HealthStatus.HEALTHY)
        degraded_count = sum(1 for h in results.values() if h.status == HealthStatus.DEGRADED)
        unhealthy_count = sum(1 for h in results.values() if h.status == HealthStatus.UNHEALTHY)
        
        self.logger.info(f"Health check summary: {healthy_count} healthy, "
                        f"{degraded_count} degraded, {unhealthy_count} unhealthy")
        
        # 记录详细问题
        for name, health in results.items():
            if health.status != HealthStatus.HEALTHY and health.error_messages:
                self.logger.warning(f"{name}: {', '.join(health.error_messages)}")
    
    def get_overall_health(self) -> HealthStatus:
        """获取整体健康状态"""
        if not self.component_status:
            return HealthStatus.UNKNOWN
        
        critical_checks = [name for name, check in self.checks.items() if check.critical]
        critical_status = [
            self.component_status[name].status for name in critical_checks 
            if name in self.component_status
        ]
        
        if not critical_status:
            return HealthStatus.UNKNOWN
        
        # 如果有任何关键检查不健康，整体状态为不健康
        if HealthStatus.UNHEALTHY in critical_status:
            return HealthStatus.UNHEALTHY
        
        # 如果有关键检查降级，整体状态为降级
        if HealthStatus.DEGRADED in critical_status:
            return HealthStatus.DEGRADED
        
        # 所有关键检查都健康
        if all(status == HealthStatus.HEALTHY for status in critical_status):
            return HealthStatus.HEALTHY
        
        return HealthStatus.UNKNOWN
    
    def get_health_report(self) -> Dict[str, Any]:
        """获取健康检查报告"""
        overall_status = self.get_overall_health()
        
        return {
            'overall_status': overall_status.value,
            'check_time': datetime.now().isoformat(),
            'total_checks': len(self.checks),
            'components': {name: health.to_dict() for name, health in self.component_status.items()},
            'summary': {
                'healthy': sum(1 for h in self.component_status.values() if h.status == HealthStatus.HEALTHY),
                'degraded': sum(1 for h in self.component_status.values() if h.status == HealthStatus.DEGRADED),
                'unhealthy': sum(1 for h in self.component_status.values() if h.status == HealthStatus.UNHEALTHY),
                'unknown': sum(1 for h in self.component_status.values() if h.status == HealthStatus.UNKNOWN)
            },
            'critical_issues': [
                f"{name}: {', '.join(health.error_messages)}"
                for name, health in self.component_status.items()
                if health.status == HealthStatus.UNHEALTHY and name in self.checks and self.checks[name].critical
            ]
        }
    
    def wait_for_healthy_state(self, timeout: int = 300, check_interval: int = 30) -> bool:
        """等待系统变为健康状态"""
        self.logger.info(f"Waiting for healthy state (timeout: {timeout}s)")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            results = self.run_all_checks()
            overall_status = self.get_overall_health()
            
            if overall_status == HealthStatus.HEALTHY:
                self.logger.info("System is healthy")
                return True
            
            self.logger.info(f"System status: {overall_status.value}, waiting {check_interval}s...")
            time.sleep(check_interval)
        
        self.logger.warning(f"System did not become healthy within {timeout}s")
        return False
    
    def enable_continuous_monitoring(self, interval: int = 300):
        """启用持续监控（这里只设置配置，实际的定时执行需要外部调度器）"""
        self.check_interval = interval
        self.logger.info(f"Continuous monitoring enabled with {interval}s interval")
    
    def disable_continuous_monitoring(self):
        """禁用持续监控"""
        self.check_interval = 0
        self.logger.info("Continuous monitoring disabled")