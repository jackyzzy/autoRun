"""
Docker容器状态检查统一服务
提供跨模块的容器发现、状态解析和Docker Compose名称映射功能
"""

import os
import re
import json
import yaml
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from .node_manager import NodeManager


@dataclass
class ContainerInfo:
    """标准化的容器信息"""
    name: str
    status: str
    ports: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    
    def is_running(self) -> bool:
        """判断容器是否处于运行状态"""
        if not self.status:
            return False
        
        status_lower = self.status.lower()
        running_indicators = [
            'up',           # Up X seconds
            'running',      # Running
            'healthy'       # (healthy)
        ]
        
        return any(indicator in status_lower for indicator in running_indicators)


@dataclass
class ContainerQueryResult:
    """容器查询结果"""
    success: bool
    containers: List[ContainerInfo]
    service_name: str
    node_name: str
    strategy_used: str
    error: Optional[str] = None
    
    def get_running_containers(self) -> List[ContainerInfo]:
        """获取运行中的容器"""
        return [c for c in self.containers if c.is_running()]
    
    def has_running_containers(self) -> bool:
        """是否有运行中的容器"""
        return len(self.get_running_containers()) > 0


class DockerContainerService:
    """统一的Docker容器状态检查服务"""
    
    def __init__(self, node_manager: NodeManager):
        self.node_manager = node_manager
        self.logger = logging.getLogger("playbook.docker_container_service")
        
    def find_containers_for_service(self, service_name: str, node_name: str, 
                                   compose_file_path: Optional[str] = None,
                                   include_stopped: bool = False) -> ContainerQueryResult:
        """
        为指定服务查找容器
        
        Args:
            service_name: 服务名称
            node_name: 目标节点名称  
            compose_file_path: compose文件路径，用于项目名检测
            include_stopped: 是否包含停止的容器
            
        Returns:
            ContainerQueryResult: 查询结果
        """
        try:
            # 策略1: Docker Compose标签匹配（最可靠）
            result = self._find_by_compose_label(service_name, node_name)
            if result.success:
                result.strategy_used = "compose_label"
                return result
            
            # 策略2: 精确container_name匹配
            result = self._find_by_exact_name(service_name, node_name, include_stopped)
            if result.success:
                result.strategy_used = "exact_name"
                return result
                
            # 策略3: 项目名检测 + 生成名称匹配
            if compose_file_path:
                project_names = self._detect_project_names(compose_file_path)
                result = self._find_by_generated_names(service_name, node_name, project_names, include_stopped)
                if result.success:
                    result.strategy_used = "generated_name"
                    return result
            
            # 策略4: 服务名模糊匹配
            result = self._find_by_fuzzy_matching(service_name, node_name, include_stopped)
            if result.success:
                result.strategy_used = "fuzzy_matching"
                return result
            
            # 策略5: 通配符匹配（兜底）
            result = self._find_by_wildcard_matching(service_name, node_name, include_stopped)
            if result.success:
                result.strategy_used = "wildcard_matching"
                return result
            
            # 所有策略都失败
            return ContainerQueryResult(
                success=False,
                containers=[],
                service_name=service_name,
                node_name=node_name,
                strategy_used="none",
                error="No containers found with any matching strategy"
            )
            
        except Exception as e:
            self.logger.error(f"Error finding containers for service {service_name} on {node_name}: {e}")
            return ContainerQueryResult(
                success=False,
                containers=[],
                service_name=service_name,
                node_name=node_name,
                strategy_used="error",
                error=str(e)
            )
    
    def _find_by_compose_label(self, service_name: str, node_name: str, include_stopped: bool = False) -> ContainerQueryResult:
        """通过Docker Compose标签查找容器"""
        ps_cmd = "docker ps -a" if include_stopped else "docker ps"
        filter_args = f"--filter 'label=com.docker.compose.service={service_name}'"
        success, output = self._execute_docker_ps_with_fallback(filter_args, node_name, ps_cmd)
        
        if success and output:
            containers = self._parse_container_output(output)
            self.logger.debug(f"Compose标签匹配找到 {len(containers)} 个容器")
            return ContainerQueryResult(
                success=True,
                containers=containers,
                service_name=service_name,
                node_name=node_name,
                strategy_used="compose_label"
            )
        
        return ContainerQueryResult(
            success=False, containers=[], service_name=service_name, 
            node_name=node_name, strategy_used="compose_label"
        )
    
    def _find_by_exact_name(self, service_name: str, node_name: str, include_stopped: bool = False) -> ContainerQueryResult:
        """通过精确名称匹配查找容器"""
        ps_cmd = "docker ps -a" if include_stopped else "docker ps"
        # 尝试原始名称和常见变换
        name_variants = [
            service_name,
            service_name.replace('-', '_'),
            service_name.replace('_', '-')
        ]
        
        for name in name_variants:
            filter_args = f"--filter 'name=^{name}$'"
            success, output = self._execute_docker_ps_with_fallback(filter_args, node_name, ps_cmd)
            
            if success and output:
                containers = self._parse_container_output(output)
                if containers:
                    self.logger.debug(f"精确名称匹配找到 {len(containers)} 个容器 (名称: {name})")
                    return ContainerQueryResult(
                        success=True,
                        containers=containers,
                        service_name=service_name,
                        node_name=node_name,
                        strategy_used="exact_name"
                    )
        
        return ContainerQueryResult(
            success=False, containers=[], service_name=service_name,
            node_name=node_name, strategy_used="exact_name"
        )
    
    def _find_by_generated_names(self, service_name: str, node_name: str, 
                                project_names: List[str], include_stopped: bool = False) -> ContainerQueryResult:
        """通过生成的容器名称查找"""
        ps_cmd = "docker ps -a" if include_stopped else "docker ps"
        containers = []
        
        for project_name in project_names:
            # Docker Compose生成名称的可能格式
            patterns = [
                f"{project_name}-{service_name}-*",  # v2格式
                f"{project_name}_{service_name}_*",  # v1格式
            ]
            
            for pattern in patterns:
                filter_args = f"--filter 'name={pattern}'"
                success, output = self._execute_docker_ps_with_fallback(filter_args, node_name, ps_cmd)
                
                if success and output:
                    found_containers = self._parse_container_output(output)
                    # 验证容器名是否真的匹配服务名
                    valid_containers = [
                        c for c in found_containers 
                        if self._validate_generated_name(c.name, service_name, project_name)
                    ]
                    containers.extend(valid_containers)
        
        if containers:
            self.logger.debug(f"生成名称匹配找到 {len(containers)} 个容器")
            return ContainerQueryResult(
                success=True,
                containers=containers,
                service_name=service_name,
                node_name=node_name,
                strategy_used="generated_name"
            )
        
        return ContainerQueryResult(
            success=False, containers=[], service_name=service_name,
            node_name=node_name, strategy_used="generated_name"
        )
    
    def _find_by_fuzzy_matching(self, service_name: str, node_name: str, include_stopped: bool = False) -> ContainerQueryResult:
        """通过模糊匹配查找容器"""
        ps_cmd = "docker ps -a" if include_stopped else "docker ps"
        # 生成服务名的各种变体
        service_variants = [
            service_name,
            service_name.replace('-', '_'),
            service_name.replace('_', '-'),
            service_name.replace('-', ''),
            service_name.replace('_', ''),
        ]
        
        containers = []
        for variant in service_variants:
            filter_args = f"--filter 'name={variant}'"
            success, output = self._execute_docker_ps_with_fallback(filter_args, node_name, ps_cmd)
            
            if success and output:
                found_containers = self._parse_container_output(output)
                containers.extend(found_containers)
        
        # 去重
        unique_containers = []
        seen_names = set()
        for c in containers:
            if c.name not in seen_names:
                unique_containers.append(c)
                seen_names.add(c.name)
        
        if unique_containers:
            self.logger.debug(f"模糊匹配找到 {len(unique_containers)} 个容器")
            return ContainerQueryResult(
                success=True,
                containers=unique_containers,
                service_name=service_name,
                node_name=node_name,
                strategy_used="fuzzy_matching"
            )
        
        return ContainerQueryResult(
            success=False, containers=[], service_name=service_name,
            node_name=node_name, strategy_used="fuzzy_matching"
        )
    
    def _find_by_wildcard_matching(self, service_name: str, node_name: str, include_stopped: bool = False) -> ContainerQueryResult:
        """通过通配符匹配查找容器（兜底策略）"""
        ps_cmd = "docker ps -a" if include_stopped else "docker ps"
        # 获取所有容器，然后在名称中搜索服务名
        success, output = self._execute_docker_ps_with_fallback("", node_name, ps_cmd)
        
        if not success or not output:
            return ContainerQueryResult(
                success=False, containers=[], service_name=service_name,
                node_name=node_name, strategy_used="wildcard_matching"
            )
        
        all_containers = self._parse_container_output(output)
        
        # 在容器名中搜索服务名的各种形式
        service_keywords = [
            service_name.lower(),
            service_name.replace('-', '_').lower(),
            service_name.replace('_', '-').lower(),
        ]
        
        matching_containers = []
        for container in all_containers:
            container_name_lower = container.name.lower()
            if any(keyword in container_name_lower for keyword in service_keywords):
                matching_containers.append(container)
        
        if matching_containers:
            self.logger.debug(f"通配符匹配找到 {len(matching_containers)} 个容器")
            return ContainerQueryResult(
                success=True,
                containers=matching_containers,
                service_name=service_name,
                node_name=node_name,
                strategy_used="wildcard_matching"
            )
        
        return ContainerQueryResult(
            success=False, containers=[], service_name=service_name,
            node_name=node_name, strategy_used="wildcard_matching"
        )
    
    def _execute_docker_ps_with_fallback(self, filter_args: str, node_name: str, ps_cmd: str = "docker ps") -> Tuple[bool, str]:
        """
        执行docker ps命令，支持多种格式的fallback
        
        Args:
            filter_args: docker ps的过滤参数
            node_name: 目标节点名称
            
        Returns:
            Tuple[bool, str]: (是否成功, 输出内容)
        """
        # 尝试JSON格式（最可靠）
        json_cmd = f"{ps_cmd} {filter_args} --format '{{{{json .}}}}'"
        results = self.node_manager.execute_command(json_cmd, [node_name])
        
        if results and results.get(node_name) and results[node_name][0] == 0:
            output = results[node_name][1].strip()
            if output and not output.startswith('NAMES'):
                self.logger.debug(f"JSON格式命令执行成功: {json_cmd}")
                return True, output
        
        # Fallback到tab分隔格式
        tab_cmd = f"{ps_cmd} {filter_args} --format '{{{{.Names}}}}\\t{{{{.Status}}}}\\t{{{{.Ports}}}}'"
        results = self.node_manager.execute_command(tab_cmd, [node_name])
        
        if results and results.get(node_name) and results[node_name][0] == 0:
            output = results[node_name][1].strip()
            if output and not output.startswith('NAMES'):
                self.logger.debug(f"Tab分隔格式命令执行成功: {tab_cmd}")
                return True, output
        
        # 最后尝试简单的table格式
        table_cmd = f"{ps_cmd} {filter_args}"
        results = self.node_manager.execute_command(table_cmd, [node_name])
        
        if results and results.get(node_name) and results[node_name][0] == 0:
            output = results[node_name][1].strip()
            if output and not output.startswith('CONTAINER ID'):
                self.logger.debug(f"Table格式命令执行成功: {table_cmd}")
                return True, output
        
        self.logger.warning(f"所有docker ps格式都失败了，filter: {filter_args}")
        return False, ""
    
    def _parse_container_output(self, output: str) -> List[ContainerInfo]:
        """
        智能解析docker ps输出，支持多种格式
        
        Args:
            output: docker ps命令的输出
            
        Returns:
            List[ContainerInfo]: 容器信息列表
        """
        if not output or not output.strip():
            self.logger.warning("Docker ps输出为空")
            return []
        
        containers = []
        lines = output.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
                
            container_info = None
            
            # 策略1: 尝试JSON解析（最可靠）
            try:
                container_data = json.loads(line)
                container_info = ContainerInfo(
                    name=container_data.get('Names', '').strip(),
                    status=container_data.get('Status', '').strip(),
                    ports=container_data.get('Ports', '').strip(),
                    labels=container_data.get('Labels', {}) or {}
                )
                self.logger.debug(f"JSON解析成功: {container_info.name}")
            except json.JSONDecodeError:
                pass
            
            # 策略2: Tab分隔解析（保留兼容性）
            if not container_info:
                parts = line.split('\t')
                if len(parts) >= 2:
                    container_info = ContainerInfo(
                        name=parts[0].strip(),
                        status=parts[1].strip(),
                        ports=parts[2].strip() if len(parts) > 2 else "",
                    )
                    self.logger.debug(f"Tab分隔解析成功: {container_info.name}")
            
            # 策略3: 正则表达式解析（处理空格分隔的情况）
            if not container_info:
                # 匹配容器名和状态的正则表达式
                # 格式: container_name   Up X seconds (healthy)   ports...
                match = re.match(r'^(\S+)\s+(Up\s+[^,\s]+(?:\s+\([^)]+\))?|Running|Exited\s+[^,\s]+)', line.strip())
                if match:
                    container_info = ContainerInfo(
                        name=match.group(1).strip(),
                        status=match.group(2).strip()
                    )
                    self.logger.debug(f"正则表达式解析成功: {container_info.name}")
            
            # 策略4: 简单字符串匹配（最后的fallback）
            if not container_info:
                words = line.split()
                if len(words) >= 2:
                    container_name = words[0]
                    status_keywords = ['Up', 'Running', 'Exited', 'Restarting', 'Paused', 'Dead']
                    status_parts = []
                    
                    for i, word in enumerate(words[1:], 1):
                        if any(keyword.lower() in word.lower() for keyword in status_keywords):
                            if word.lower().startswith('up') or word.lower().startswith('running'):
                                status_end = min(i + 4, len(words))
                                status_parts = words[i:status_end]
                            else:
                                status_parts = [word]
                            break
                    
                    if status_parts:
                        container_info = ContainerInfo(
                            name=container_name.strip(),
                            status=' '.join(status_parts).strip()
                        )
                        self.logger.debug(f"字符串匹配解析成功: {container_info.name}")
            
            # 验证解析结果
            if container_info and container_info.name and container_info.status:
                containers.append(container_info)
                self.logger.debug(f"成功解析容器: {container_info.name} -> {container_info.status}")
            elif container_info:
                self.logger.warning(f"解析结果不完整: name='{container_info.name}', status='{container_info.status}'")
            else:
                self.logger.warning(f"无法解析容器信息: {line}")
        
        self.logger.info(f"总共解析到 {len(containers)} 个容器")
        return containers
    
    def _detect_project_names(self, compose_file_path: str) -> List[str]:
        """
        检测可能的Docker Compose项目名
        
        Args:
            compose_file_path: compose文件路径
            
        Returns:
            List[str]: 可能的项目名列表（按优先级排序）
        """
        project_names = []
        
        try:
            compose_path = Path(compose_file_path)
            
            # 1. 从compose文件中读取name属性
            if compose_path.exists():
                try:
                    with open(compose_path, 'r', encoding='utf-8') as f:
                        compose_data = yaml.safe_load(f)
                        if isinstance(compose_data, dict) and 'name' in compose_data:
                            project_names.append(compose_data['name'])
                            self.logger.debug(f"从compose文件获取项目名: {compose_data['name']}")
                except Exception as e:
                    self.logger.warning(f"读取compose文件失败: {e}")
            
            # 2. 从目录名推导
            directory_name = compose_path.parent.name
            project_names.extend(self._generate_project_name_variants(directory_name))
            
            # 3. 从环境变量COMPOSE_PROJECT_NAME
            env_project_name = os.environ.get('COMPOSE_PROJECT_NAME')
            if env_project_name:
                project_names.append(env_project_name)
                self.logger.debug(f"从环境变量获取项目名: {env_project_name}")
            
        except Exception as e:
            self.logger.warning(f"检测项目名失败: {e}")
            
        # 去重并保持顺序
        unique_names = []
        for name in project_names:
            if name and name not in unique_names:
                unique_names.append(name)
        
        self.logger.info(f"检测到可能的项目名: {unique_names}")
        return unique_names
    
    def _generate_project_name_variants(self, directory_name: str) -> List[str]:
        """
        根据目录名生成项目名的各种变体
        
        Args:
            directory_name: 目录名
            
        Returns:
            List[str]: 项目名变体列表
        """
        variants = [directory_name]
        
        # Docker Compose项目名规则：只能包含小写字母、数字、连字符、下划线，且必须以字母或数字开头
        normalized = directory_name.lower()
        
        # 处理特殊字符
        normalized = re.sub(r'[^a-z0-9\-_]', '-', normalized)
        normalized = re.sub(r'-+', '-', normalized)  # 合并连续的连字符
        normalized = normalized.strip('-_')  # 移除首尾的连字符和下划线
        
        if normalized and normalized != directory_name:
            variants.append(normalized)
        
        # 生成连字符和下划线的变体
        if '-' in normalized:
            variants.append(normalized.replace('-', '_'))
        if '_' in normalized:
            variants.append(normalized.replace('_', '-'))
        
        return variants
    
    def _validate_generated_name(self, container_name: str, service_name: str, project_name: str) -> bool:
        """
        验证容器名是否符合Docker Compose生成规则
        
        Args:
            container_name: 容器名
            service_name: 服务名
            project_name: 项目名
            
        Returns:
            bool: 是否匹配
        """
        # Docker Compose生成名称的格式：
        # v2: project-service-number
        # v1: project_service_number
        
        patterns = [
            rf'^{re.escape(project_name)}-{re.escape(service_name)}-\d+$',  # v2格式
            rf'^{re.escape(project_name)}_{re.escape(service_name)}_\d+$',  # v1格式
        ]
        
        for pattern in patterns:
            if re.match(pattern, container_name):
                self.logger.debug(f"容器名 {container_name} 匹配生成规则: {pattern}")
                return True
        
        return False